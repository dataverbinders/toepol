import wget
import os
from pyarrow import csv
import pyarrow.parquet as pq
from google.cloud import storage
from google.oauth2 import service_account
# from bag-extract.src.util.gcp import gcs

import prefect
from prefect import Flow, Parameter, task, unmapped, resource_manager
from prefect.tasks.secrets import PrefectSecret
from pyspark.sql import SparkSession

enexis_data = [
    "2019"
    , "2020"
    , "2021"
    , "2022"
]


@task
def get_csv(years):
    csv_output = f"{os.getcwd()}/data/enexis-kleinverbruik-{years}.csv"
    csv_url = f"https://s3-eu-west-1.amazonaws.com/enxp433-oda01/kv/Enexis_kleinverbruiksgegevens_0101{years}.csv"

    
    if os.path.exists(csv_output):
        os.remove(csv_output)

    wget.download(csv_url, csv_output)

    return csv_output


# REPLACE with nl-open-data method 'csv_to_parquet()'.
@task
def csv_to_parquet(csv_file, encoding="utf-8", delimiter=","):
    encoding

    pq_file = f"{csv_file.split('.')[0]}.parquet"

    table = csv.read_csv(
        csv_file,
        read_options=csv.ReadOptions(encoding=encoding),
        parse_options=csv.ParseOptions(delimiter=delimiter)
    )

    pq.write_table(table, pq_file)

    os.remove(csv_file)

    return pq_file


# REPLACE with method from bag-extract.src.util.gcp.gcs.
@task
def get_storage_client(credentials: dict):
    credentials = service_account.Credentials.from_service_account_info(
        credentials
    )
    storage_client = storage.Client(None, credentials)
    return storage_client


@task
def parquet_to_gcs(file, bucket_name, credential):
    file_name = file.split("/")[-1]
    blob_name = f"enexis/{file_name}"

    # Local Testing.
    # client = storage.Client.from_service_account_json(
    #     credential
    # )

    # Prefect
    client = get_storage_client.run(credential)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(file)


if __name__ == "__main__":
    # for year in enexis_data:
    #     path_csv = get_csv(year)
    #     path_pq = csv_to_parquet(path_csv, delimiter=";")
    #     parquet_to_gcs(path_pq, "temp-prefect-data", "/Users/eddylim/Documents/gcp_keys/prefect_key.json")
    
    # with Flow("Download Enexis", storage=local()) as flow:
    with Flow("Download Enexis") as flow:
        logger = prefect.context.get("logger")

        # Secrets
        gcp_credentials = PrefectSecret("GCP_CREDENTIALS")
        # gcp_credentials = "/Users/eddylim/Documents/gcp_keys/prefect_key.json"
        bucket_name = Parameter("bucket_name", default="temp-prefect-data")

        # for year in enexis_data:
        #     path_csv = get_csv(year)
        #     path_pq = csv_to_parquet(path_csv, delimiter=";")
        #     parquet_to_gcs(path_pq, bucket_name, gcp_credentials)


        path_csv = get_csv.map(enexis_data)
        path_pq = csv_to_parquet.map(path_csv, delimiter=unmapped(";"))
        parquet_to_gcs.map(path_pq, unmapped(bucket_name), unmapped(gcp_credentials))
        
    # Local testing.
    # flow.run()

    # Register the Flow.
    flow.register("toepol")