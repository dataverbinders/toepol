import wget
import os
from pyarrow import csv
import pyarrow.parquet as pq
from util.gcp import gcs
from dotenv import load_dotenv

from prefect import Flow, Parameter, task, unmapped
from prefect.tasks.secrets import PrefectSecret
from prefect.storage.github import GitHub
from prefect.run_configs import DockerRun

enexis_data = [
    "2019"
    , "2020"
    , "2021"
    , "2022"
]

@task
def create_dir(dir):
    os.mkdir(dir)
    return dir


@task
def get_csv(years, directory):
    csv_output = f"{directory}/enexis-kleinverbruik-{years}.csv"
    csv_url = f"https://s3-eu-west-1.amazonaws.com/enxp433-oda01/kv/Enexis_kleinverbruiksgegevens_0101{years}.csv"

    
    if os.path.exists(csv_output):
        os.remove(csv_output)

    wget.download(csv_url, csv_output)

    return csv_output


@task
def csv_to_parquet(csv_file, encoding="utf-8", delimiter=","):
    pq_file = f"{csv_file.split('.')[0]}.parquet"

    table = csv.read_csv(
        csv_file,
        read_options=csv.ReadOptions(encoding=encoding),
        parse_options=csv.ParseOptions(delimiter=delimiter)
    )

    pq.write_table(table, pq_file)

    os.remove(csv_file)

    return pq_file


@task
def parquet_to_gcs(file, bucket_name, credential):
    client = gcs.get_storage_client.run(credential)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file)

    blob.upload_from_filename(file)


with Flow(
    "enexis",
    storage=GitHub(
        repo="dataverbinders/toepol",
        path="flows/enexis/src/enexis/flow.py",
    ),
    run_config=DockerRun(
        image=os.getenv("image"),
        labels=["enexis"],
        env={"PREFECT__CLOUD__HEARTBEAT_MODE": "thread"},
    ),
) as flow:
    # Secrets
    gcp_credentials = PrefectSecret("GCP_CREDENTIALS")

    # Parameters
    enexis_dir = Parameter("enexis_dir", default="enexis")
    bucket_name = Parameter("bucket_name", default="temp-prefect-data")
    
    data_dir = create_dir(enexis_dir)
    path_csv = get_csv.map(years=enexis_data, directory=unmapped(data_dir))
    path_pq = csv_to_parquet.map(path_csv, delimiter=unmapped(";"))
    parquet_to_gcs.map(path_pq, unmapped(bucket_name), unmapped(gcp_credentials))

prefect_project = "toepol" if os.getenv("production") == "True" else "dev-toepol"

flow.register("prefect-project")