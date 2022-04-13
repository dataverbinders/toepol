import uuid
from datetime import timedelta

import prefect
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from prefect import Flow, Parameter, task
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.secrets import PrefectSecret


@task
def auth(credentials: dict) -> service_account:
    return service_account.Credentials.from_service_account_info(credentials)


@task(max_retries=8, retry_delay=timedelta(seconds=10))
def get_object_uris(
    credentials: service_account, bucket: str, path: str, obj: str
):
    client = storage.Client(credentials=credentials)
    blobs = list(
        client.list_blobs(bucket_or_name=bucket, prefix=f"{path}/{obj}")
    )
    uris = [
        f"gs://{blob.bucket.name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]
    print(uris)
    return uris


@task
def gen_uri(bucket: str, path: str, obj: str) -> str:
    uri = f"gs://{bucket}/{path}/{obj}/*.parquet"
    return uri


@task(max_retries=4, retry_delay=timedelta(seconds=10))
def load_to_bq(
    credentials: service_account,
    project_id: str,
    dataset: str,
    table: str,
    uris: list,
):
    client = bigquery.Client(credentials=credentials)

    dataset_ref = bigquery.DatasetReference(project_id, dataset)
    table_ref = bigquery.TableReference(dataset_ref, table)

    job_id = f"load_{table}_{uuid.uuid1()}"
    load_job_config = bigquery.LoadJobConfig(
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
    )
    load_job = bigquery.LoadJob(
        job_id=job_id,
        source_uris=uris,
        destination=table_ref,
        client=client,
        job_config=load_job_config,
    )

    result = load_job.result()
    return result


with Flow("bag_gcs-to-bq") as flow:
    # Secrets
    gcp_credentials = PrefectSecret("GCP_CREDENTIALS")

    # Parameters
    gcs_bucket = Parameter("gcs_bucket", default="dataverbinders-dev")
    gcs_path = Parameter("gcs_path", default="kadaster/bag")
    project_id = Parameter("project_id", default="dataverbinders-dev")
    bq_dataset = Parameter("bq_dataset", default="bag")

    # Create credentials object
    credentials = auth(gcp_credentials)

    for obj in [
        "Woonplaats",
        "OpenbareRuimte",
        "Nummeraanduiding",
        "Ligplaats",
        "Standplaats",
        "Pand",
        "Verblijfsobject"
    ]:
        # List URIs
        #  uris = get_object_uris(credentials, gcs_bucket, f"{gcs_path}")
        uri = gen_uri(gcs_bucket, gcs_path, obj)

        # Load URIs into BigQuery
        load_to_bq(credentials, project_id, bq_dataset, obj, uri)


if __name__ == "__main__":
    #  schedule = Schedule(clocks=[CronClock("0 6 8 * *")])
    #  flow.schedule = schedule

    flow.register(project_name="toepol")
    #  flow.run()
