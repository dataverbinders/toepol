from zipfile import ZipFile

import prefect
import requests
from prefect import Flow, Parameter, mapped, task
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.gcp.storage import GCSUpload
from prefect.executors import DaskExecutor


@task
def download_file(url: str, data_dir: str, target_file: str):
    r = requests.get(url)
    with open(f"{data_dir}/{target_file}", "wb") as f:
        f.write(r.content)
    return f"{data_dir}/{target_file}"


@task
def unzip_file(file: str, target_directory) -> str:
    with ZipFile(file) as zip:
        file = zip.namelist()[0]
        zip.extractall(target_directory)
    return f"{target_directory}/{file}"


@task
def store_csv_on_gcs(file: str, bucket: str, blob: str, credentials: dict):
    with open(file, "rb") as f:
        data = f.read()
        GCSUpload(bucket=bucket).run(data, blob=blob, credentials=credentials)


with Flow("download_zipped_csv_to_gcs") as flow:
    source_url = Parameter("source_url")

    data_dir = Parameter("data_dir", default="data")
    zip_file = Parameter("zip_file", default="f.zip")

    gcs_bucket = Parameter("gcs_bucket")
    gcs_blob = Parameter("gcs_blob")

    gcp_credentials = PrefectSecret("GCP_CREDENTIALS")

    zipfile = download_file(source_url, data_dir, zip_file)
    csvfile = unzip_file(zipfile, data_dir)
    store_csv_on_gcs(csvfile, gcs_bucket, gcs_blob, gcp_credentials)


if __name__ == "__main__":
    flow.executor = DaskExecutor()
    flow.register(project_name="toepol")
