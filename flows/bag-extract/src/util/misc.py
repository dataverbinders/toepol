import os
from datetime import timedelta
from zipfile import ZipFile

import requests
from prefect import task
from prefect.tasks.gcp.storage import GCSUpload


@task
def download_file(url, target_directory, target_file, write_mode="wb"):
    r = requests.get(url)
    with open(f"{target_directory}/{target_file}", write_mode) as f:
        f.write(r.content)
    return f"{target_directory}/{target_file}"


@task
def unzip(file, target_directory, select_extension=None):
    with ZipFile(file) as zip:
        files = [f"{target_directory}/{x}" for x in zip.namelist()]
        zip.extractall(target_directory)

    if select_extension:
        files = [f for f in files if f.endswith(select_extension)]

    return files


@task
def object_from_zipfile(zipfile):
    object_dir = "".join([c for c in zipfile.split(".")[0] if not c.isdigit()])
    return object_dir


@task
def generate_blob_directory(zipfile):
    zipname = zipfile.split("/")[-1]
    object_type = "".join([c for c in zipname.split(".")[0] if not c.isdigit()])
    return f"bag/xml/{object_type}"


@task(max_retries=16, retry_delay=timedelta(seconds=5))
def upload_to_gcs(credentials, file, bucket, folder=None):
    filename = file.split("/")[-1]
    if folder is not None:
        blob = "/".join([folder, filename])
    else:
        blob = filename

    with open(file, "rb") as f:
        data = f.read()
        GCSUpload(bucket=bucket).run(data, blob=blob, credentials=credentials)

    uri = f"gs://{bucket}/{blob}"

    return uri


@task
def upload_files_to_gcs(credentials, files, bucket, folder=None):
    uris = []
    for file in files:
        uris.append(upload_to_gcs.run(credentials, file, bucket, folder))
    return uris
