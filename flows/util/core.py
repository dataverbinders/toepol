import requests
from prefect import task
from zipfile import ZipFile
from prefect.tasks.gcp.storage import GCSUpload


@task
def download_file(url, target_directory, target_file, download):
    if download:
        r = requests.get(url)
        with open(f"{target_directory}/{target_file}", "wb") as f:
            f.write(r.content)
    return target_directory + "/" + target_file


@task
def unzip(file, target_directory, select_extension=None):
    with ZipFile(file) as zip:
        files = [f"{target_directory}/{x}" for x in zip.namelist()]
        zip.extractall(target_directory)

    if select_extension:
        files =  [f for f in files if f.endswith(select_extension)]

    return files


@task
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
        uris.append(upload_to_gcs(credentials, file, bucket, folder))
    return uris
