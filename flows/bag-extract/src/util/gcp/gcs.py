from prefect import task
from google.cloud import storage
from google.oauth2 import service_account


@task
def get_storage_client(credentials: dict):
    credentials = service_account.Credentials.from_service_account_info(
        credentials
    )
    storage_client = storage.Client(None, credentials)
    return storage_client


@task
def get_storage_bucket(client, bucket):
    bucket = storage.Bucket(client, bucket)
    return bucket


@task
def upload_file_to_blob(file, blob_name, bucket):
    blob = storage.Blob(blob_name, bucket)
    with open(file, "r") as f:
        blob.upload_from_file(f)
    return blob_name


@task
def upload_files_to_gcs(files, blob_names, credentials, bucket):
    client = get_storage_client.run(credentials)
    bucket = get_storage_bucket.run(client, bucket)

    blobs = [
        upload_file_to_blob.run(files[i], blob_names[i], bucket)
        for i in range(len(files))
    ]

    return blobs
