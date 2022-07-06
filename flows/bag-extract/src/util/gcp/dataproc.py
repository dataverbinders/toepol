import uuid

from google.cloud import dataproc_v1
from prefect import task


@task
def submit_batch_job(credentials: dict, region: str, config: dict, **kwargs):
    client = dataproc_v1.BatchControllerClient().from_service_account_info(
        credentials,
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)
        },
    )

    batch = dataproc_v1.Batch(**config)
    batch_id = f"bag-{uuid.uuid1()}"

    request = dataproc_v1.CreateBatchRequest(
        batch_id=batch_id,
        parent=f"projects/{credentials['project_id']}/regions/{region}",
        batch=batch,
    )

    operation = client.create_batch(request=request)
    response = operation.result()

    return response
