from google.cloud import dataproc_v1
from prefect import task


@task
def create_cluster(credentials: dict, region: str, config: dict, **kwargs):
    """Create a dataproc cluster.

    :param credentials: the GCP credentials
    :type credentials: dict
    :param region: region to create the cluster in. example: 'europe-west4'
    :type region: str
    :param config: the configuration for the dataproc cluster
    :type config: dict
    :param kwargs: any additional parameters. This can be used to easily make this task depend on other tasks.
    """
    client = dataproc_v1.ClusterControllerClient().from_service_account_info(
        credentials,
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)
        },
    )

    cluster_config = dataproc_v1.ClusterConfig(**config["config"])
    cluster = dataproc_v1.Cluster(config=cluster_config)
    cluster.cluster_name = config["cluster_name"]

    request = dataproc_v1.CreateClusterRequest(
        project_id=config["project_id"], region=region, cluster=cluster
    )

    operation = client.create_cluster(request=request)
    response = operation.result()
    return response


@task
def delete_cluster(credentials: dict, region: str, config: dict, **kwargs):
    """Delete a dataproc cluster.

    :param credentials: the GCP credentials
    :type credentials: dict
    :param region: region to create the cluster in. example: 'europe-west4'
    :type region: str
    :param config: the configuration for the dataproc cluster
    :type config: dict
    :param kwargs: any additional parameters. This can be used to easily make this task depend on other tasks.
    """
    client = dataproc_v1.ClusterControllerClient().from_service_account_info(
        credentials,
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)
        },
    )

    request = dataproc_v1.DeleteClusterRequest(
        project_id=config["project_id"],
        region=region,
        cluster_name=config["cluster_name"],
    )

    operation = client.delete_cluster(request=request)
    response = operation.result()
    return response


@task
def start_cluster(
    credentials: dict,
    project_id: str,
    region: str,
    cluster_name: str,
    **kwargs
):
    """Start a stopped dataproc cluster.

    :param credentials: the GCP credentials
    :type credentials: dict
    :param project_id: project id
    :type project_id: str
    :param region: region the cluster exists in
    :type region: str
    :param cluster_name: name of the cluster
    :type cluster_name: str
    :param kwargs: any additional parameters. This can be used to easily make this task depend on other tasks.
    """
    client = dataproc_v1.ClusterControllerClient().from_service_account_info(
        credentials,
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)
        },
    )

    request = dataproc_v1.StartClusterRequest(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
    )

    operation = client.start_cluster(request=request)
    print("Waiting for operation to complete")
    response = operation.result()
    return response


@task
def stop_cluster(
    credentials: dict,
    project_id: str,
    region: str,
    cluster_name: str,
    **kwargs
):
    """Stop a running dataproc cluster.

    :param credentials: the GCP credentials
    :type credentials: dict
    :param project_id: project id
    :type project_id: str
    :param region: region the cluster exists in
    :type region: str
    :param cluster_name: name of the cluster
    :type cluster_name: str
    :param kwargs: any additional parameters. This can be used to easily make this task depend on other tasks.
    """
    client = dataproc_v1.ClusterControllerClient().from_service_account_info(
        credentials,
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)
        },
    )

    request = dataproc_v1.StopClusterRequest(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
    )

    operation = client.stop_cluster(request=request)
    print("Waiting for operation to complete...")

    response = operation.result()
    return response


@task
def submit_job_as_operation(credentials: dict, region: str, config: dict, **kwargs):
    """Submit a job as an operation.
    This task only returns once the job is finished.

    :param credentials:
    :type credentials: dict
    :param region:
    :type region: str
    :param config:
    :type config: dict
    :param kwargs:
    """
    client = dataproc_v1.JobControllerClient().from_service_account_info(
        credentials,
        client_options={
            "api_endpoint": "{}-dataproc.googleapis.com:443".format(region)
        },
    )

    job = dataproc_v1.Job(**config["job"])
    request = dataproc_v1.SubmitJobRequest(
        project_id=config["project_id"], region=region, job=job
    )
    operation = client.submit_job_as_operation(request=request)
    response = operation.result()
    return response
