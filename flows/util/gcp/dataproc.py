from google.cloud import dataproc_v1
from prefect import Flow, task


@task
def create_cluster(credentials, region, config, **kwargs):
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
def delete_cluster(credentials, region, config, **kwargs):
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
def start_cluster(credentials, project_id, region, cluster_name, **kwargs):
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
def stop_cluster(credentials, project_id, region, cluster_name, **kwargs):
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
