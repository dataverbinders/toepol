import prefect
from prefect import Flow, Parameter, mapped, task
from prefect.backend import get_key_value
from prefect.executors import DaskExecutor
from prefect.run_configs import UniversalRun
from prefect.tasks.secrets import PrefectSecret
from util.core import download_file, unzip, upload_to_gcs
from util.gcp import dataproc


@task
def print_var(var):
    logger.info(var)
    logger.info(type(var))


@task
def add_archive_uris_to_config(config, uris):
    config["job"]["pyspark_job"]["archive_uris"] = uris
    return config


with Flow("bag2gcs_with_dataproc") as flow:
    logger = prefect.context.get("logger")
    gcp_credentials = PrefectSecret("GCP_CREDENTIALS")

    # parameters
    data_dir = Parameter("data_dir", default="data")
    bag_file_name = Parameter("bag_file", default="lvbag-extract-nl.zip")
    bag_url = Parameter(
        "bag_url",
        default="https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip",
    )
    gcs_temp_bucket = Parameter("temp_bucket", default="temp-prefect-data")
    gcp_region = Parameter("gcp_region", default="europe-west4")

    # download bag zip
    bag_file = download_file(bag_url, data_dir, bag_file_name)
    #  bag_file = "lvbag-extract-nl.zip"

    # create spark cluster
    dataproc_config = get_key_value("dataproc_cluster_config")
    cluster = dataproc.create_cluster(
        gcp_credentials,
        gcp_region,
        dataproc_config,
        bag_file=bag_file,
    )

    # unzip main bag zip
    files = unzip(bag_file, data_dir, select_extension=".zip")

    # upload 'subzips' to GCS
    uris = upload_to_gcs(
        gcp_credentials, mapped(files), gcs_temp_bucket, "bag/subzips"
    )

    # upload pyspark script and jar to GCS
    py_file = upload_to_gcs(
        gcp_credentials,
        "flows/spark_jobs/bag/job.py",
        gcs_temp_bucket,
        "bag/dataproc",
    )
    jar_file = upload_to_gcs(
        gcp_credentials,
        "flows/spark_jobs/bag/spark-xml_2.12-0.14.0.jar",
        gcs_temp_bucket,
        "bag/dataproc",
    )

    # submit job
    job_config = get_key_value("dataproc_job_bag_config")
    job_config = add_archive_uris_to_config(job_config, uris)
    job_result = dataproc.submit_job_as_operation(
        gcp_credentials,
        gcp_region,
        job_config,
        cluster=cluster,
        py_file=py_file,
        jar_file=jar_file,
    )

    # delete spark cluster
    dataproc.delete_cluster(
        gcp_credentials, gcp_region, dataproc_config, cluster=job_result
    )


if __name__ == "__main__":
    flow.executor = DaskExecutor()
    flow.run_config = UniversalRun(labels=["bag"])
    flow.register(project_name="toepol")
