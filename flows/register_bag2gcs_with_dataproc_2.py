from os import listdir, mkdir
from typing import List

import prefect
from prefect import Flow, Parameter, mapped, task
from prefect.backend import get_key_value
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.secrets import PrefectSecret
from prefect.tasks.shell import ShellTask
from util.core import download_file, unzip, upload_files_to_gcs, upload_to_gcs
from util.gcp.dataproc import submit_batch_job


@task
def print_var(var):
    logger.info(var)
    logger.info(type(var))


@task
def add_archive_uris_to_config(config: dict, uris: List):
    """add_archive_uris_to_config.

    :param config: a dictionary containing the configuration for a dataproc job
    :type config: dict
    :param uris: a list of archives to be passed to the configuration
    :type uris: List
    """
    config["job"]["pyspark_job"]["archive_uris"] = uris
    return config


@task
def get_target_directories(zipfile):
    object_dir = "".join([c for c in zipfile.split(".")[0] if not c.isdigit()])
    return object_dir


@task
def get_blob_directories(zipfile):
    zipname = zipfile.split("/")[-1]
    object_type = "".join([c for c in zipname.split(".")[0] if not c.isdigit()])
    return f"bag/xml/{object_type}"


@task
def create_data_dir(name: str) -> str:
    if name not in listdir():
        mkdir(name)
    return name


@task
def delete_dir(name: str, **kwargs):
    ShellTask(command=f"rm -r {name}")


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
    download_bag = Parameter("download_bag", default=True)
    gcs_temp_bucket = Parameter("temp_bucket", default="temp-prefect-data")
    gcp_region = Parameter("gcp_region", default="europe-west4")

    data_dir = create_data_dir(data_dir)

    # get batch job config
    job_config = get_key_value(key="dataproc_bag_batch_job_config")

    # download bag zip
    bag_file = download_file(bag_url, data_dir, bag_file_name, download_bag)

    # unzip main bag zip
    sub_zipfiles = unzip(bag_file, data_dir, select_extension=".zip")

    # unzip subzips and upload xml files to GCS
    target_dirs = get_target_directories.map(sub_zipfiles)
    xml_files = unzip.map(sub_zipfiles, target_dirs)

    # upload xml files to GCS
    paths = get_blob_directories(mapped(sub_zipfiles))
    uris = upload_files_to_gcs(
        gcp_credentials, mapped(xml_files), gcs_temp_bucket, mapped(paths)
    )

    # upload pyspark script and jar to GCS
    py_file = upload_to_gcs(
        gcp_credentials,
        "flows/spark_jobs/bag/batch_job.py",
        gcs_temp_bucket,
        "bag/dataproc",
    )
    jar_file = upload_to_gcs(
        gcp_credentials,
        "flows/spark_jobs/bag/spark-xml_2.12-0.14.0.jar",
        gcs_temp_bucket,
        "bag/dataproc",
    )

    batch_result = submit_batch_job(
        gcp_credentials,
        gcp_region,
        job_config,
        dependencies=[uris, py_file, jar_file]
    )

    delete_dir(data_dir, dep=[batch_result])

if __name__ == "__main__":
    flow.executor = LocalDaskExecutor()
    flow.run_config = UniversalRun(
        labels=["bag"], env={"PREFECT__CLOUD__HEARTBEAT_MODE": "thread"}
    )

    schedule = Schedule(clocks=[CronClock("0 3 8 * *")])
    flow.schedule = schedule

    flow.register(project_name="toepol", add_default_labels=False)
