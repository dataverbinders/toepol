from typing import List

import prefect
from prefect import Flow, Parameter, mapped, task
from prefect.backend import get_key_value
from prefect.executors import LocalExecutor
from prefect.run_configs import UniversalRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.secrets import PrefectSecret
from util.core import download_file, unzip, upload_to_gcs
from util.gcp import dataproc


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

    # download bag zip
    bag_file = download_file(bag_url, data_dir, bag_file_name, download_bag)

# unzip main bag zip
    sub_zipfiles = unzip(bag_file, data_dir, select_extension=".zip")

    # unzip subzips and upload xml files to GCS
    for subzip in sub_zipfiles:
        xml_files = unzip(subzip, f"{data_dir}/{subzip.split('.')[0]}")

    # upload 'subzips' to GCS
    #  uris = upload_to_gcs(
        #  gcp_credentials, mapped(files), gcs_temp_bucket, "bag/subzips"
    #  )

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

if __name__ == "__main__":
    flow.executor = LocalExecutor()
    flow.run_config = UniversalRun(labels=["bag"])

    schedule = Schedule(clocks=[CronClock("0 3 8 * *")])
    flow.schedule = schedule

    flow.register(project_name="toepol", add_default_labels=False)
