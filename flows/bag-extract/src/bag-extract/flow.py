import os

import prefect
from dotenv import load_dotenv
from prefect import Flow, Parameter, mapped, task, case
from prefect.tasks.flow_control import merge
from prefect.backend import get_key_value
from prefect.run_configs import DockerRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.storage.github import GitHub
from prefect.tasks.secrets import PrefectSecret
from util.gcp.dataproc import submit_batch_job
from util.gcp import gcs
from util.misc import (
    create_directory,
    download_file,
    generate_blob_directory,
    generate_blob_names,
    object_from_zipfile,
    upload_to_gcs,
    unzip,
)

schedule = Schedule(clocks=[CronClock("0 0 9 * *")])

load_dotenv()


@task
def print_var(var):
    print(var)

@task
def eval_bool(b):
    return b

with Flow(
    "bag-extract",
    #  schedule=schedule,
    storage=GitHub(
        repo="dataverbinders/toepol",
        path="flows/bag-extract/src/bag-extract/flow.py",
    ),
    run_config=DockerRun(
        image=os.getenv("image"),
        labels=["bag"],
        env={"PREFECT__CLOUD__HEARTBEAT_MODE": "thread"},
    ),
) as flow:

    # Constants
    DATA_DIR = "data"
    BAG_FILE_NAME = "lvbag-extract-nl.zip"

    # Secrets
    gcp_credentials = PrefectSecret("GCP_CREDENTIALS")

    # Parameters
    bag_url = Parameter(
        "bag_url",
        default="https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip",
    )
    gcs_temp_bucket = Parameter("temp_bucket", default="temp-prefect-data")
    gcp_region = Parameter("gcp_region", default="europe_west_4")
    download_new_bag = Parameter("download_new_bag", default=True)

    # Key Value Pairs
    job_config = get_key_value(key="dataproc_bag_batch_job_config")

    data_dir = create_directory(DATA_DIR)

    with case(eval_bool(download_new_bag), True):
        # Download BAG zip
        bag_file = download_file(bag_url, data_dir, BAG_FILE_NAME)

        # Unzip main bag file
        zipfiles = unzip(bag_file, data_dir, select_extension=".zip")

        # Unzip object level zipfiles
        objects = object_from_zipfile.map(zipfiles)
        xml_files = unzip.map(zipfiles, objects)

        # Upload XML files to GCS
        paths = generate_blob_directory.map(zipfiles)
        blob_names = generate_blob_names.map(paths, xml_files)
        uris1 = gcs.upload_files_to_gcs(
            mapped(xml_files), mapped(blob_names), gcp_credentials, gcs_temp_bucket
        )

        # Upload files for spark job
        py_file1 = upload_to_gcs(
            gcp_credentials,
            "/opt/prefect/pyspark/batch_job.py",
            gcs_temp_bucket,
            "bag/dataproc",
        )
        jar_file1 = upload_to_gcs(
            gcp_credentials,
            "/opt/prefect/pyspark/spark-xml_2.12-0.14.0.jar",
            gcs_temp_bucket,
            "bag/dataproc",
        )
    with case(eval_bool(download_new_bag), True):
        uris2 = None
        py_file2 = None
        jar_file2 = None

    uris = merge(uris1, uris2)
    py_file = merge(py_file1, py_file2)
    jar_file = merge(jar_file1, jar_file2)

    # Run batch job
    batch_result = submit_batch_job(
        gcp_credentials,
        gcp_region,
        job_config,
        dependencies=[uris, py_file, jar_file],
    )

flow.register("toepol")
