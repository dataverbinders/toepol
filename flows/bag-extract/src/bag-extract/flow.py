import os
from typing import List

import prefect
from dotenv import load_dotenv
from prefect import Flow, Parameter, mapped, task
from prefect.backend import get_key_value
from prefect.run_configs import DockerRun
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.tasks.secrets import PrefectSecret
from util.gcp.dataproc import submit_batch_job
from util.misc import (download_file, generate_blob_directory,
                       object_from_zipfile, unzip, upload_files_to_gcs,
                       upload_to_gcs)

schedule = Schedule(clocks=[CronClock("0 0 9 * *")])

load_dotenv()

with Flow(
    "bag-extract",
    #  schedule=schedule,
    run_config=DockerRun(
        image=os.getenv("image"),
        labels=["bag"],
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

    # Key Value Pairs
    job_config = get_key_value(key="dataproc_bag_batch_job_config")

    # Download BAG zip
    bag_file = download_file(bag_url, DATA_DIR, BAG_FILE_NAME)

    # Unzip main bag file
    zipfiles = unzip(bag_file, DATA_DIR, select_extension=".zip")

    # Unzip object level zipfiles
    objects = object_from_zipfile.map(zipfiles)
    xml_files = unzip.map(zipfiles, objects)

    # Upload XML files to GCS
    paths = generate_blob_directory.map(zipfiles)
    uris = upload_files_to_gcs(
        gcp_credentials, mapped(xml_files), gcs_temp_bucket, mapped(paths)
    )

    # Upload files for spark job
    py_file = upload_to_gcs(
        gcp_credentials,
        "src/spark/batch_job.py",
        gcs_temp_bucket,
        "bag/dataproc",
    )
    jar_file = upload_to_gcs(
        gcp_credentials,
        "src/spark/spark-xml_2.12-0.14.0.jar",
        gcs_temp_bucket,
        "bag/dataproc",
    )

    # Run batch job
    batch_result = submit_batch_job(
        gcp_credentials,
        gcp_region,
        job_config,
        dependencies=[uris, py_file, jar_file]
    )


flow.register("toepol")
