from prefect import Client as PrefectClient
from datetime import datetime

# Flow Parameters.
SOURCE = "bag"
BAG_FILE = "lvbag-extract-nl.zip"
BAG_URL = "https://service.pdok/nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip"
DOWNLOAD_BAG = False
GCS_BUCKET = "dataverbinders-dev"
GCS_FOLDER = "kadaster/bag"

# Run Parameters.
VERSION_GROUP_ID = "bag_to_gcs"
RUN_NAME = f"{SOURCE}_{datetime.today().date()}_{datetime.today().time()}"

if __name__ == "__main__":
    # Configure Flow.
    prefect_client = PrefectClient()  # Local api key has been stored previously
    # prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

    parameters = {
        "bag_file": BAG_FILE,
        "bag_url": BAG_URL,
        "download_bag": DOWNLOAD_BAG,
        "gcs_bucket": GCS_BUCKET,
        "gcs_folder": GCS_FOLDER,
    }

    flow_run_id = prefect_client.create_flow_run(
        version_group_id=VERSION_GROUP_ID,
        run_name=RUN_NAME,
        parameters=parameters
    )


