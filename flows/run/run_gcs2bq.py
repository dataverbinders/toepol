from prefect import Client as PrefectClient
from datetime import datetime

# Flow Parameters.
SOURCE = "gcs_bag"
BQ_DATASET_ID = "bag"
GCP_LOCATION = "EU"
SOURCE_FORMAT = "PARQUET"
WRITE_DISPOSITION = "WRITE_TRUNCATE"
gcs_uri_table_map = {
    "gs://dataverbinders-dev/kadaster/bag/Verblijfsobject.parquet": "Verblijfsobject",
    "gs://dataverbinders-dev/kadaster/bag/Woonplaats.parquet":"Woonplaats",
}
mirror_time = f"{datetime.today().date()}_{datetime.today().time()}"


if __name__ == "__main__":
    # Configure Flow.
    prefect_client = PrefectClient()  # Local api key has been stored previously
    # prefect_client.login_to_tenant(tenant_slug=TENANT_SLUG)  # For user-scoped API token

    for gcs_uri, table_name in gcs_uri_table_map.items():
        # Run Parameters.
        VERSION_GROUP_ID = "bag_gcs_to_bq"
        RUN_NAME = f"bag_objects_{table_name}_{mirror_time}"

        parameters = {
            "bq_dataset_id": BQ_DATASET_ID,
            "gcp_location": GCP_LOCATION,
            "source_format": SOURCE_FORMAT,
            "write_disposition": WRITE_DISPOSITION,
            "gcs_uri": gcs_uri,
            "bq_table": table_name,
        }

        flow_run_id = prefect_client.create_flow_run(
            version_group_id=VERSION_GROUP_ID,
            run_name=RUN_NAME,
            parameters=parameters
        )
