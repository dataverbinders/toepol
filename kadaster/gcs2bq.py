import prefect
from prefect import Flow, Parameter, task
from prefect.executors import DaskExecutor
from prefect.tasks.gcp.bigquery import BigQueryLoadGoogleCloudStorage
from prefect.tasks.secrets import PrefectSecret


@task
def load_to_bq_from_gcs(gcs_uri, dataset, table_name, dataset_location, source_format, write_option, gcp_credentials):
    logger.info(f"Loading {gcs_uri} into {dataset}.{table_name}")

    BigQueryLoadGoogleCloudStorage().run(
        uri=gcs_uri, 
        dataset_id=dataset, 
        table=table_name, 
        credentials=gcp_credentials, 
        location=dataset_location, 
        source_format=source_format, 
        write_disposition=write_option)




with Flow("gcs2bq") as flow:
    # GCP Parameters
    GCS_URI = Parameter("gcs_uri")
    BQ_DATASET_ID = Parameter("bq_dataset_id")
    BQ_TABLE = Parameter("bq_table")
    GCP_LOCATION = Parameter("gcp_location")
    SOURCE_FORMAT = Parameter("source_format")
    WRITE_DISPOSITION = Parameter("write_disposition")

    GCP_CREDENTIALS = PrefectSecret("GCP_CREDENTIALS")

    logger = prefect.context.get("logger")

    load_to_bq_from_gcs(GCS_URI, BQ_DATASET_ID, BQ_TABLE, GCP_LOCATION, SOURCE_FORMAT, WRITE_DISPOSITION, GCP_CREDENTIALS)


if __name__ == "__main__":
    #  gcs_uploader = GCSUpload('')
#
     flow.executor = DaskExecutor()
     flow.register(
         project_name='toepol',
     )

    #################
    ### RUN LOCAL ###
    #################

    # flow.run()
