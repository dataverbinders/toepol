import json, os
import prefect
from prefect import task,Parameter, Flow
from google.cloud import storage
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def export_schema(object_type, df_schema, gcp_credential):
    client = storage.Client.from_service_account_json(
        gcp_credential
    )
    bucket = client.bucket("temp-prefect-data")
    blob = bucket.blob(f"bag/schemas/{object_type}_schema.json")
    blob.upload_from_string(
        data=json.dumps(df_schema.jsonValue()),
        content_type="application/json",
    )

@task
def load_schema(object_type, gcp_credential):
    client = storage.Client.from_service_account_json(
        gcp_credential
    )
    bucket = client.bucket("temp-prefect-data")
    blob = bucket.blob(f"bag/schemas/{object_type}_schema.json")

    with blob.open(mode="rt") as schema_file:
        df_schema = StructType.fromJson(json.load(schema_file))
    
    return df_schema


@task
def export_schema_spark_df(
    object_type: str, gcp_credential
) -> DataFrame:
    """Given a string containing xml files, returns the schema of the DataFrame..

    :param spark: a spark session
    :type spark: SparkSession
    :param object_type: the type of object in the given files. eg: 'Woonplaats'
    :type object_type: str
    :param files: a string containing a concatenation of the XML files
    :type files: str
    :rtype: DataFrame
    """
    spark = (
        SparkSession.builder.appName("bag2gcs")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", "temp-data-pyspark")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    
    df = spark.read.load(f"{os.getcwd()}/data/bag_output/Nummeraanduiding/*.parquet")

    # print(df.schema)
    
    # with open(f"{os.getcwd()}/{object_type}_schema.json", "w") as wr:
    #     json.dump(df.schema.jsonValue(), wr)

    export_schema(object_type, df.schema, gcp_credential)


@task
def test(schema_df):
    print(schema_df)
    spark = (
        SparkSession.builder.appName("bag2gcs")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", "temp-data-pyspark")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    
    df = spark.read.schema(schema_df).load(f"{os.getcwd()}/Documents/work_repos/toepol/data/bag_output/Nummeraanduiding/*.parquet")

    logger.info(df.count())


with Flow("test_flow") as test_flow:
    logger = prefect.context.get("logger")
    # gcp_credentials = "/Users/eddylim/Documents/gcp_keys/prefect_key.json"
    gcp_credentials = Parameter("gcp_credentials", "/Users/eddylim/Documents/gcp_keys/prefect_key.json")
    bag_object = Parameter("bag_object", "Nummeraanduiding")
    # export_schema_spark_df("Nummeraanduiding", gcp_credentials)
    df_schema = load_schema(bag_object, gcp_credentials)
    test(df_schema)
    

if __name__ == "__main__":
    test_flow.register(project_name="toepol", labels=["test"])
    # test_flow.run()