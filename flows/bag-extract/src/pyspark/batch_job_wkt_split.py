from pyspark.sql import DataFrame, SparkSession
from google.cloud import storage
from pyspark.sql.functions import length, substring, lit
from pyspark.sql.types import *
from math import ceil



bag_objecten = [
    "Woonplaats",
    "Nummeraanduiding",
    "Ligplaats",
    "Standplaats",
    "Pand",
]


def init_spark_session():
    """Initializes and returns a PySpark session."""
    spark = (
        SparkSession.builder.appName("geo4pbi")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    
    return spark


def get_file_uris(object_key: str) -> str:
    client = storage.Client()

    blobs = list(
        client.list_blobs(
            bucket_or_name="dataverbinders-dev", prefix=f"kadaster/bag/{object_key}"
        )
    )
    uris = [f"gs://{blob.bucket.name}/{blob.name}" for blob in blobs if blob.name.endswith(".snappy.parquet")]

    return uris


def get_folder_names(object):
    client = storage.Client()
    
    bucket = client.get_bucket(bucket_or_name="dataverbinders-dev")
    blob_iter = bucket.list_blobs(prefix=f"kadaster/bag/{object}/", delimiter="/")

    list(blob_iter)
    parts = list(blob_iter.prefixes)

    return parts


def get_underlying_files(prefix):
    client = storage.Client()

    blobs = client.list_blobs(
        bucket_or_name="dataverbinders-dev", prefix=prefix
    )

    files = [f"gs://{blob.bucket.name}/{blob.name}" for blob in blobs if blob.name.endswith("snappy.parquet")]

    return files


def load_parts_parquet(spark, path_parquet):
    df = spark.read.load(path_parquet)

    return df


def store_df_on_gcs(df: DataFrame, target_blob: str):
    df.write.format("parquet").mode("overwrite").save(
        target_blob
    )


def split_cols(df, geo_type):
    df_tmp = df.withColumn('geometry_length', length(f'geometry.{geo_type}'))
    max_value = df_tmp.agg({"geometry_length": "max"}).collect()[0][0]
    df_tmp = df_tmp.drop('geometry_length')
    # print(max_value)

    num_cols = ceil(max_value / 30000)
    end_point = 1

    # concept for dividing into multiple columns:
    for i in range(num_cols):
        df_tmp = df_tmp.withColumn(f"{geo_type}_{i}", substring(f"geometry.{geo_type}", end_point, 30000))
        end_point += 30000

    return df_tmp


def split_col_geometry(df):
    geo_df_columns = df.select("geometry.*").columns
    
    for i in geo_df_columns:
        df = split_cols(df, i)

    return df


if __name__ == "__main__":
    spark = init_spark_session()

    for i in bag_objecten:
        files = get_file_uris(i)
        df = spark.read.parquet(*files)

        if "geometry" in df.columns:
            print(i)
            df = split_col_geometry(df)

            if i in ["Pand"]:
                list_parts = get_folder_names(i)

                emptyRDD = spark.sparkContext.emptyRDD()
                df_schema = df.schema
                df_structure = spark.createDataFrame(emptyRDD, df_schema)

                for j in list_parts:
                    part_file = get_underlying_files(j)
                    df = spark.read.parquet(*part_file)
                    df = split_col_geometry(df) 

                    for column in df_structure.columns:
                        # print(col)
                        if column not in df.columns:
                            df = df.withColumn(column, lit(None).cast("string"))

                    store_df_on_gcs(df, f"gs://dataverbinders-dev/kadaster/bag/{i}_geo_splitted/{j.split('/')[-2]}/")

            
            else:
                store_df_on_gcs(df, f"gs://dataverbinders-dev/kadaster/bag/{i}_geo_splitted/")
    


