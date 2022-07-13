from pyspark.sql import DataFrame, SparkSession
from google.cloud import storage
from pyspark.sql.functions import length, substring, lit
from pyspark.sql.types import *
from math import ceil



bag_objecten = [
    "Woonplaats",
    # "Nummeraanduiding",
    # "Ligplaats",
    # "Standplaats",
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

def load_parquet(spark, object_name):
# def load_parquet(path_parquet):
    # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/test2/{object_name}/*/*.parquet")
    # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/bag_output/{object_name}/*/*.parquet")
    if object_name in ["Pand", "Verblijfsobject"]:
        # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/{object_name}/*/")
        # df = spark.read.load(f"gs://dataverbinders-dev/kadaster/bag/{object_name}/*/")
        df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/bag_output/{object_name}/*/*.parquet")
    else:
        # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/{object_name}/")
        # df = spark.read.load(f"gs://dataverbinders-dev/kadaster/bag/{object_name}/")
        # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/bag_output/{object_name}/*.parquet")
        df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol_old/data/bag_output/{object_name}/*.parquet")

    # df = spark.read.load(path_parquet)

    return df


def get_file_uris(object_key: str) -> str:
    client = storage.Client()

    blobs = list(
        client.list_blobs(
            bucket_or_name="dataverbinders-dev", prefix=f"kadaster/bag/{object_key}/"
        )
    )
    uris = [f"gs://{blob.bucket.name}/{blob.name}" for blob in blobs]
    uris = ",".join(uris)

    return uris


def get_folder_names(object):
    client = storage.Client()
    # client = storage.Client().from_service_account_json(
    #     "/Users/eddylim/Documents/gcp_keys/prefect_key.json"
    # )

    blobs = client.list_blobs(
        # bucket_or_name="dataverbinders-dev", prefix=f"kadaster/bag/{object}/", delimiter='/'
        bucket_or_name="dataverbinders-dev", prefix=f"kadaster/bag/{object}/"
    )
        
    parts = set(
        ["/".join(blob.name.split("/")[:-1]) for blob in blobs if blob.name.endswith(".snappy.parquet")]
    )

    return parts


def get_underlying_files(prefix):
    client = storage.Client()

    blobs = client.list_blobs(
        bucket_or_name="dataverbinders-dev", prefix=prefix
    )

    files = ",".join(
        [f"gs://{blob.bucket.name}/{blob.name}" for blob in blobs]
    )

    return files


def load_parts_parquet(spark, path_parquet):
    df = spark.read.load(path_parquet)

    return df



def store_df_on_gcs(df: DataFrame, target_blob: str):
    df.write.format("parquet").mode("overwrite").save(
        target_blob
    )
    # df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(
    #     target_blob
    # )
    # df.repartition(1).write.csv(f"{target_blob}", mode="overwrite", header=True)


def split_cols(df, geo_type):
    df_tmp = df.withColumn('geometry_length', length(f'geometry.{geo_type}'))
    max_value = df_tmp.agg({"geometry_length": "max"}).collect()[0][0]
    df_tmp = df_tmp.drop('geometry_length')
    # print(max_value)

    num_cols = ceil(max_value / 30000)
    end_point = 1

    # concept for dividing into multiple columns:
    for i in range(num_cols):
        # print(f"Iteration number: {i}")
        # df_polygon = df_polygon.withColumn(f"geo_polygon_{i}", expr(f"substring(geo_poly, {end_point + 1}, {end_point + 30000})"))
        # df_polygon = df_polygon.withColumn(f"geo_polygon_{i}", substring("geo_poly", end_point, 30000))
        df_tmp = df_tmp.withColumn(f"{geo_type}_{i}", substring(f"geometry.{geo_type}", end_point, 30000))
        end_point += 30000

    return df_tmp


def split_col_geometry(df):
    geo_df_columns = df.select("geometry.*").columns
    # df = df.withColumn("identificatie", col("Objecten:identificatie._VALUE"))
    # df = df.withColumn("naam", col("Objecten:naam"))
    print(geo_df_columns)
    for i in geo_df_columns:
        df = split_cols(df, i)

    # if "geo_multi_polygon" in geo_df_columns:
    #     df_geom = df.select(df.columns[9:])
    # elif "geo_point" in geo_df_columns:
    #     df_geom = df.select(df.columns[13:])
    # else: 
    #     df_geom = df.select(df.columns[10:])
    # print(df.columns)
    
    # df_geom.repartition(1).write.csv("/Users/eddylim/test.csv", mode="overwrite", header=True)

    return df


# def split_geography(spark, object):
# def split_geography(df, object_name):
#     if "geometry" in df.columns:
#         print(object_name)
#         df = split_col_geometry(df)

#         return df
#         # store_df_on_gcs(df, f"{local_dir}/{object_name}3")


if __name__ == "__main__":
    spark = init_spark_session()
    # local_dir = "/Users/eddylim/Documents/work_repos/toepol_old/data/bag_output"
    # output_dir = "/Users/eddylim/Documents/work_repos/toepol_old/data/geo_test"

    for i in bag_objecten:
        files = get_file_uris(i)
        # df = load_parquet(spark, i) # For local
        df = spark.read.load(files) 
        
        if "geometry" in df.columns:
            print(i)
            df = split_col_geometry(df)

            if i in ["Pand"]:
                # df.printSchema()
                list_parts = get_folder_names(i)

                emptyRDD = spark.sparkContext.emptyRDD()
                df_schema = df.schema
                df_structure = spark.createDataFrame(emptyRDD, df_schema)

                # print(list_parts)
                for j in list_parts:
                    # df = load_parts_parquet(spark, get_underlying_files(j))
                    df = spark.read.load(get_underlying_files(j))
                    df = split_col_geometry(df) 

                    for column in df_structure.columns:
                        # print(col)
                        if column not in df.columns:
                            df = df.withColumn(column, lit(None).cast("string"))
                            # df = df.withColumn("geometry", struct(*[lit(None).alias(f"{column}"), col("geometry")["geo_point"].alias("geo_point")]))
                            # print(column)

                    store_df_on_gcs(df, f"gs://dataverbinders-dev/kadaster/bag/{i}_geo_splitted/{j.split('/')[-1]}")

            
            else:
                # store_df_on_gcs(df, f"{output_dir}/{i}3") # For local
                store_df_on_gcs(df, f"gs://dataverbinders-dev/kadaster/bag/{i}_geo_splitted/")
    


