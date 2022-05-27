# import os, glob
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from google.cloud import storage
from pyspark.sql.functions import regexp_replace, col, split, explode, sequence, size, lit, arrays_zip, udf
from pyspark.sql.types import *
from marc_big_boi_brain import find_nearest_index
from math import floor, sqrt



bag_objecten = [
    "Woonplaats",
    "Nummeraanduiding",
    # "Ligplaats",
    # "Standplaats",
    # "Pand",
    # "Verblijfsobject",
]


def init_spark_session():
    """Initializes and returns a PySpark session."""
    spark = (
        SparkSession.builder.appName("bag2gcs")
        .getOrCreate()
    )
    
    return spark

def load_parquet(spark, object_name):
    # df = spark.read.load(f"gs://dataverbinders-dev/kadaster/bag/{object_name}/*.snappy.parquet")
    df = spark.read.load(f"gs://dataverbinders-dev/kadaster/bag/{object_name}")
    # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/Woonplaats")
    # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/{object_name}")

    return df


def store_df_on_gcs(df: DataFrame, target_blob: str):
    # df.repartition(1).write.format("parquet").mode("overwrite").save(
    #     target_blob
    # )

    # if "Objecten:geometrie" in df.columns:
    #     df = df.drop("Objecten:geometrie")

    df.write.format("parquet").mode("overwrite").save(
        target_blob
    )


def create_col_ids(df):
    # df = df.withColumn("identificatie", col("Objecten:identificatie._VALUE").cast("integer"))
    df = df.withColumn("identificatie", col("Objecten:identificatie._VALUE"))
    df = df.drop("Objecten:identificatie")

    df = df.withColumn("historieIdentificatie", col("Objecten:voorkomen.Historie:Voorkomen.Historie:voorkomenidentificatie"))
    df = df.drop("Objecten:voorkomen")

    return df

def extract_polygon(df):
    df = df.withColumn('geometry', regexp_replace(col('geometry.geo_polygon'), "POLYGON\\s\\(\\(", ""))
    df = df.withColumn('geometry', regexp_replace(col('geometry'), "\\)\\)", ""))
    df = df.withColumn('geometry', split(col("geometry"), "\\), \\("))
    
    df_outer = df
    df_outer = df_outer.withColumn('outerPolygon', col("geometry").getItem(0))
    df_outer = df_outer.withColumn('outerPolygon', split(col("outerPolygon"), ", "))
    df_outer = df_outer.withColumn('outerPolygon', arrays_zip(sequence(lit(0), size(col("outerPolygon")) - 1), col("outerPolygon")))
    df_outer = df_outer.withColumn("outerPolygon", explode(col("outerPolygon")))
    df_outer = df_outer.withColumn('geometry', split(col("outerPolygon.outerPolygon"), " "))
    df_outer = df_outer.withColumn('vlakID', lit(None))
    df_outer = df_outer.withColumn("puntId", col("outerPolygon.0"))

    df_outer = df_outer.withColumn('lat', col("geometry").getItem(0).cast("float"))
    df_outer = df_outer.withColumn('long', col("geometry").getItem(1).cast("float"))
    df_outer = df_outer.drop("geometry").drop("outerPolygon")

    # df_inner = df
    # df_inner = df_inner.withColumn('innerPolygon', col("geometry").getItem(1))
    # df_inner = df_inner.na.drop(subset=['innerPolygon'])
    # df_inner = df_inner.withColumn('innerPolygon', split(col("innerPolygon"), ", "))
    # df_inner = df_inner.withColumn('innerPolygon', arrays_zip(sequence(lit(-1), (-1 * (size(col("innerPolygon"))))), col("innerPolygon")))
    # df_inner = df_inner.withColumn("innerPolygon", explode(col("innerPolygon")))
    # df_inner = df_inner.withColumn('geometry', split(col("innerPolygon.innerPolygon"), " "))
    # df_inner = df_inner.withColumn("puntId", col("innerPolygon.0"))

    # df_inner = df_inner.withColumn('lat', col("geometry").getItem(0))
    # df_inner = df_inner.withColumn('long', col("geometry").getItem(1))
    # df_inner = df_inner.drop("geometry").drop("innerPolygon")

    # df = df_outer.union(df_inner)
    df_outer = df_outer.withColumn("refID", find_nearest_index(col("lat"), col("long")))
    # df_test.show()

    # df.show()
    # df_outer.show()
    # df_outer.write.format("csv").mode("overwrite").save("/Users/eddylim/Documents/work_repos/toepol/data/test.csv")

    return df_outer

def extract_multi_polygon(df):
    # Add another column based on the geometry column and remove all rows that have the value NULL.
    df = df.withColumn("geometry", col("geometry.geo_multi_polygon"))
    df = df.na.drop(subset=['geometry'])

    # Remove the Multipolygon ....
    df = df.withColumn("geometry", regexp_replace(col('geometry'), "MULTIPOLYGON\\s\\(\(\(", ""))
    df = df.withColumn("geometry", regexp_replace(col('geometry'), "\\)\\)\\)", ""))
    df = df.withColumn('geometry', split(col("geometry"), "\\)\\),\\s\\(\\("))

    # Zip the geometry column with a sequence to get an id number for each multipolygon entry.
    df = df.withColumn('geometry', arrays_zip(sequence(lit(0), size(col("geometry")) - 1), col("geometry")))
    df = df.withColumn("geometry", explode(col("geometry")))

    # Extract the id (sequence) and the geometry points from the zipped column geometry.
    df = df.withColumn("vlakID", col("geometry.0"))
    df = df.withColumn("geometry", col("geometry.geometry"))

    # Split the geometry points to seperate each polygon from the multipolygon.
    df = df.withColumn('geometry', split(col("geometry"), "\\),\\s\\("))
    df = df.withColumn('geometry', col("geometry").getItem(0))

    # Split each paired geometry point and zip these using the sequence to generate and id for each entry.
    # Explode the row to get ... entry having the same values for the other columns.
    df = df.withColumn('geometry', split(col("geometry"), ", "))
    df = df.withColumn('geometry', arrays_zip(sequence(lit(0), size(col("geometry")) - 1), col("geometry")))
    df = df.withColumn("geometry", explode(col("geometry")))

    # Extract the id (sequence) and the single paired geometry points.
    df = df.withColumn('puntID', col("geometry.0"))
    df = df.withColumn("geometry", col("geometry.geometry"))

    # Split the paired geometry points to get a latitude and longitude point.
    df = df.withColumn('geometry', split(col("geometry"), " "))
    df = df.withColumn('lat', col("geometry").getItem(0).cast("float"))
    df = df.withColumn('long', col("geometry").getItem(1).cast("float"))

    df = df.drop("geometry")


    # Convert lat/long coordinates from String to type float.
    df = df.withColumn("refId", find_nearest_index(col("lat"), col("long")))

    # df.show()
    # df.write.format("csv").mode("overwrite").save("/Users/eddylim/Documents/work_repos/toepol/data/test.csv")

    return df


def extract_point(df):
    df = df.withColumn("geometry", col("geometry.point"))
    df = df.withColumn('geometry', regexp_replace(col('geometry'), "POINT\\s\\(", ""))
    df = df.withColumn('geometry', regexp_replace(col('geometry'), "\\)", ""))

    df = df.withColumn('geometryPoints', arrays_zip(sequence(lit(0), size(col("geometry")) - 1), col("geometry")))
    df = df.withColumn("geometryPoints", explode(col("geometryPoints")))
    df = df.withColumn('geometry', split(col("geometryPoints.geometryPoints"), " "))
    df = df.withColumn('vlakID', lit(None))
    df = df.withColumn("puntId", col("geometryPoints.0"))

    df = df.withColumn('lat', col("geometryPoints").getItem(0).cast("float"))
    df = df.withColumn('long', col("geometryPoints").getItem(1).cast("float"))
    df.drop("geometry")

    
    df.show()


def create_geo_df(df, object):
    # df = df.withColumn('objType', lit("Woonplaats"))
    df = df.withColumn('objType', lit(object))
    df = create_col_ids(df)

    geo_df_columns = df.select("geometry.*").columns

    if "geo_multi_polygon" in geo_df_columns:
        df_mp = extract_multi_polygon(df)
        # df_mp.show()
        # pass

    
    if "geo_polygon" in geo_df_columns:
        df_polygon = extract_polygon(df)
        # df_polygon.show()
        # pass


    if "geo_point" in geo_df_columns:
        df_point = extract_point(df)
        # pass
    
    df = df_polygon.union(df_mp)
    # df = df_mp.union(df_polygon)

    # df.printSchema()
    
    df.show()
    # print(f"Length of df_mp: {df_mp.count()}\nLength of df_polygon: {df_polygon.count()}\nTotal Length: {df.count()}")

    # df.write.format("csv").mode("overwrite").option('nullValue', None).save("/Users/eddylim/Documents/work_repos/toepol/data/test.csv")
    # df.write.format("csv").mode("overwrite").option('nullValue', None).save("/Users/eddylim/Documents/work_repos/toepol/data/test.csv", header='true')
    # df.write.parquet("/Users/eddylim/Documents/work_repos/toepol/data/test")
    # df.write.format("parquet").mode("overwrite").save("/Users/eddylim/Documents/work_repos/toepol/data/test")

    return df




def extract_geography(spark, object):
    df = load_parquet(spark, object)

    if "geometry" in df.columns:
        df = df[["Objecten:identificatie", "Objecten:voorkomen", "geometry"]]
        df = create_geo_df(df, object)
        
        # blob_name = f"gs://dataverbinders-dev/kadaster/bag/dim_{object}"
        # print(blob_name)
        # store_df_on_gcs(df, blob_name)


if __name__ == "__main__":
    spark = init_spark_session()

    for i in bag_objecten:
        extract_geography(spark, i)