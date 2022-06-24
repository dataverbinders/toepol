from pyspark.sql import DataFrame, SparkSession
from google.cloud import storage
from pyspark.sql.functions import regexp_replace, col, split, explode, sequence, size, lit, arrays_zip, udf, length, expr, substring
from pyspark.sql.types import *
from math import floor, sqrt, ceil



bag_objecten = [
    "Woonplaats",
    "Nummeraanduiding",
    "Ligplaats",
    "Standplaats",
    "Pand",
    "Verblijfsobject",
]

def index_to_lat(index: int) -> float:
    return 50.75 + ((53.7 - 50.75) / 44700) * floor(index / 32700)


def index_to_long(index: int) -> float:
    return 3.2 + ((7.22 - 3.2) / 32700) * (index % 32700)


def index_to_lat_long(index: int):
    return (index_to_lat(index), index_to_long(index))


def lat_long_to_index(lat: float, long: float) -> int:
    i_lat = (lat - 50.75) / ((53.7 - 50.75) / 44700)
    i_long = (long - 3.2) / ((7.22 - 3.2) / 32700)
    index = i_long + 32700 * i_lat

    return index


@udf(returnType=IntegerType())
def find_nearest_index(lat: float, long: float) -> int:
    approx_index = floor(lat_long_to_index(lat, long))

    nearest_index = None
    nearest_index_dist = float("inf")

    lat_offset = [0]
    if approx_index % 32700 != 0:
        lat_offset.append(-1)
    if approx_index % 32700 != 32699:
        lat_offset.append(1)

    long_offset = [0]
    if approx_index >= 32700:
        long_offset.append(-1)
    if approx_index <= (44700 * 32700 - 32700):
        long_offset.append(1)

    for lat_o in lat_offset:
        for long_o in long_offset:
            comp_index = approx_index + lat_o + long_o * 32700
            comp_lat, comp_long = index_to_lat_long(comp_index)

            dist = sqrt((lat - comp_lat) ** 2 + (long - comp_long) ** 2)
            if dist < nearest_index_dist:
                nearest_index = comp_index
                nearest_index_dist = dist

    return nearest_index


def init_spark_session():
    """Initializes and returns a PySpark session."""
    spark = (
        SparkSession.builder.appName("bag2gcs")
        .getOrCreate()
    )
    
    return spark

def load_parquet(spark, object_name):
    # df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/test_wpl_poly/*.parquet")
    if object_name in ["Pand", "Verblijfsobject"]:
        df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/bag_output/{object_name}/*/*.parquet")
        # df = spark.read.load(f"gs://dataverbinders-dev/kadaster/bag/{object_name}/*/")
    else:
        df = spark.read.load(f"/Users/eddylim/Documents/work_repos/toepol/data/bag_output/{object_name}/")
        # df = spark.read.load(f"gs://dataverbinders-dev/kadaster/bag/{object_name}/")

    return df


def store_df_on_gcs(df: DataFrame, target_blob: str):
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
    df_outer = df_outer.withColumn("puntID", col("outerPolygon.0"))

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
    # df_outer = df_outer.withColumn('vlakID', lit(None))
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
    df = df.withColumn('vlakID', lit(None))
    df = df.withColumn('puntID', lit(None))
    df = df.withColumn("geometry", col("geometry.geo_point"))
    df = df.na.drop(subset=['geometry'])
    df = df.withColumn('geometry', regexp_replace(col('geometry'), "POINT\\s\\(", ""))
    df = df.withColumn('geometry', regexp_replace(col('geometry'), "\\)", ""))
    df = df.withColumn('geometry', split(col("geometry"), " "))

    df = df.withColumn('lat', col("geometry").getItem(0).cast("float"))
    df = df.withColumn('long', col("geometry").getItem(1).cast("float"))

    df = df.drop("geometry")
    df = df.withColumn("refID", find_nearest_index(col("lat"), col("long")))
    # df.show()

    return df


def create_df_list(df, object):
    # df = df.withColumn('objType', lit("Woonplaats"))
    df = df.withColumn('objType', lit(object))
    df = create_col_ids(df)

    geo_df_columns = df.select("geometry.*").columns
    # print(df.count())
    df_list = list()

    if "geo_multi_polygon" in geo_df_columns:
        df_mp = extract_multi_polygon(df)
        # df_mp.show()
        # pass
        df_list.append(df_mp)
    # print(df.count())
    
    if "geo_polygon" in geo_df_columns:
        df_polygon = extract_polygon(df)
        df_list.append(df_polygon)
        
        # pass
    

    if "geo_point" in geo_df_columns:
        df_point = extract_point(df)
        # df_point.show()
        df_list.append(df_point)
        # pass
    

    # df.printSchema()

    # print(df.count())
    
    # df.show()
    # df_new.show()

    # df.write.format("parquet").mode("overwrite").save("/Users/eddylim/Documents/work_repos/toepol/data/test")

    # df_new = df_new.na.fill("")
    # df_new.na.fill("")
    # df_new.show()
    
    return df_list


def create_geo_df(list_df):    
    emptyRDD = spark.sparkContext.emptyRDD()
    schema = StructType([
        StructField("objType", StringType(), False),
        StructField("identificatie", LongType(), True),
        StructField("historieIdentificatie", LongType(), True),
        StructField("vlakID", IntegerType(), True),
        StructField("puntID", IntegerType(), True),
        StructField("lat", FloatType(), True),
        StructField("long", FloatType(), True),
        StructField("refID", IntegerType(), True),
    ])

    df_new = spark.createDataFrame(emptyRDD, schema)

    for dfs in list_df:
        df_new = df_new.union(dfs)
    
    # print(df_new.count())

    return df_new


def extract_geography(spark, object):
    df = load_parquet(spark, object)

    if "geometry" in df.columns:
        df = df[["Objecten:identificatie", "Objecten:voorkomen", "geometry"]]
        dfs = create_df_list(df, object)
        df = create_geo_df(dfs)
        
        # blob_name = f"gs://dataverbinders-dev/kadaster/bag/dim_{object}"
        blob_name = f"/Users/eddylim/Documents/work_repos/toepol/data/test_geo/{object}"
        # print(blob_name)
        store_df_on_gcs(df, blob_name)


if __name__ == "__main__":
    spark = init_spark_session()

    for i in bag_objecten:
        extract_geography(spark, i)
    


