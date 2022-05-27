from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from math import floor
from pyspark.sql.functions import udf
from datetime import datetime

def init_spark_session():
    """Initializes and returns a PySpark session."""
    spark = (
        SparkSession.builder.appName("test")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", "temp-data-pyspark")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set(
        "spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED"
    )
    
    return spark


# @udf(returnType=FloatType())
def index_to_lat(index: int) -> float:
    return 50.75 + ((53.7 - 50.75) / 44700) * floor(index / 32700)


# @udf(returnType=FloatType())
def index_to_long(index: int) -> float:
    return 3.2 + ((7.22 - 3.2) / 32700) * (index % 32700)


def store_df_on_gcs(df: DataFrame, target_blob: str):
    df.write.format("parquet").mode("overwrite").save(
        target_blob
    )


def test(spark):
    total_dict = list()

    for i in range(1, 100):
        if i % 10 == 0:
            # print(f"After 10: {total_dict}")
            # print(len(total_dict))
            # print("--------------------------------------------------\n")
            df_schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("lat", FloatType(), True),
                StructField("long", FloatType(), True)
            ])
    
            df = spark.createDataFrame(total_dict, df_schema)
            # print(f"Create df at {datetime.now()}")
            # df.printSchema()
            df.show()
            df.write.mode("append").parquet("/Users/eddylim/Documents/work_repos/toepol/data/geo_index2")
            total_dict = list()
            
        total_dict.append(
            {"id": i,
            "lat": index_to_lat(i),
            "long": index_to_long(i)}
        )


def create_indx_table(spark):
    # test = list(range(0, 1000))
    total_dict = list()

    # df = spark.createDataFrame(test, IntegerType())

    # df = df.withColumn("id", col("value"))
    # df = df.drop("value")
    # df = df.withColumn("lat", index_to_lat(col("id")))
    # df = df.withColumn("long", index_to_long(col("id")))
    # df.show()
    # df.write.mode("overwrite").parquet("/Users/eddylim/Documents/work_repos/toepol/data/geo_index")

    # for i in range(1, 1185900001):
    # for i in range(1, 5000001):
    for i in range(1, 100000001):
        # if i % 5000000 == 0:
        if i % 2000000 == 0:
            df_schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("lat", FloatType(), True),
                StructField("long", FloatType(), True)
            ])
    
            df = spark.createDataFrame(total_dict, df_schema)
            print(f"Create df at {datetime.now()}")
            # df.printSchema()
            # df.show()
            # df.write.mode("append").parquet(f"/Users/eddylim/Documents/work_repos/toepol/data/geo_index2/test_{i}")
            df.write.mode("append").parquet("/Users/eddylim/Documents/work_repos/toepol/data/geo_index2/")
            total_dict = list()
            print(f"Created indices with lat and long coordinates until {i} at {datetime.now()}")
            
        total_dict.append(
            {"id": i,
            "lat": index_to_lat(i),
            "long": index_to_long(i)}
        )

    print(f"Created all indices with lat and long coordinates at {datetime.now()}")

    # df_schema = StructType([
    #     StructField("id", IntegerType(), True),
    #     StructField("lat", FloatType(), True),
    #     StructField("long", FloatType(), True)
    # ])
    
    # df = spark.createDataFrame(total_dict, df_schema)
    # print(f"Create df at {datetime.now()}")
    # df.printSchema()
    # df.show()
    # df.write.mode("append").parquet("/Users/eddylim/Documents/work_repos/toepol/data/geo_index2")

    # target = "gs://dataverbinders-dev/kadaster/bag/geoIndex"
    # store_df_on_gcs(df, target)


if __name__ == "__main__":
    spark = init_spark_session()
    create_indx_table(spark)

    # df = spark.read.load("/Users/eddylim/Documents/work_repos/toepol/data/geo_index2/")
    # df.show()

    # test(spark)