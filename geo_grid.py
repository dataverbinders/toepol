from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from math import floor
from datetime import datetime

def init_spark_session():
    """Initializes and returns a PySpark session."""
    spark = (
        SparkSession.builder.appName("test")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set(
        "spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED"
    )
    
    return spark


def index_to_lat(index: int) -> float:
    return 50.75 + ((53.7 - 50.75) / 44700) * floor(index / 32700)


def index_to_long(index: int) -> float:
    return 3.2 + ((7.22 - 3.2) / 32700) * (index % 32700)


def store_df_on_gcs(df: DataFrame, target_blob: str):
    # df.write.format("parquet").mode("overwrite").save(
    #     target_blob
    # )

    df.write.format("parquet").mode("overwrite").save(
        f"gs://dataverbinders-dev/kadaster/bag/geoIndex/{target_blob}"
    )


def create_indx_table(spark):
    total_dict = list()
    df_schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("lat", FloatType(), True),
                StructField("long", FloatType(), True)
            ])

    for i in range(0, 1185900001):
        total_dict.append(
            {"id": i,
            "lat": index_to_lat(i),
            "long": index_to_long(i)}
        )
        
        if i % 1000000 == 0 or i == 1185900000:
            df = spark.createDataFrame(total_dict, df_schema)
            print(f"Create df at {datetime.now()}")
            
            store_df_on_gcs(df, f"part_{i}")
            total_dict = list()
            print(f"Created indices with lat and long coordinates until {i} at {datetime.now()}")


if __name__ == "__main__":
    spark = init_spark_session()
    create_indx_table(spark)