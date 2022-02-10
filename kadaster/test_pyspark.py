import logging
import os
import sys
from datetime import datetime
from zipfile import ZipFile

from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import col, date_format

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%y/%m/%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


# local files
# CWD = os.getcwd()
os.chdir("/Users/eddylim/Documents/work_repos/toepol/kadaster/bag/")
CWD = os.getcwd()
BAG_FILE = 'lvbag-extract-nl.zip'
# DATA_DIR = f'{CWD}/data'
DATA_DIR = f'{CWD}/output'


# GCP
TMP_BUCKET = 'temp-data-kadaster'
DATASET = 'testing'


# Object map
object_map = {
    'LIG': 'Ligplaats',
    'NUM': 'Nummeraanduiding',
    'OPR': 'OpenbareRuimte',
    'PND': 'Pand',
    'STA': 'Standplaats',
    'VBO': 'Verblijfsobject',
    'WPL': 'Woonplaats'
}


def init_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages',
                'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2,com.databricks:spark-xml_2.12:0.14.0') \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.conf.set('temporaryGcsBucket', TMP_BUCKET)
    # spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    # spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
    # spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    return spark


def extract_bag_zip():
    with ZipFile(BAG_FILE) as zip:
        zip.extractall(DATA_DIR)


def unzip_bag_data():
    logging.info('Extracting BAG zip')
    with ZipFile(BAG_FILE) as zip:
        zipnames = [f for f in zip.namelist() if f.endswith('.zip')]
        zip.extractall(DATA_DIR)

    file_dict = {}
    for name in zipnames:
        logging.info(f'Extracting {name}')
        with ZipFile(f"{DATA_DIR}/{name}") as zip:
            zip.extractall(f"{DATA_DIR}/{name.split('.')[0]}")
            files = [f"{DATA_DIR}/{name.split('.')[0]}/{f}" for f in zip.namelist() if f.endswith('.xml')]
        file_dict[name.split('.')[0]] = files

    return file_dict


def process_table(object_type, files):
    logging.info(f'Processing {object_type} xml files')

    df = spark.read.format('xml') \
        .option('rootTag', 'sl-bag-extract:bagStand') \
        .option('rowTag', 'sl-bag-extract:bagObject') \
        .option('path', files) \
        .load()
    df = df.select(f'Objecten:{object_type}.*')

    print(df.count())

    df.write.parquet(f"/Users/eddylim/{object_type}.parquet")

    # df.write \
    #     .format('bigquery') \
    #     .option('table', f'{DATASET}.{object_type}') \
    #     .save()


def read_parquet(object_type):
    logging.info(f'Reading {object_type} parquet files')

    df = spark.read.parquet(f"/Users/eddylim/{object_type}.parquet")

    # df_documentdatum = df.select('Objecten:documentdatum')
    df_documentdatum = df.sort(col("Objecten:documentdatum").asc()).select("Objecten:documentdatum")

    df_documentdatum.show(5)

    # print(df.count())

if __name__ == '__main__':
    spark = init_spark_session()
    file_dict = unzip_bag_data()

    # read xml files
    for key, val in file_dict.items():
        key_no_digit = ''.join([c for c in key if not c.isdigit()])
        bag_root = key_no_digit.split("/")[-1]
        print(bag_root)

        # print("key:", key)
        # print("val:", val)

        if bag_root in object_map.keys():
            # files = [f'file://{x}' for x in val]
            # joined_files = ','.join(files)

            joined_files = ','.join(val)
            # print(joined_files)

            object_type = object_map[bag_root]
            process_table(object_type, joined_files)
        
        # if bag_root in object_map.keys():
        #     object_type = object_map[bag_root]
        #     read_parquet(object_type)
