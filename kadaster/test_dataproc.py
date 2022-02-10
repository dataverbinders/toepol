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
CWD = os.getcwd()
BAG_FILE = 'lvbag-extract-nl.zip'
DATA_DIR = f'{CWD}/data'


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
        .master('yarn') \
        .appName('bq-test') \
        .config('spark.jars.packages',
                'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2,com.databricks:spark-xml_2.12:0.14.0') \
        .getOrCreate()

    spark.conf.set('temporaryGcsBucket', TMP_BUCKET)
    # spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    # spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
    # spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
    # spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
    # spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
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


def fix_broken_dates(df):
    df = df.withColumn('objecten:documentdatum',
                       functions.when(df['objecten:documentdatum'] < '1900-01-01',
                             datetime.strptime('1900-01-01', '%Y-%m-%d').date()) \
                                      .otherwise(df['objecten:documentdatum']))

    df_voorkomen = df.select('Objecten:voorkomen.*')
    df_hist = df_voorkomen.select('Historie:Voorkomen.*')

    df_hist = df_hist.withColumn('Historie:beginGeldigheid',
                                 functions.when(df['Historie:beginGeldigheid'] < '1900-01-01',
                                                datetime.strptime('1900-01-01', '%Y-%m-%d').date()) \
                                 .otherwise(df['Historie:beginGeldigheid']))

    #  df_LV = df_hist.select('Historie:BeschikbaarLV.*')
    #  df_LV.show()
    #  df_LV = df_LV.withColumn('Historie:tijdstipEindRegistratieLV',
                             #  functions.when(df['Historie:tijdstipEindRegistratieLV'] < '1900-01-01',
                                            #  datetime.strptime('1900-01-01', '%Y-%m-%d').date()) \
                             #  .otherwise(df['Historie:tijdstipEindRegistratieLV']))
    #  df_LV = df_LV.withColumn('Historie:tijdstipRegistratieLV',
                             #  functions.when(df['Historie:tijdstipRegistratieLV'] < '1900-01-01',
                                            #  datetime.strptime('1900-01-01', '%Y-%m-%d').date()) \
                             #  .otherwise(df['Historie:tijdstipRegistratieLV']))

    return df


def dates_to_string(df):
    df = df.withColumn('objecten:documentdatum', date_format(col('objecten:documentdatum'), 'yyyy-MM-dd'))

    df_voorkomen = df.select('Objecten:voorkomen.*')
    df_historie = df_voorkomen.select('Historie:Voorkomen.*')

    df_historie = df_historie.withColumn('Historie:beginGeldigheid', date_format(col('Historie:beginGeldigheid'), 'yyyy-MM-dd'))
    df_historie = df_historie.withColumn('Historie:eindGeldigheid', date_format(col('Historie:eindGeldigheid'), 'yyyy-MM-dd'))
    df_historie = df_historie.withColumn('Historie:eindRegistratie', date_format(col('Historie:eindRegistratie'), 'yyyy-MM-dd'))
    df_historie = df_historie.withColumn('Historie:tijdstipRegistratie', date_format(col('Historie:tijdstipRegistratie'), 'yyyy-MM-dd'))

    df_LV = df_historie.select('Historie:BeschikbaarLV.*')
    df_LV = df_LV.withColumn('Historie:tijdstipEindRegistratieLV', date_format(col('Historie:tijdstipEindRegistratieLV'), 'yyyy-MM-dd'))
    df_LV = df_LV.withColumn('Historie:tijdstipRegistratieLV', date_format(col('Historie:tijdstipRegistratieLV'), 'yyyy-MM-dd'))

    df_historie = df_historie.withColumn('Historie:BeschikbaarLV', df_LV)
    df_historie.show()

def process_table(object_type, files):
    logging.info(f'Processing {object_type} xml files')

    df = spark.read.format('xml') \
        .option('rootTag', 'sl-bag-extract:bagStand') \
        .option('rowTag', 'sl-bag-extract:bagObject') \
        .option('path', files) \
        .load()
    df = df.select(f'Objecten:{object_type}.*')
    df.write \
        .format('bigquery') \
        .option('table', f'{DATASET}.{object_type}') \
        .save()

if __name__ == '__main__':
    spark = init_spark_session()
    file_dict = unzip_bag_data()

    # read xml files
    for key, val in file_dict.items():

        key_no_digit = ''.join([c for c in key if not c.isdigit()])
        if key_no_digit in object_map.keys():
            logging.info("Pickle Rick")

            files = [f'file://{x}' for x in val]
            # files = [f'gs://{x}' for x in val]
            joined_files = ','.join(files)

            object_type = object_map[key_no_digit]
            process_table(object_type, joined_files)
