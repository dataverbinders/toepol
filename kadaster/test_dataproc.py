import logging
import os
from sys import stdout
from zipfile import ZipFile

from pyspark.sql import SparkSession


# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%y/%m/%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(stdout)
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
    """
    Initiates a spark session.

    Returns:
        SparkSession
    """
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('bq-test') \
        .config('spark.jars.packages',
                'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2,com.databricks:spark-xml_2.12:0.14.0') \
        .getOrCreate()

    spark.conf.set('temporaryGcsBucket', TMP_BUCKET)
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead",
                   "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite",
                   "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead",
                   "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite",
                   "CORRECTED")

    return spark


def unzip_bag_data():
    """
    Unzips the lvbag-extract-nl.zip to its individual zips.
    Then unzips those zips to individual folders.

    Returns:
        dict: a dictionary with the object type as key, and a list of the
            corresponding file paths.
    """
    logging.info('Extracting BAG zip')
    with ZipFile(BAG_FILE) as zip:
        zipnames = [f for f in zip.namelist() if f.endswith('.zip')]
        zip.extractall(DATA_DIR)

    file_dict = {}
    for name in zipnames:
        logging.info(f'Extracting {name}')
        with ZipFile(f"{DATA_DIR}/{name}") as zip:
            zip.extractall(f"{DATA_DIR}/{name.split('.')[0]}")
            files = [f"{DATA_DIR}/{name.split('.')[0]}/{f}"
                     for f in zip.namelist() if f.endswith('.xml')]
        file_dict[name.split('.')[0]] = files

    return file_dict


def process_table(object_type, files):
    """
    Processes the XML files of a given type.

    Parameters:
        object_type (str): the object type to process (i.e. 'Woonplaats')
        files (str): A string containing all files to process. Joined using
            a comma. (i.e. 'file1,file2')
    """
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
            files = [f'file://{x}' for x in val]
            joined_files = ','.join(files)

            object_type = object_map[key_no_digit]
            process_table(object_type, joined_files)
