import logging
import os
from os import listdir, remove
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


# bag zip url
BAG_URL = None


# local files
CWD = os.getcwd()
BAG_FILE = 'lvbag-extract-nl.zip'
DATA_DIR = f'{CWD}/data'


# GCP
TMP_BUCKET = 'temp-data-kadaster'
DATASET = 'testing'


# Object maps
object_map = {
    'WPL': 'Woonplaats',
    'OPR': 'OpenbareRuimte',
    'NUM': 'Nummeraanduiding',
    'LIG': 'Ligplaats',
    'STA': 'Standplaats',
    'PND': 'Pand',
    'VBO': 'Verblijfsobject',
    'IAWPL': 'Woonplaats_IA',
    'IAOPR': 'OpenbareRuimte_IA',
    'IANUM': 'Nummeraanduiding_IA',
    'IALIG': 'Ligplaats_IA',
    'IASTA': 'Standplaats_IA',
    'IAPND': 'Pand_IA',
    'IAVBO': 'Verblijfsobject_A',
    'NBWPL': 'Woonplaats_NB',
    'NBOPR': 'OpenbareRuimte_NB',
    'NBNUM': 'Nummeraanduiding_NB',
    'NBLIG': 'Ligplaats_NB',
    'NBSTA': 'Standplaats_NB',
    'NBPND': 'Pand_NB',
    'NBVBO': 'Verblijfsobject_NB',
}
inonderzoek_map = {
    'IOWPL': 'KenmerkWoonplaatsInOnderzoek',
    'IOOPR': 'KenmerkOpenbareRuimteInOnderzoek',
    'IONUM': 'KenmerkNummeraanduidingInOnderzoek',
    'IOLIG': 'KenmerkLigplaatsInOnderzoek',
    'IOSTA': 'KenmerkStandplaatsInOnderzoek',
    'IOPND': 'KenmerkPandInOnderzoek',
    'IOVBO': 'KenmerkVerblijfsobjectInOnderzoek',
}


def init_spark_session():
    """
    Initiates a spark session.

    Returns:
        SparkSession
    """
    spark = SparkSession \
        .builder \
        .master('yarn') \
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
    # extract zips on depth 1 (i.e. 9999LIG<date>.zip)
    for name in zipnames:
        logging.info(f'Extracting {name}')
        with ZipFile(f"{DATA_DIR}/{name}") as zip:
            zip.extractall(f"{DATA_DIR}/{name.split('.')[0]}")
            files = [f"{DATA_DIR}/{name.split('.')[0]}/{f}"
                     for f in zip.namelist() if f.endswith('.xml')]
        file_dict[name.split('.')[0]] = files
        remove(f'{DATA_DIR}/{name}')

    # extract zips on depth 2 (InOnderzoek, Inactief, NietBAG)
    for directory in listdir(DATA_DIR):
        dir_no_digit = ''.join([c for c in directory if not c.isdigit()])
        if dir_no_digit in ['InOnderzoek', 'Inactief', 'NietBag']:
            zipnames = listdir(f'{DATA_DIR}/{directory}')
            for name in zipnames:
                if not name.endswith('.zip'):
                    continue
                logging.info(f'Extracting {name}')
                with ZipFile(f'{DATA_DIR}/{directory}/{name}') as zip:
                    zip.extractall(f"{DATA_DIR}/{directory}/{name.split('.')[0]}")
                    files = [f"{DATA_DIR}/{directory}/{name.split('.')[0]}/{f}"
                             for f in zip.namelist() if f.endswith('.xml')]
                file_dict[name.split('.')[0]] = files
                remove(f'{DATA_DIR}/{directory}/{name}')

    return file_dict


def process_table(object_type, files, root_tag, row_tag, select_tag):
    """
    Processes the XML files of a given type.

    Parameters:
        object_type (str): the object type to process (i.e. 'Woonplaats')
        files (str): A string containing all files to process. Joined using
            a comma. (i.e. 'file1,file2')
    """
    try:
        logging.info(f'Processing {object_type} xml files')

        df = spark.read.format('xml') \
            .option('rootTag', root_tag) \
            .option('rowTag', row_tag) \
            .option('path', files) \
            .load()
        df = df.select(f"{select_tag}:{object_type.split('_')[0]}.*")
        df.write \
            .format('bigquery') \
            .option('table', f'{DATASET}.{object_type}') \
            .mode('overwrite') \
            .save()
    except:
        logging.warn(f'Unable to load {object_type} files.')

if __name__ == '__main__':
    spark = init_spark_session()
    file_dict = unzip_bag_data()

    # read xml files
    for key, val in file_dict.items():

        key_no_digit = ''.join([c for c in key if not c.isdigit()])
        if key_no_digit in object_map.keys():
            logging.info(key_no_digit)
            files = [f'file://{x}' for x in val]
            joined_files = ','.join(files)

            object_type = object_map[key_no_digit]
            process_table(object_type, joined_files,
                          'sl-bag-extract:bagStand',
                          'sl-bag-extract:bagObject',
                          'Objecten')

            continue

        if key_no_digit in inonderzoek_map.keys():
            logging.info(f'InOnderzoek: {key_no_digit}')
            files = [f'file://{x}' for x in val]
            joined_files = ','.join(files)

            object_type = inonderzoek_map[key_no_digit]
            process_table(object_type, joined_files,
                          'sl-bag-extract:bagStand',
                          'sl-bag-extract:kenmerkInOnderzoek',
                          'KenmerkInOnderzoek')

            continue

        #  if key_no_digit in inactief_map.keys():
            #  continue
#
        #  if key_no_digit in nietbag_map.keys():
            #  continue
