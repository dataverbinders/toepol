import logging
import os
import sys
from datetime import datetime
from zipfile import ZipFile
import re
from pyproj import Proj, transform
from pyproj import Transformer
from pyspark import SQLContext
from pyspark.sql import SparkSession, functions, Window
from pyspark.sql.functions import col, monotonically_increasing_id, row_number

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
        .option('dateFormat', 'ISO_LOCAL_DATE') \
        .option('path', files) \
        .load()
    df = df.select(f'Objecten:{object_type}.*')

    print(df.count())

    df.printSchema()

    # if "objecten:geomertie" in (name.lower() for name in df.columms()):
    #     print(df["Objecten:geomertie"])

    df.select("Objecten:geometrie.Objecten:vlak.gml:Polygon.gml:exterior.gml:LinearRing.gml:posList._VALUE").show(1)
    
    if "Objecten:geometrie" in df.columns:
        geo_list = df.select("Objecten:geometrie.Objecten:vlak.gml:Polygon.gml:exterior.gml:LinearRing.gml:posList._VALUE").rdd.flatMap(lambda x: x).collect()
    
    print(len(geo_list))
    
    new_coords = []
    
    for i in geo_list:
        tmp_list = []

        if i is not None:
            # Split the String in apris of coordinates.
            for j in re.split('(\s?\d+\.\d+\s\d+\.\d+\s?)', i):
                tmp_list2 = []
                # Convert the captured coordinates seperately into float and save as nested list.
                if j:
                    for k in j.split():
                        tmp_list2.append(float(k))
                    tmp_list.append(tmp_list2)
        
        new_coords.append(tmp_list)
    
    inProj = Proj(init='epsg:28992') # Amersfoort / RD New -- Netherlands - Holland - Dutch
    outProj = Proj(init='epsg:4326') # WGS 84 -- WGS84 - World Geodetic System 1984, used in GPS

    transformer = Transformer.from_crs('epsg:28992', 'epsg:4326')

    # print(len(new_coords))
    # print(new_coords[0])
    # print(new_coords[0][0])

    new_coord_list = []
    # list_tmp = []

    # for i in new_coords[0]:
    #     x1, y1 = i
    #     # print(x1, y1)

    #     x2,y2 = transform(inProj,outProj,x1,y1) # To epsg:4326
    #     # print(y2, x2)

    #     list_tmp.append(str(y2 ) + " " + str(x2))
    
    # coord_string = ", ".join(list_tmp)

    # print(coord_string)

    for i in new_coords:
        list_tmp = []
        for coords_pair in i:
            x1, y1 = coords_pair
            # print(x1, y1)

            # x2, y2 = transform(inProj,outProj,x1,y1) # To epsg:4326
            x2, y2 = transformer.transform(x1, y1) # To epsg:4326
            # print(y2, x2)
            list_tmp.append(str(x2) + " " + str(y2))
        
        coord_string = ", ".join(list_tmp)
        print(coord_string)


        new_coord_list.append(coord_string)
    
    print(len(new_coord_list))
    # print(new_coord_list[0])




    # df.write.parquet(f"/Users/eddylim/{object_type}.parquet")

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
