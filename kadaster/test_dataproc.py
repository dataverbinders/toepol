import logging
import os
from os import remove
from sys import stdout
from zipfile import ZipFile

import requests
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct
from shapely import geometry, wkt

from pyproj import Proj, Transformer, transform


# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%y/%m/%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(stdout)
    ]
)


# download
BAG_URL = 'https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'
DOWNLOAD_BAG = True


# local files
CWD = os.getcwd()
BAG_FILE = 'lvbag-extract-nl.zip'
DATA_DIR = f'{CWD}/data'


# GCP
TMP_BUCKET = 'temp-data-pyspark'
DATASET = 'bag'


# Object maps
object_map = {
    'WPL': 'Woonplaats',
    'OPR': 'OpenbareRuimte',
    'NUM': 'Nummeraanduiding',
    'LIG': 'Ligplaats',
    'STA': 'Standplaats',
    'PND': 'Pand',
    'VBO': 'Verblijfsobject',
}


def download_bag_zip():
    logging.info('Downloading bag file.')
    r = requests.get(BAG_URL)
    with open(BAG_FILE, 'wb') as f:
        f.write(r.content)


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
    return file_dict


def transform_coords(x, y):
    x, y = transformer.transform(x, y)
    return [x, y]


def convert_punt(s: str):
    x, y, _ = s.split()
    x, y = transform_coords(x, y)

    p = wkt.dumps(geometry.Point(x, y))
    return p


def create_linear_ring(s: str, dimension=2):
    points_list = []
    s = s.split()
    for i in range(0, len(s), dimension):
        x, y = transform_coords(s[i], s[i+1])
        points_list.append([x, y])
    lr = geometry.LinearRing(points_list)
    return lr


def process_table(object_type, files, root_tag, row_tag, select_tag):
    """
    Processes the XML files of a given type.

    Parameters:
        object_type (str): the object type to process (i.e. 'Woonplaats')
        files (str): A string containing all files to process. Joined using
            a comma. (i.e. 'file1,file2')
    """
    logging.info(f'Processing {object_type} xml files')

    df = spark.read.format('xml') \
        .option('rootTag', root_tag) \
        .option('rowTag', row_tag) \
        .option('path', files) \
        .load()
    df = df.select(f"{select_tag}:{object_type.split('_')[0]}.*")
    if 'Objecten:geometrie' in df.columns:
        new_geo_cols = []
        geo_df = df.select('Objecten:geometrie.*')

        if 'Objecten:multivlak' in geo_df.columns:
            new_multi_polygons = []

            for row in df.collect():
                id = row['Objecten:identificatie']['_VALUE']
                voorkomen = row['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie']

                if row['Objecten:geometrie']['Objecten:multivlak'] is not None:
                    multi_surface = row['Objecten:geometrie']['Objecten:multivlak']['gml:MultiSurface']['gml:surfaceMember']
                    polygons = []
                    for surface in multi_surface:
                        # exterior
                        positions = surface['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList']['_VALUE']
                        exterior_lr = create_linear_ring(positions)

                        # interiors
                        interiors = surface['gml:Polygon']['gml:interior']
                        interior_lrs = []
                        if interiors is not None:
                            for interior in interiors:
                                positions = interior['gml:LinearRing']['gml:posList']['_VALUE']
                                interior_lr = create_linear_ring(positions)
                                interior_lrs.append(interior_lr)
                        polygon = geometry.Polygon(exterior_lr, interior_lrs)
                        polygons.append(polygon)
                    multipolygon = wkt.dumps(geometry.MultiPolygon(polygons))
                    new_multi_polygons.append((id, voorkomen, multipolygon))

            new_df = spark.createDataFrame(new_multi_polygons, ['id', 'voorkomen', 'geo_multi_polygon'])

            cond = [
                df['Objecten:identificatie']['_VALUE'] == new_df.id,
                df['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie'] == new_df.voorkomen
            ]

            new_geo_cols.append('geo_multi_polygon')
            df = df.join(new_df, cond, 'full').drop('id').drop('voorkomen')


        if 'Objecten:vlak' in geo_df.columns:
            polygons = []
            for row in df.collect():
                id = row['Objecten:identificatie']['_VALUE']
                voorkomen = row['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie']

                if row['Objecten:geometrie']['Objecten:vlak'] is not None:
                    # exterior
                    positions = row['Objecten:geometrie']['Objecten:vlak']['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList']['_VALUE']
                    exterior_lr = create_linear_ring(positions)

                    # interiors
                    interior = row['Objecten:geometrie']['Objecten:vlak']['gml:Polygon']['gml:interior']
                    interior_lrs = []
                    if interior is not None:
                        positions = interior['gml:LinearRing']['gml:posList']['_VALUE']
                        interior_lr = create_linear_ring(positions)
                        interior_lrs.append(interior_lr)

                    polygon = geometry.Polygon(exterior_lr, interior_lrs)
                    polygons.append((id, voorkomen, wkt.dumps(polygon)))

            new_df = spark.createDataFrame(polygons, ['id', 'voorkomen', 'geo_polygon'])

            cond = [
                df['Objecten:identificatie']['_VALUE'] == new_df.id,
                df['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie'] == new_df.voorkomen
            ]

            new_geo_cols.append('geo_polygon')
            df = df.join(new_df, cond, 'full').drop('id').drop('voorkomen')

        if 'Objecten:punt' in geo_df.columns:
            points = []
            for row in df.collect():
                id = row['Objecten:identificatie']['_VALUE']
                voorkomen = row['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie']
                point = row['Objecten:geometrie']['Objecten:punt']['gml:Point']['gml:pos']
                points.append((id, voorkomen, convert_punt(point)))

            points_df = spark.createDataFrame(points, ['id', 'voorkomen', 'geo_point'])

            cond = [
                df['Objecten:identificatie']['_VALUE'] == points_df.id,
                df['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie'] == points_df.voorkomen
            ]
            df = df.join(points_df, cond, 'full')
            df = df.drop('id').drop('voorkomen')
            new_geo_cols.append('geo_point')

        if 'gml:Polygon' in geo_df.columns:
            polygons = []
            for row in df.collect():
                id = row['Objecten:identificatie']['_VALUE']
                voorkomen = row['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie']

                if row['Objecten:geometrie']['gml:Polygon'] is not None:
                    dimension = row['Objecten:geometrie']['gml:Polygon']['_srsDimension']
                    # exterior
                    positions = row['Objecten:geometrie']['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList']['_VALUE']
                    exterior_lr = create_linear_ring(positions, dimension)

                    # interiors
                    interiors = row['Objecten:geometrie']['gml:Polygon']['gml:interior']
                    interior_lrs = []
                    if interiors is not None:
                        for interior in interiors:
                            positions = interior['gml:LinearRing']['gml:posList']['_VALUE']
                            interior_lr = create_linear_ring(positions, dimension)
                            interior_lrs.append(interior_lr)

                    polygon = wkt.dumps(geometry.Polygon(exterior_lr, interior_lrs))
                    polygons.append((id, voorkomen, polygon))

            new_df = spark.createDataFrame(polygons, ['id', 'voorkomen', 'geo_polygon'])

            cond = [
                df['Objecten:identificatie']['_VALUE'] == new_df.id,
                df['Objecten:voorkomen']['Historie:Voorkomen']['Historie:voorkomenidentificatie'] == new_df.voorkomen
            ]

            df = df.join(new_df, cond, 'full').drop('id').drop('voorkomen')
            new_geo_cols.append('geo_polygon')

        if new_geo_cols:
            df = df.withColumn('geometry', struct(*new_geo_cols))
            for col in new_geo_cols:
                df = df.drop(col)

    df.write \
        .format('bigquery') \
        .option('table', f'{DATASET}.{object_type}') \
        .mode('overwrite') \
        .save()

if __name__ == '__main__':
    if DOWNLOAD_BAG:
        download_bag_zip()

    transformer = Transformer.from_crs('epsg:28992', 'epsg:4326')
    spark = init_spark_session()
    file_dict = unzip_bag_data()

    # read xml files
    for key, val in file_dict.items():

        key_no_digit = ''.join([c for c in key if not c.isdigit()])
        if key_no_digit in object_map.keys():
            files = [f'file://{x}' for x in val]
            joined_files = ','.join(files)

            object_type = object_map[key_no_digit]
            process_table(object_type, joined_files,
                          'sl-bag-extract:bagStand',
                          'sl-bag-extract:bagObject',
                          'Objecten')
