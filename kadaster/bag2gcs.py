from os import getcwd, remove, listdir
from zipfile import ZipFile

import prefect
import requests
from prefect import Flow, Parameter, mapped, resource_manager, task, unmapped
from prefect.executors import DaskExecutor
from prefect.tasks.gcp.storage import GCSUpload


from pyspark.sql import SparkSession
from pyspark.sql.functions import struct
from shapely import geometry, wkt

from pyproj import Transformer

# BAG Download
#  DOWNLOAD_BAG = False
#  BAG_URL = 'https://service.pdok/nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip'


# Local Files
CWD = getcwd()
BAG_FILE = f"{CWD}/lvbag-extract-nl.zip"
DATA_DIR = f"{CWD}/data"


# GCP


# Object maps
object_map = {
    "WPL": "Woonplaats",
    "OPR": "OpenbareRuimte",
    "NUM": "Nummeraanduiding",
    "LIG": "Ligplaats",
    "STA": "Standplaats",
    "PND": "Pand",
    "VBO": "Verblijfsobject",
}


@resource_manager()
class SparkCluster:
    def setup(self) -> SparkSession:
        spark = (
            SparkSession.builder.appName("bag2gcs")
            .config(
                "spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0"
            )
            .getOrCreate()
        )

        spark.conf.set(
            "spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED"
        )
        spark.sql("set spark.sql.parquet.compression.codec=uncompressed")
        return spark

    def cleanup(self, spark: SparkSession):
        spark.stop()


@task(checkpoint=False)
def init_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("bag2gcs")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    return spark


@task(checkpoint=False)
def download_bag_zip(download_bag, bag_url, bag_file=BAG_FILE):
    if download_bag:
        r = requests.get(bag_url)
        with open(bag_file, "wb") as f:
            f.write(r.content)
    return bag_file


def init_transformer():
    transformer = Transformer.from_crs("epsg:28992", "epsg:4326")
    return transformer


@task(nout=2)
def unzip_bag_data(bag_file=BAG_FILE, data_dir=DATA_DIR):
    with ZipFile(bag_file) as zip:
        zipnames = [f for f in zip.namelist() if f.endswith(".zip")]
        zip.extractall(data_dir)

        lev_doc = [f for f in zip.namelist() if f.endswith(".xml")][0]

    locations = {}
    for name in zipnames:
        key = "".join([c for c in name if not c.isdigit()]).split(".")[0]
        locations[key] = f"{data_dir}/{name}"
    #  return locations, lev_doc

    file_dict = {}
    for name in zipnames:
        with ZipFile(f"{data_dir}/{name}") as zip:
            target_folder = name.split(".")[0]
            zip.extractall(f"{data_dir}/{target_folder}")
            files = [
                f"{DATA_DIR}/{target_folder}/{f}"
                for f in zip.namelist()
                if f.endswith(".xml")
            ]

        key = "".join([c for c in target_folder if not c.isdigit()])
        concat_files = ",".join(files)
        file_dict[key] = concat_files
        remove(f"{data_dir}/{name}")
    return file_dict, lev_doc


@task
def unzip_sub_file(file, target_folder, data_dir=DATA_DIR):
    with ZipFile(file) as zip:
        zip.extractall(target_folder)
        files = [
            f"{target_folder}/{f}"
            for f in zip.namelist()
            if f.endswith(".xml")
        ]
    remove(file)
    concat_files = ",".join(files)
    return concat_files


#  @task(checkpoint=False)
def create_spark_df(spark, object_type, files):
    df = (
        spark.read.format("xml")
        .option("rootTag", "sl-bag-extract:bagStand")
        .option("rowTag", "sl-bag-extract:bagObject")
        .option("path", files)
        .load()
    )
    df = df.select(f"Objecten:{object_type}.*")
    return df


def transform_coords(x, y, transformer):
    x, y = transformer.transform(x, y)
    return [x, y]


def convert_point(s: str, transformer):
    x, y, _ = s.split()
    x, y = transform_coords(x, y, transformer)
    p = wkt.dumps(geometry.Point(x, y))
    return p


def create_linear_ring(positions: str, transformer, dimension: int = 2):
    points_list = []
    s = positions.split()
    for i in range(0, len(s), dimension):
        x, y = transform_coords(s[i], s[i + 1], transformer)
        points_list.append([x, y])
    lr = geometry.LinearRing(points_list)
    return lr


def convert_points(df, spark, transformer):
    points = []
    for row in df.collect():
        id = row["Objecten:identificatie"]["_VALUE"]
        voorkomen = row["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]
        point = row["Objecten:geometrie"]["Objecten:punt"]["gml:Point"][
            "gml:pos"
        ]
        points.append((id, voorkomen, convert_point(point, transformer)))

    points_df = spark.createDataFrame(points, ["id", "voorkomen", "geo_point"])

    cond = [
        df["Objecten:identificatie"]["_VALUE"] == points_df.id,
        df["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]
        == points_df.voorkomen,
    ]

    df = df.join(points_df, cond, "full").drop("id").drop("voorkomen")

    return df


def convert_multivlak(df, spark, transformer):
    new_multi_polygons = []
    for row in df.collect():
        id = row["Objecten:identificatie"]["_VALUE"]
        voorkomen = row["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]

        if row["Objecten:geometrie"]["Objecten:multivlak"] is not None:
            multi_surface = row["Objecten:geometrie"]["Objecten:multivlak"][
                "gml:MultiSurface"
            ]["gml:surfaceMember"]
            polygons = []
            for surface in multi_surface:
                # exterior
                positions = surface["gml:Polygon"]["gml:exterior"][
                    "gml:LinearRing"
                ]["gml:posList"]["_VALUE"]
                exterior_lr = create_linear_ring(positions, transformer)

                # interior
                interiors = surface["gml:Polygon"]["gml:interior"]
                interior_lrs = []
                if interiors is not None:
                    for interior in interiors:
                        positions = interior["gml:LinearRing"]["gml:posList"][
                            "_VALUE"
                        ]
                        interior_lr = create_linear_ring(
                            positions, transformer
                        )
                        interior_lrs.append(interior_lr)
                polygon = geometry.Polygon(exterior_lr, interior_lrs)
                polygons.append(polygon)
            multipolygon = wkt.dumps(geometry.MultiPolygon(polygons))
            new_multi_polygons.append((id, voorkomen, multipolygon))

    new_df = spark.createDataFrame(
        new_multi_polygons, ["id", "voorkomen", "geo_multi_polygon"]
    )

    cond = [
        df["Objecten:identificatie"]["_VALUE"] == new_df.id,
        df["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]
        == new_df.voorkomen,
    ]

    df = df.join(new_df, cond, "full").drop("id").drop("voorkomen")

    return df


def convert_vlak(df, spark, transformer):
    new_polygons = []
    for row in df.collect():
        id = row["Objecten:identificatie"]["_VALUE"]
        voorkomen = row["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]

        if row["Objecten:geometrie"]["Objecten:vlak"] is not None:
            # exterior
            positions = row["Objecten:geometrie"]["Objecten:vlak"][
                "gml:Polygon"
            ]["gml:exterior"]["gml:LinearRing"]["gml:posList"]["_VALUE"]
            exterior_lr = create_linear_ring(positions, transformer)

            # interiors
            interior = row["Objecten:geometrie"]["Objecten:vlak"][
                "gml:Polygon"
            ]["gml:interior"]
            interior_lrs = []
            if interior is not None:
                positions = interior["gml:LinearRing"]["gml:posList"]["_VALUE"]
                interior_lr = create_linear_ring(positions, transformer)
                interior_lrs.append(interior_lr)

            polygon = geometry.Polygon(exterior_lr, interior_lrs)
            new_polygons.append((id, voorkomen, wkt.dumps(polygon)))

    new_df = spark.createDataFrame(
        new_polygons, ["id", "voorkomen", "geo_polygon"]
    )
    cond = [
        df["Objecten:identificatie"]["_VALUE"] == new_df.id,
        df["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]
        == new_df.voorkomen,
    ]
    df = df.join(new_df, cond, "full").drop("id").drop("voorkomen")
    return df


def convert_polygon(df, spark, transformer):
    new_polygons = []
    for row in df.collect():
        id = row["Objecten:identificatie"]["_VALUE"]
        voorkomen = row["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]
        dimension = row["Objecten:geometrie"]["gml:Polygon"]["_srsDimension"]
        if row["Objecten:geometrie"]["gml:Polygon"] is not None:
            # exterior
            positions = row["Objecten:geometrie"]["gml:Polygon"][
                "gml:exterior"
            ]["gml:LinearRing"]["gml:posList"]["_VALUE"]
            exterior_lr = create_linear_ring(positions, transformer, dimension)

            # interiors
            interiors = row["Objecten:geometrie"]["gml:Polygon"][
                "gml:interior"
            ]
            interior_lrs = []
            if interiors is not None:
                for interior in interiors:
                    positions = interior["gml:LinearRing"]["gml:posList"][
                        "_VALUE"
                    ]
                    interior_lr = create_linear_ring(
                        positions, transformer, dimension
                    )
                    interior_lrs.append(interior_lr)

            polygon = geometry.Polygon(exterior_lr, interior_lrs)
            new_polygons.append((id, voorkomen, wkt.dumps(polygon)))

    new_df = spark.createDataFrame(
        new_polygons, ["id", "voorkomen", "geo_polygon"]
    )
    cond = [
        df["Objecten:identificatie"]["_VALUE"] == new_df.id,
        df["Objecten:voorkomen"]["Historie:Voorkomen"][
            "Historie:voorkomenidentificatie"
        ]
        == new_df.voorkomen,
    ]
    df = df.join(new_df, cond, "full").drop("id").drop("voorkomen")
    return df


#  @task(checkpoint=False)
def convert_geometry(df, spark):
    if "Objecten:geometrie" not in df.columns:
        return df

    transformer = Transformer.from_crs("epsg:28992", "epsg:4326")

    new_geo_cols = []
    geo_df_columns = df.select("Objecten:geometrie.*").columns

    if "Objecten:multivlak" in geo_df_columns:
        df = convert_multivlak(df, spark, transformer)
        new_geo_cols.append("geo_multi_polygon")

    if "Objecten:vlak" in geo_df_columns:
        df = convert_vlak(df, spark, transformer)
        new_geo_cols.append("geo_polygon")

    if "Objecten:punt" in geo_df_columns:
        df = convert_points(df, spark, transformer)
        new_geo_cols.append("geo_point")

    if "gml:Polygon" in geo_df_columns:
        df = convert_polygon(df, spark, transformer)
        new_geo_cols.append("geo_polygon")

    if new_geo_cols:
        df = df.withColumn("geometry", struct(*new_geo_cols))
        for col in new_geo_cols:
            df = df.drop(col)

    return df


#  @task
def store_df_as_parquet(df, object_type):
    target_folder = f"{DATA_DIR}/{object_type}"
    df.repartition(1).write.format("parquet").mode("overwrite").save(
        target_folder
    )
    return target_folder


@task
def write_parquet_to_gcs(source_dir, bucket, folder):
    print(source_dir)
    files = [f for f in listdir(source_dir) if f.endswith(".parquet")]
    file = files[0]
    with open(f"{source_dir}/{file}", "r") as f:
        print(f)


@task(checkpoint=False)
def process_data(spark, object_map, file_dict):
    parquet_files = []
    for key, val in object_map.items():
        logger.info(f"Creating {key} dataframe")
        df = create_spark_df(spark, val, file_dict[key])

        logger.info(f"Converting geodata: {key}")
        df = convert_geometry(df, spark)

        logger.info(f"Writing {key} dataframe to disk")
        parquet_file = store_df_as_parquet(df, val)
        parquet_files.append(parquet_file)
    return parquet_files


with Flow("bag2gcs") as flow:
    # Download Paramters
    BAG_URL = Parameter(
        "bag_url",
        default="https://service.pdok/nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip",
    )
    DOWNLOAD_BAG = Parameter("download_bag", default=False)

    # GCS Parameters
    GCP_PROJECT = Parameter("gcp_project", default="dataverbinders-dev")
    GCS_BUCKET = Parameter("gcs_bucket", default="dataverbinders-dev")
    GCS_FOLDER = Parameter("gcs_folder", default="kadaster/bag")

    logger = prefect.context.get("logger")

    bag_file = download_bag_zip(DOWNLOAD_BAG, BAG_URL)

    file_dict, lev_doc = unzip_bag_data(bag_file)

    with SparkCluster() as spark:
        parquet_dirs = process_data(spark, object_map, file_dict)

    write_parquet_to_gcs(mapped(parquet_dirs), GCS_BUCKET, GCS_FOLDER)


if __name__ == "__main__":
    #  gcs_uploader = GCSUpload('')
#
    #  flow.executor = DaskExecutor()
    #  flow.register(
        #  project_name='toepol',
    #  )

    #################
    ### RUN LOCAL ###
    #################

    params = {
        "download_bag": 0,
    }
    flow.run(parameters=params)
