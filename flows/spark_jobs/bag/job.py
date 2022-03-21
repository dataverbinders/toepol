from os import listdir
from os.path import abspath
from typing import List
from zipfile import ZipFile

from google.cloud import storage
from pyproj import Transformer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import struct
from shapely import geometry, wkt

# Object map
object_map = {
    "WPL": "Woonplaats",
    "OPR": "OpenbareRuimte",
    "NUM": "Nummeraanduiding",
    "LIG": "Ligplaats",
    "STA": "Standplaats",
    "PND": "Pand",
    "VBO": "Verblijfsobject",
}


def init_spark_session():
    """Initializes and returns a PySpark session."""
    spark = (
        SparkSession.builder.appName("bag2gcs").master("local").getOrCreate()
    )
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set(
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED"
    )
    spark.sql("set spark.sql.parquet.compression.codec=uncompressed")
    return spark


def init_transformer():
    """Initializes and returns a pyproj transformer."""
    transformer = Transformer.from_crs("epsg:28992", "epsg:4326")
    return transformer


def unzip_subzip(file: str) -> str:
    """Unzips a bag 'sub zipfile' and returns a concatenation of the locations
    of the extracted files..

    :param file: the file to be extracted
    :type file: str
    :rtype: str
    """
    with ZipFile(file) as zip:
        target_folder = file.split(".")[0]
        zip.extractall(f"data/{target_folder}")
        files = [
            abspath(f"data/{target_folder}/{f}")
            for f in zip.namelist()
            if f.endswith(".xml")
        ]
    files = [f"file://{abspath(f)}" for f in files]
    concat_files = ",".join(files)
    return concat_files


def create_spark_df(
    spark: SparkSession, object_type: str, files: str
) -> DataFrame:
    """Given a string containing xml files, returns a Spark dataframe..

    :param spark: a spark session
    :type spark: SparkSession
    :param object_type: the type of object in the given files. eg: 'Woonplaats'
    :type object_type: str
    :param files: a string containing a concatenation of the XML files
    :type files: str
    :rtype: DataFrame
    """
    df = (
        spark.read.format("xml")
        .option("rootTag", "sl-bag-extract:bagStand")
        .option("rowTag", "sl-bag-extract:bagObject")
        .option("path", files)
        .load()
    )
    df = df.select(f"Objecten:{object_type}.*")
    return df


def transform_coords(x: int, y: int, transformer: Transformer) -> List[int]:
    """Transforms x and y coördinates from one projectino to the other based
    on the given transformer

    :param x: x-coördinate
    :type x: int
    :param y: y-coördinate
    :type y: int
    :param transformer: a pyproj transformer
    :type transformer: Transformer
    :rtype: List[int]
    """
    x, y = transformer.transform(x, y)
    return [x, y]


def convert_point(point: str, transformer: Transformer) -> geometry.Point:
    """Converts a point in three dimensional space to a two dimensional point.
    Also converts it to a different projection based on the provided
    transformer..

    :param point: a string containing three coördinates split by spaces
    :type point: str
    :param transformer: a pyproj transformer
    :type transformer: Transformer
    :rtype: geometry.Point
    """
    x, y, _ = point.split()
    x, y = transform_coords(x, y, transformer)
    p = wkt.dumps(geometry.Point(x, y))
    return p


def create_linear_ring(
    positions: str, transformer: Transformer, dimension: int = 2
) -> geometry.LinearRing:
    """Creates a linear ring given a string of coördinates.

    :param positions: a string of coördinates
    :type positions: str
    :param transformer: a pyproj transformer
    :type transformer: Transformer
    :param dimension: the dimension of the provided points
    :type dimension: int
    :rtype: geometry.LinearRing
    """
    points_list = []
    s = positions.split()
    for i in range(0, len(s), dimension):
        x, y = transform_coords(s[i], s[i + 1], transformer)
        points_list.append([x, y])
    lr = geometry.LinearRing(points_list)
    return lr


def convert_points(
    df: DataFrame, spark: SparkSession, transformer: Transformer
) -> DataFrame:
    """Given a Spark DataFrame with a column of containing points, it adds a
    new column containing the points in WKT format.

    :param df: a Spark DataFrame
    :type df: DataFrame
    :param spark: a SparkSession
    :type spark: SparkSession
    :param transformer: a pyproj transformer
    :type transformer: Transformer
    :rtype: DataFrame
    """
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


def convert_multivlak(
    df: DataFrame, spark: SparkSession, transformer: Transformer
) -> DataFrame:
    """Given a Spark DataFrame with a column containing 'multivlak', add a
    new column containing a multisurface in WKT format.

    :param df: a Spark DataFrame
    :type df: DataFrame
    :param spark: a SparkSession
    :type spark: SparkSession
    :param transformer: a pyproj transformer
    :type transformer: Transformer
    :rtype: DataFrame
    """
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


def convert_vlak(
    df: DataFrame, spark: SparkSession, transformer: Transformer
) -> DataFrame:
    """Given a Spark DataFrame with a column containing 'vlak', add a
    new column containing a surface in WKT format.

    :param df: a Spark DataFrame
    :type df: DataFrame
    :param spark: a SparkSession
    :type spark: SparkSession
    :param transformer: a pyproj transformer
    :type transformer: Transformer
    :rtype: DataFrame
    """
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


def convert_polygon(
    df: DataFrame, spark: SparkSession, transformer: Transformer
) -> DataFrame:
    """Given a Spark DataFrame with a column containing 'polygon', add a new
    column containing a polygon in WKT format.

    :param df: a Spark DataFrame
    :type df: DataFrame
    :param spark: a SparkSession
    :type spark: SparkSession
    :param transformer: a pyproj transformer
    :type transformer: Transformer
    :rtype: DataFrame
    """
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


def convert_geometry(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Checks if the provided DataFrame contains a geometry column, and if so
    transforms it to the appropriate WKT format.

    :param df: a Spark DataFrame
    :type df: DataFrame
    :param spark: a SparkSession
    :type spark: SparkSession
    :rtype: DataFrame
    """
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


def store_df_as_parquet(df: DataFrame, object_type: str, data_dir: str) -> str:
    """Stores a Spark DataFrame as a .parquet file.
    Returns the folder the .parquet file is located in..

    :param df: a Spark DataFrame
    :type df: DataFrame
    :param object_type: the object type corresponding to the DataFrame. Used as the directory name of the .parquet file
    :type object_type: str
    :param data_dir: directory to store the file in
    :type data_dir: str
    :rtype: str
    """
    target_folder = f"{data_dir}/{object_type}"
    df.repartition(1).write.format("parquet").mode("overwrite").save(
        target_folder
    )
    return target_folder


def write_file_to_gcs(file: str, bucket_name: str, blob_name: str):
    """Stores the file <file> to the bucket <bucket_name> as <blob_name>.

    :param file:
    :type file: str
    :param bucket_name:
    :type bucket_name: str
    :param blob_name:
    :type blob_name: str
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = storage.Blob(blob_name, bucket)
    with open(file, "rb") as f:
        blob.upload_from_file(f)


if __name__ == "__main__":
    zipfiles = [f for f in listdir(".") if f.endswith(".zip")]
    spark = init_spark_session()

    for zf in zipfiles:
        key = "".join([c for c in zf.split(".")[0] if not c.isdigit()])
        if key not in object_map.keys():
            continue
        obj_type = object_map[key]

        print(f"PROCESSING {obj_type}")

        files = unzip_subzip(zf)

        # extract data dir from files
        data_dir = "/".join(files.split(",")[0].split("/")[:-2])

        df = create_spark_df(spark, object_map[key], files)
        df = convert_geometry(df, spark)
        parquet_folder = store_df_as_parquet(df, obj_type, data_dir)
        parquet_folder = f"data/{obj_type}"
        parquet_file = [
            f for f in listdir(parquet_folder) if f.endswith(".parquet")
        ][0]

        write_file_to_gcs(
            f"{parquet_folder}/{parquet_file}",
            "dataverbinders-dev",
            f"kadaster/bag/{obj_type}.parquet",
        )
