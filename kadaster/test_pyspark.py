from pyspark.sql import SparkSession
from pathlib import Path
import zipfile
import prefect
from prefect import task, unmapped, Parameter, Flow
from prefect.engine.results import PrefectResult
from prefect.executors import LocalDaskExecutor
from google.cloud import bigquery
import pandas as pd
from tabulate import tabulate

# GCP configurations
GCP_PROJECT = "testenvironment-338811"
DATASET = "kadaster"

# BAG Configurations
BAG_VERSION = "08012022"

# Relative paths for BAG files.
WORK_DIR = Path.cwd() / 'kadaster' / 'bag'
BAG = WORK_DIR / 'lvbag-extract-nl'
TESTING = WORK_DIR / 'testing'
OUTPUT_DIR = WORK_DIR / 'output'

WPL_FILE = BAG / (f"9999WPL{BAG_VERSION}" + ".zip")
OPR_FILE = BAG / (f"9999OPR{BAG_VERSION}" + ".zip")
NUM_FILE = BAG / (f"9999NUM{BAG_VERSION}" + ".zip")
PND_FILE = BAG / (f"9999PND{BAG_VERSION}" + ".zip")
VBO_FILE = BAG / (f"9999VBO{BAG_VERSION}" + ".zip")
LIG_FILE = BAG / (f"9999LIG{BAG_VERSION}" + ".zip")
STA_FILE = BAG / (f"9999STA{BAG_VERSION}" + ".zip")

# Root tags.
WPL_ROOT = "Woonplaats"
OPR_ROOT = "OpenbareRuimte"
NUM_ROOT = "Nummeraanduiding"
PND_ROOT = "Pand"
VBO_ROOT = "Verblijfsobject"
LIG_ROOT = "Ligplaats"
STA_ROOT = "Standplaats"

XPATH = ".//"

def create_xml_list(zip_file):
    """
    Creates new directory and list of xml files from nested_zipfile which is in main BAG zipfile.
    """

    # new_dir = OUTPUT_DIR / zip_file.split(".")[0].split("/")[-1]
    new_dir = OUTPUT_DIR / zip_file.name.split(".")[0]
    new_dir.mkdir(exist_ok=True)

    # print(new_dir)

    with zipfile.ZipFile(zip_file) as z:
        return [f for f in z.namelist() if f.endswith(".xml")], new_dir

def reading_xml(bag_file, xml_file, output_dir):
    spark = SparkSession.builder.config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2,com.databricks:spark-xml_2.12:0.14.0").getOrCreate()

    # Read and extract zipfile.
    zipfile.ZipFile(bag_file).extractall(output_dir)

    paths = [(output_dir / i).as_posix() for i in xml_file]

    path_changed = ",".join(paths)
    # print(path_changed)
    
    # for i in paths:
    #     df = spark.read.format("xml").option("rootTag", "sl-bag-extract:bagStand").option("rowTag", "sl-bag-extract:bagObject").load(i)

    df = spark.read.format("xml").option("rootTag", "sl-bag-extract:bagStand").option("rowTag", "sl-bag-extract:bagObject").load(path_changed)
    
    
    # print(df.count())

    # df.write.format("bigquery").option("writeMethod", "direct").save(f"{GCP_PROJECT}.{DATASET}.{bag_file.name.split('.')[0]}")
    # df.write.format("bigquery").option("writeMethod", "direct").option("temporaryGcsBucket","gcp_bucket").save(f"{GCP_PROJECT}.{DATASET}.{bag_file.name.split('.')[0]}")
    # df.write.format("bigquery").option("table",f"{GCP_PROJECT}.{DATASET}.{bag_file.name.split('.')[0]}").option("temporaryGcsBucket", "gcp_bucket").mode('overwrite').save()


if __name__ == "__main__":
    wpl_xml, wpl_dir = create_xml_list(WPL_FILE)
    reading_xml(WPL_FILE, wpl_xml, wpl_dir)

    # lig_xml, lig_dir = create_xml_list(PND_FILE)
    # reading_xml(PND_FILE, lig_xml, lig_dir)