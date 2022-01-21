from pathlib import Path
import json
import zipfile
from nl_open_data.config import get_config
from lxml import etree, objectify
import xmltodict
# from tasks import curl_cmd, download, unzip, create_dir
import prefect
from prefect import task, unmapped, Parameter, Flow
from prefect.engine.results import PrefectResult
from google.cloud import bigquery
from bag_schemas import schema

# Loading 'txe' configuration.
CONFIG = get_config("txe")

# GCP configurations
GCP = CONFIG.gcp
DATASET = "kadaster"

# BAG Configurations
BAG_VERSION = "08012022"

# Relative paths for BAG files.
BAG = CONFIG.path.root / CONFIG.path.bag
TESTING = CONFIG.path.root / CONFIG.path.cbs
OUTPUT_DIR = CONFIG.path.root / CONFIG.path.tmp

# TEST constants of BAG files.
TEST_WPL_FILE = TESTING / (f"9999WPL{BAG_VERSION}" + ".zip")
TEST_NUM_FILE = TESTING / (f"9999NUM{BAG_VERSION}" + ".zip")
TEST_PND_FILE = TESTING / (f"9999PND{BAG_VERSION}" + ".zip")
TEST_VBO_FILE = TESTING / (f"9999VBO{BAG_VERSION}" + ".zip")

# BAG files as constants.
WPL_FILE = BAG / (f"9999WPL{BAG_VERSION}" + ".zip")
OPR_FILE = BAG / (f"9999OPR{BAG_VERSION}" + ".zip")
NUM_FILE = BAG / (f"9999NUM{BAG_VERSION}" + ".zip")
LIG_FILE = BAG / (f"9999LIG{BAG_VERSION}" + ".zip")
STA_FILE = BAG / (f"9999STA{BAG_VERSION}" + ".zip")
PND_FILE = BAG / (f"9999PND{BAG_VERSION}" + ".zip")
VBO_FILE = BAG / (f"9999VBO{BAG_VERSION}" + ".zip")

# Root tags.
WPL_ROOT = "Woonplaats"
OPR_ROOT = "OpenbareRuimte"
NUM_ROOT = "Nummeraanduiding"
LIG_ROOT = "Ligplaats"
STA_ROOT = "Standplaats"
PND_ROOT = "Pand"
VBO_ROOT = "Verblijfsobject"

XPATH = ".//"

@task(result=PrefectResult())
def create_xml_list(zip_file):
    """
    Creates new directory and list of xml files from nested_zipfile which is in main BAG zipfile.
    """

    new_dir = OUTPUT_DIR / zip_file.name.split(".")[0]
    new_dir.mkdir(exist_ok=True)
    # print(new_dir)

    with zipfile.ZipFile(zip_file) as z:
        return [f for f in z.namelist() if f.endswith(".xml")], new_dir

@task
def create_ndjson(bag_file, xml_file, ndjson_dir, root_tag):
    # Description, see Danny code

    def remove_ns_keys(dict_, root):
        # Description, see Danny code
        keys = list(dict_[root].keys())
        for key in keys:
            if key.startswith("@xmlns"):
                del dict_[root][key] # same as dict_['Woonplaats']['@xmlns']
        return dict_


    # Read zipfile.
    zip = zipfile.ZipFile(bag_file)

    
    # with zip.open(xml_file[0]) as file_:
    with zip.open(xml_file) as file_:
        xml = etree.parse(file_).getroot()

    # Removing namespace and prefix lxml:
    # https://stackoverflow.com/questions/18159221/remove-namespace-and-prefix-from-xml-in-python-using-lxml
    for elem in xml.getiterator():
        elem.tag = etree.QName(elem).localname
    
    etree.cleanup_namespaces(xml)

    # Get normal xml structure and delete (remove_ns_keys) unnecessary xml-tags.
    elements = [
        remove_ns_keys(xmltodict.parse(etree.tostring(element), xml_attribs=False), root_tag)
        for element in xml.findall(XPATH + root_tag)
    ]

    # Transform OrderedDict to json format.
    json_records = [json.dumps(element[root_tag]) for element in elements]
    # print(json_records)

    # ndjson = ndjson_dir / (xml_file[0].split(".")[0] + ".ndjson")
    ndjson = ndjson_dir / (xml_file.split(".")[0] + ".ndjson")
    # print(ndjson)
    
    with open(ndjson, "w") as file_:
        file_.write("\n".join(json_records))
    
    return ndjson


@task
def load_bq(name_bag, path_bag, schema_bag):
    table_ref = dataset_ref.table(name_bag.name)
    job_config.schema = schema_bag
    # job_config.autodetect = True

    with open(path_bag, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print(f"Loaded {job.output_rows} rows into {DATASET}: {name_bag.name}.")


with Flow("BAG-Extract") as flow:
    client = bigquery.Client(project=GCP.project)

    dataset_ref = client.dataset(DATASET)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    # job_config.autodetect = True
    # job_config.schema = schema
    
    wpl_xml, wpl_dir = create_xml_list.run(WPL_FILE)
    wpl_mapped = create_ndjson.map(bag_file=unmapped(WPL_FILE), xml_file=wpl_xml, ndjson_dir=unmapped(wpl_dir), root_tag=unmapped(WPL_ROOT))
    load_bq.map(name_bag=unmapped(wpl_dir), path_bag=wpl_mapped, schema_bag=unmapped(schema["wpl"]))

    num_xml, num_dir = create_xml_list.run(NUM_FILE)
    num_mapped = create_ndjson.map(bag_file=unmapped(NUM_FILE), xml_file=num_xml, ndjson_dir=unmapped(num_dir), root_tag=unmapped(NUM_ROOT))
    load_bq.map(name_bag=unmapped(num_dir), path_bag=num_mapped, schema_bag=unmapped(schema["num"]))

    # opr_xml, opr_dir = create_xml_list.run(OPR_FILE)
    # opr_mapped = create_ndjson.map(bag_file=unmapped(OPR_FILE), xml_file=opr_xml, ndjson_dir=unmapped(opr_dir), root_tag=unmapped(OPR_ROOT))
    # load_bq.map(name_bag=unmapped(opr_dir), path_bag=opr_mapped, schema_bag=unmapped(schema["opr"]))

    # lig_xml, lig_dir = create_xml_list.run(LIG_FILE)
    # lig_mapped = create_ndjson.map(bag_file=unmapped(LIG_FILE), xml_file=lig_xml, ndjson_dir=unmapped(lig_dir), root_tag=unmapped(LIG_ROOT))
    # load_bq.map(name_bag=unmapped(lig_dir), path_bag=lig_mapped, schema_bag=unmapped(schema["lig"]))

    # sta_xml, sta_dir = create_xml_list.run(STA_FILE)
    # sta_mapped = create_ndjson.map(bag_file=unmapped(STA_FILE), xml_file=sta_xml, ndjson_dir=unmapped(sta_dir), root_tag=unmapped(STA_ROOT))
    # load_bq.map(name_bag=unmapped(sta_dir), path_bag=sta_mapped, schema_bag=unmapped(schema["sta"]))

    # pnd_xml, pnd_dir = create_xml_list.run(TEST_PND_FILE)
    # pnd_mapped = create_ndjson.map(bag_file=unmapped(TEST_PND_FILE), xml_file=pnd_xml, ndjson_dir=unmapped(pnd_dir), root_tag=unmapped(PND_ROOT))
    # load_bq.map(name_bag=unmapped(pnd_dir), path_bag=pnd_mapped, schema_bag=unmapped(schema["pnd"]))

    # vbo_xml, vbo_dir = create_xml_list.run(TEST_VBO_FILE)
    # vbo_mapped = create_ndjson.map(bag_file=unmapped(TEST_VBO_FILE), xml_file=vbo_xml, ndjson_dir=unmapped(vbo_dir), root_tag=unmapped(VBO_ROOT))
    # load_bq.map(name_bag=unmapped(vbo_dir), path_bag=vbo_mapped, schema_bag=unmapped(schema["vbo"]))


def prefect_main():
    flow.run()


if __name__ == "__main__":
    prefect_main()