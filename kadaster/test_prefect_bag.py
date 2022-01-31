import json
import zipfile
from lxml import etree
import xmltodict
# from tasks import curl_cmd, download, unzip, create_dir
from prefect import task, unmapped, Flow
from prefect.engine.results import PrefectResult
from prefect.executors import LocalDaskExecutor
from google.cloud import bigquery
from bag_schemas import schema
from datetime import timedelta
import os

import ndjson

# GCP configurations
GCP_TOKEN = 'gcp_token.json'
DATASET = "kadaster"

# BAG Configurations
BAG_VERSION = "08012022"

# Relative paths for BAG files.
BAG = 'data'
TESTING = 'testing'
OUTPUT_DIR = 'output'

# BAG files as constants.
WPL_FILE = f'{BAG}/9999WPL{BAG_VERSION}.zip'
OPR_FILE = f'{BAG}/9999OPR{BAG_VERSION}.zip'
NUM_FILE = f'{BAG}/9999NUM{BAG_VERSION}.zip'
LIG_FILE = f'{BAG}/9999LIG{BAG_VERSION}.zip'
STA_FILE = f'{BAG}/9999STA{BAG_VERSION}.zip'
PND_FILE = f'{BAG}/9999PND{BAG_VERSION}.zip'
VBO_FILE = f'{BAG}/9999VBO{BAG_VERSION}.zip'

# Root tags.
WPL_ROOT = "Woonplaats"
OPR_ROOT = "OpenbareRuimte"
NUM_ROOT = "Nummeraanduiding"
LIG_ROOT = "Ligplaats"
STA_ROOT = "Standplaats"
PND_ROOT = "Pand"
VBO_ROOT = "Verblijfsobject"

XPATH = ".//"


#with zipfile.ZipFile('lvbag-extract-nl.zip') as z:
#    z.extractall(path=BAG)


@task(result=PrefectResult())
def create_xml_list(zip_file):
    """
    Creates new directory and list of xml files from nested_zipfile
    which is in main BAG zipfile.
    """

    new_dir = f'{OUTPUT_DIR}/{zip_file.split(".")[0]}'
    os.makedirs(new_dir, exist_ok=True)
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
    ndjson = f'{ndjson_dir}/{xml_file.split(".")[0]}.ndjson'
    # print(ndjson)

    with open(ndjson, "w") as file_:
        file_.write("\n".join(json_records))
    return ndjson


@task
def fix_heeftAlsNevenadres(file):
    with open(file, 'r') as f:
        json_file = ndjson.load(f)
    for entry in json_file:
        if 'heeftAlsNevenadres' not in entry:
            entry['heeftAlsNevenadres'] = {'NummeraanduidingRef': []}
        else:
            if type(entry['heeftAlsNevenadres']['NummeraanduidingRef']) is not list:
                entry['heeftAlsNevenadres']['NummeraanduidingRef'] = [entry['heeftAlsNevenadres']['NummeraanduidingRef']]
    with open(file, 'w') as f:
        ndjson.dump(json_file, f)
    return file

@task
def fix_geometrie(file):
    with open(file, 'r') as f:
        json_file = ndjson.load(f)
    for entry in json_file:
        if 'interior' not in entry['geometrie']['Polygon'].keys():
            entry['geometrie']['Polygon']['interior'] = []
        else:
            if type(entry['geometrie']['Polygon']['interior']) is not list:
                entry['geometrie']['Polygon']['interior'] = [entry['geometrie']['Polygon']['interior']]

        if 'exterior' not in entry['geometrie']['Polygon'].keys():
            entry['geometrie']['Polygon']['exterior'] = []
        else:
            if type(entry['geometrie']['Polygon']['exterior']) is not list:
                entry['geometrie']['Polygon']['exterior'] = [entry['geometrie']['Polygon']['exterior']]

    with open(file, 'w') as f:
        ndjson.dump(json_file, f)

    return file


@task
def fix_geometrie_with_vlak(file):
    with open(file, 'r') as f:
        json_file = ndjson.load(f)
    for entry in json_file:
        if 'vlak' in entry['geometrie']:
            if 'interior' not in entry['geometrie']['vlak']['Polygon']:
                entry['geometrie']['vlak']['Polygon']['interior'] = []
            else:
                if type(entry['geometrie']['vlak']['Polygon']['interior']) is not list:
                    entry['geometrie']['vlak']['Polygon']['interior'] = [entry['geometrie']['vlak']['Polygon']['interior']]

            if 'exterior' not in entry['geometrie']['vlak']['Polygon']:
                entry['geometrie']['vlak']['Polygon']['exterior'] = []
            else:
                if type(entry['geometrie']['vlak']['Polygon']['exterior']) is not list:
                    entry['geometrie']['vlak']['Polygon']['exterior'] = [entry['geometrie']['vlak']['Polygon']['exterior']]
        elif 'multivlak' in entry['geometrie']:
            if 'interior' not in entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']:
                entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['interior'] = []
            else:
                if type(entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['interior']) is not list:
                    entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['interior'] = [entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['interior']]

            if 'exterior' not in entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']:
                entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['exterior'] = []
            else:
                if type(entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['exterior']) is not list:
                    entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['exterior'] = [entry['geometrie']['multivlak']['MultiSurface']['surfaceMember']['Polygon']['exterior']]

    with open(file, 'w') as f:
        ndjson.dump(json_file, f)

    return file


@task
def fix_maaktDeelUitVan(file):
    with open(file, 'r') as f:
        json_file = ndjson.load(f)
    for entry in json_file:
        if type(entry['maaktDeelUitVan']['PandRef']) is not list:
            entry['maaktDeelUitVan']['PandRef'] = [entry['maaktDeelUitVan']['PandRef']]

    with open(file, 'w') as f:
        ndjson.dump(json_file, f)

    return file


@task
def fix_gebruiksdoel(file):
    with open(file, 'r') as f:
        json_file = ndjson.load(f)

    for entry in json_file:
        if type(entry['gebruiksdoel']) is not list:
            entry['gebruiksdoel'] = [entry['gebruiksdoel']]

    with open(file, 'w') as f:
        ndjson.dump(json_file, f)

    return file


@task(max_retries=5000,retry_delay=timedelta(minutes=5))
def load_bq(name_bag, path_bag, schema_bag):
    table_ref = dataset_ref.table(name_bag.split('/')[-1])
    job_config.schema = schema_bag
    with open(path_bag, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()


with Flow("BAG-Extract") as flow:
    client = bigquery.Client.from_service_account_json(GCP_TOKEN)
    dataset_ref = client.dataset(DATASET)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    # job_config.write_disposition = 'WRITE_TRUNCATE'
    # job_config.autodetect = True
    # job_config.schema = schema

    #  wpl_xml, wpl_dir = create_xml_list.run(WPL_FILE)
    #  wpl_mapped = create_ndjson.map(bag_file=unmapped(WPL_FILE), xml_file=wpl_xml, ndjson_dir=unmapped(wpl_dir), root_tag=unmapped(WPL_ROOT))
    #  load_bq.map(name_bag=unmapped(wpl_dir), path_bag=wpl_mapped, schema_bag=unmapped(schema["wpl"]))

    #  num_xml, num_dir = create_xml_list.run(NUM_FILE)
    #  num_mapped = create_ndjson.map(bag_file=unmapped(NUM_FILE), xml_file=num_xml, ndjson_dir=unmapped(num_dir), root_tag=unmapped(NUM_ROOT))
    #  load_bq.map(name_bag=unmapped(num_dir), path_bag=num_mapped, schema_bag=unmapped(schema["num"]))

    #  opr_xml, opr_dir = create_xml_list.run(OPR_FILE)
    #  opr_mapped = create_ndjson.map(bag_file=unmapped(OPR_FILE), xml_file=opr_xml, ndjson_dir=unmapped(opr_dir), root_tag=unmapped(OPR_ROOT))
    #  load_bq.map(name_bag=unmapped(opr_dir), path_bag=opr_mapped, schema_bag=unmapped(schema["opr"]))

    #  lig_xml, lig_dir = create_xml_list.run(LIG_FILE)
    #  lig_mapped = create_ndjson.map(bag_file=unmapped(LIG_FILE), xml_file=lig_xml, ndjson_dir=unmapped(lig_dir), root_tag=unmapped(LIG_ROOT))
    #  lig_mapped2 = fix_heeftAlsNevenadres.map(lig_mapped)
    #  lig_mapped3 = fix_geometrie.map(lig_mapped2)
    #  load_bq.map(name_bag=unmapped(lig_dir), path_bag=lig_mapped3, schema_bag=unmapped(schema["lig"]))

    #  sta_xml, sta_dir = create_xml_list.run(STA_FILE)
    #  sta_mapped = create_ndjson.map(bag_file=unmapped(STA_FILE), xml_file=sta_xml, ndjson_dir=unmapped(sta_dir), root_tag=unmapped(STA_ROOT))
    #  sta_mapped2 = fix_heeftAlsNevenadres.map(sta_mapped)
    #  sta_mapped3 = fix_geometrie.map(sta_mapped2)
    #  load_bq.map(name_bag=unmapped(sta_dir), path_bag=sta_mapped3, schema_bag=unmapped(schema["sta"]))

    pnd_xml, pnd_dir = create_xml_list.run(PND_FILE)
    pnd_mapped = create_ndjson.map(bag_file=unmapped(PND_FILE), xml_file=pnd_xml, ndjson_dir=unmapped(pnd_dir), root_tag=unmapped(PND_ROOT))
    pnd_mapped2 = fix_geometrie.map(pnd_mapped)
    load_bq.map(name_bag=unmapped(pnd_dir), path_bag=pnd_mapped2, schema_bag=unmapped(schema["pnd"]))

    #  vbo_xml, vbo_dir = create_xml_list.run(VBO_FILE)
    #  vbo_mapped = create_ndjson.map(bag_file=unmapped(VBO_FILE), xml_file=vbo_xml, ndjson_dir=unmapped(vbo_dir), root_tag=unmapped(VBO_ROOT))
    #  vbo_mapped2 = fix_heeftAlsNevenadres.map(vbo_mapped)
    #  vbo_mapped3 = fix_maaktDeelUitVan.map(vbo_mapped2)
    #  vbo_mapped4 = fix_gebruiksdoel.map(vbo_mapped3)
    #  load_bq.map(name_bag=unmapped(vbo_dir), path_bag=vbo_mapped4, schema_bag=unmapped(schema["vbo"]))




def prefect_main():
    flow.run(executor=LocalDaskExecutor(num_workers=1))


if __name__ == "__main__":
    prefect_main()
