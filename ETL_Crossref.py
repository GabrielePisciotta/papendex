# Extract, Transform, Load script
#
# Take each json file from the Crossref dump and create an object containing it that is loaded in Solr

__author__ = 'Gabriele Pisciotta'

from tqdm import tqdm
import json
import pysolr
import time
import gzip
from os import listdir
from os.path import isfile, join, basename, splitext
import re
import tarfile
import argparse


# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files

# Read json content in file
def read_json_file(f):
    with open(f) as data_file:
        data = json.load(data_file)
    return data


# Extract a string from the metadata
def extract_string_from_metadata(content):
    text = ""

    if 'author' in content and len(content['author']) > 0:
        for a in content['author']:
            if 'given' in a and 'family' in a:
                text = "".join([text, a['given'], " ", a['family'], ", "])

    if 'title' in content and len(content['title']) > 0:
        text = "".join([text, content['title'][0], ", "])

    if 'short-container-title' in content and len(content['short-container-title']) > 0:
        text = "".join([text, content['short-container-title'][0], ", "])

    if 'issued' in content and 'date_parts' in content['issued'] and len(content['issued']['date_parts']) > 0:
        dates = "".join([str(x)+ " " for x in content['issued']['date-parts'][0]])
        text = "".join([text, dates, ", "])

    elif 'published-print' in content and 'date-parts' in content['published-print'] and len(content['published-print']['date-parts'][0]) > 0:
        dates = "".join([str(x)+ " " for x in content['published-print']['date-parts'][0]])
        text = "".join([text, dates, ", "])

    if 'volume' in content:
        text = "".join([text, content['volume'], ", "])

    if 'issue' in content:
        text = "".join([text, content['issue'], ", "])

    if 'page' in content:
        text = "".join([text, content['page'], ", "])

    if 'DOI' in content:
        text = "".join([text, content['DOI'].lower()])

    return text

def crossref_ETL(source, start_path, dump_filename, solr_address):
    start = time.time()

    try:
        solr = pysolr.Solr(solr_address, always_commit=True, timeout=1000)
        solr.ping()
        print("Connection enstablished to Solr")

    except:
        print("Can't enstablish a connection to Solr")
        exit()

    json_file = 0
    doc_in_json_file = 0

    if source == 'path':
        inpath = start_path
        file_list = get_files_in_dir(inpath)
    else:
        print("Extracting Crossref dump... This may take a while.")
        crossref_dump_compressed = tarfile.open(dump_filename)
        file_list = crossref_dump_compressed.getmembers()

    # For each file in the crossref compressed dump
    for f in tqdm(file_list):

        if source == 'compressed':
            # Extract a single file from the dump
            f = crossref_dump_compressed.extractfile(f)

            # When extracting the dump, it may happen that is read something that isn't a file
            if f is None:
                continue
            content = json.loads(f.read())
        else:
            # Read the file as a json object
            content = read_json_file(join(inpath, f))

        # Each element in the json object is a single document
        elements = []

        # Iterate through all the element in the chunk
        for element in content['items']:
            # For each document, create the structure of the object that will be updated in Solr
            doc = {
                "id": element['DOI'].lower(),
                "bibref": extract_string_from_metadata(element).encode('utf-8'),
                "original": json.dumps(element).encode('utf-8')
            }

            elements.append(doc)
            doc_in_json_file += 1

        # Upload in Solr the list of elements
        solr.add(elements)
        json_file += 1

    end = time.time()
    print("Loaded {} docs from {} dumps Time ETL: {}".format(doc_in_json_file, json_file, (end - start)))



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("start_path", default="", help="Working path")
    parser.add_argument("source", choices=['path', 'compressed'], default="compressed",
                        help="Kind of source. Can be 'path' or 'compressed'")
    parser.add_argument("dump_filename")
    parser.add_argument("solr_address", default="http://localhost:8983/solr/crossref")

    args = parser.parse_args()

    crossref_ETL(source=args.source, start_path=args.start_path, dump_filename=args.dump_filename, solr_address=args.solr_address)
