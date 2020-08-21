# This script extract each json file from the crossref dump, transform it and load in Solr
__author__ = 'Gabriele Pisciotta'
from tqdm import tqdm
import tarfile
import codecs
from os import listdir
from os.path import isfile, join, basename, splitext
import json
import gzip
import re
import pysolr
import time

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

# Write json content in file
def write_json_file(id, out_path, data):
    with gzip.open(join(out_path, '{}.json'.format(id)), 'wt', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=0)

# Return an id related to the next file to be written
def get_last_id(path):
    last_file = get_files_in_dir(path)
    if len(last_file) == 0:
        return 0
    else:
        last_file = last_file[-1]
        base = basename(last_file)
        id = int(splitext(base)[0])
        id = id+1
        return id

# Extract a string from the metadata
def extract_string_from_metadata(content):
    text = ""

    if 'author' in content:
        for a in content['author']:
            if 'given' in a and 'family' in a:
                text = "".join([text, a['given'], " ", a['family']])

    if 'title' in content and len(content['title']) > 0:
        text = "".join([text, ", ", content['title'][0]])

    if 'short-container-title' in content and len(content['short-container-title']) > 0:
        text = "".join([text, ", ", content['short-container-title'][0]])

    if 'published-print' in content and 'date-parts' in content['published-print']:
        dates = "".join([str(x) for x in content['published-print']['date-parts'][0]])

        text = "".join([text, ", ", dates])

    if 'DOI' in content:
        text = "".join([text, ", ", content['DOI'].lower()])

    return text



solr = pysolr.Solr('http://localhost:8983/solr/papendex', always_commit=True)
print(solr.ping())

crossref_dump_file = "/mie/crossref-data-2020-06.tar.gz"
#crossref_dump_file = "/mie/0.tar.xz"
crossref_dump_compressed = tarfile.open(crossref_dump_file)

_id = 0

# For each json chunk in the crossref compressed dump
for member in tqdm(crossref_dump_compressed.getmembers()):

    # Extract a single file from the dump
    f=crossref_dump_compressed.extractfile(member)

    # Read it as a json object
    content=json.loads(f.read())

    # Each element in the json object is a single document
    elements = []

    # Iterate through all the element in the chunk
    for element in content['items']:

        # For each document, create the structure of the object that will be updated in Solr
        elements.append({"id":_id,
                         "doi": element['DOI'],
                         "title":extract_string_from_metadata(element),
                         "original": str(element)})

        _id += 1

    # Upload in Solr the list of elements
    solr.add(elements)

# Example of query for a single DOI
start = time.time()
results = solr.search('doi:"10.1001/.389" ')
end = time.time()
print("Time: {}".format((end-start)))

print("Saw {0} result(s).".format(len(results)))
for result in results:
    print("'{0}'.".format(result))