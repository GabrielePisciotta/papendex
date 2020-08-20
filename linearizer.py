# The aim of this script is to extract a single string from a structured document
__author__ = 'Gabriele Pisciotta'
from os import listdir
from os.path import isfile, join, basename, splitext
import json
import gzip
import argparse
import re

# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files


# Read json content in file
def read_json_file(f):
    with gzip.open(f) as data_file:
        data = json.load(data_file)
    return data

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

# Write json content in file
def write_to_file(id, out_path, data):
    with gzip.open(join(out_path, '{}.txt'.format(id)), 'wt', encoding='utf-8') as outfile:
        outfile.write(data)


#parser = argparse.ArgumentParser()
#parser.add_argument("in_path")
#parser.add_argument("out_path")
#args = parser.parse_args()
#in_path = args.in_path
#out_path = args.out_path
in_path = '/mie/expanded/'
out_path = '/mie/linearized/'

list_of_files = get_files_in_dir(in_path)
for f in list_of_files:
    content = read_json_file(join(in_path, f))
    id = get_last_id(out_path)
    text = ""

    for a in content['author']:
        if 'given' in a and 'family' in a:
            text = "".join([text, a['given'], " ", a['family']])

    if 'title' in content:
        text = "".join([text, ", ", content['title'][0]])

    if 'short-container-title' in content:
        text = "".join([text, ", ", content['short-container-title'][0]])

    if 'published-print' in content and 'date-parts' in content['published-print']:

        dates = "".join([str(content['published-print']['date-parts'][0][0]), " ",
                        str(content['published-print']['date-parts'][0][1]), " ",
                        str(content['published-print']['date-parts'][0][2])])

        text = "".join([text, ", ", dates])

    if 'DOI' in content:
        text = "".join([text, ", ", content['DOI'].lower()])

    write_to_file(id, out_path, text)
