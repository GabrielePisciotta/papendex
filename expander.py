# This script simply expand each Crossref set of json document into single files

__author__ = 'Gabriele Pisciotta'
from os import listdir
from os.path import isfile, join, basename, splitext
import json
import argparse
import re

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
    with open(join(out_path, '{}.json'.format(id)), 'w') as outfile:
        json.dump(data, outfile, indent=2)

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

#parser = argparse.ArgumentParser()
#parser.add_argument("in_path")
#parser.add_argument("out_path")
#args = parser.parse_args()
#in_path = args.in_path
#out_path = args.out_path
in_path = '/mie/scompatt/'
out_path = '/mie/expanded/'

list_of_files = [ get_files_in_dir(in_path)[-1] ]
for f in list_of_files:
    content = read_json_file(join(in_path, f))
    for it in content['items']:
        id = get_last_id(out_path)
        write_json_file(id, out_path, it)

