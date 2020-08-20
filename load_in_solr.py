import pysolr
from os import listdir
from os.path import isfile, join, basename, splitext
import json
import gzip
import argparse
import re
import time

# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files


# Read json content in file
def read_file(f):
    with gzip.open(f, 'rt', encoding='utf-8') as outfile:
        data = outfile.read()
    return data

def load(solr):
    print(solr.ping())
    list_of_files = get_files_in_dir(in_path)
    for f in list_of_files:
        content = read_file(join(in_path, f))

        solr.add([
            {
                "id": f,  # filename,
                "title": content,  # summarizing string
            }
        ])
        print("Added {}".format(f))


#parser = argparse.ArgumentParser()
#parser.add_argument("in_path")
#parser.add_argument("out_path")
#args = parser.parse_args()
#in_path = args.in_path
#out_path = args.out_path
in_path = '/mie/linearized/'

solr = pysolr.Solr('http://localhost:8983/solr/papendex', always_commit=True)

# Uncomment this to load data
#load(solr)


start = time.time()
results = solr.search("title:LAURENTP 10.1002/chin.198407289")
end = time.time()
print("Time: {}".format((end-start)))

print("Saw {0} result(s).".format(len(results)))
for result in results:
    print("'{0}'.".format(result))










