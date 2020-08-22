# This script extract each json file from the crossref dump, transform it and load in Solr
__author__ = 'Gabriele Pisciotta'
from tqdm import tqdm
import tarfile
import json
import pysolr
import time

# Extract a string from the metadata
def extract_string_from_metadata(content):
    text = ""

    if 'author' in content:
        for a in content['author']:
            if 'given' in a and 'family' in a:
                text = "".join([text, a['given'], " ", a['family'], " "])

    if 'title' in content and len(content['title']) > 0:
        text = "".join([text, ", ", content['title'][0]])

    if 'short-container-title' in content and len(content['short-container-title']) > 0:
        text = "".join([text, ", ", content['short-container-title'][0]])

    if 'published-print' in content and 'date-parts' in content['published-print'] and len(content['published-print']['date-parts'][0]) > 0:
        dates = "".join([str(x)+ " " for x in content['published-print']['date-parts'][0]])
        text = "".join([text, ", ", dates])

    if 'DOI' in content:
        text = "".join([text, ", ", content['DOI'].lower()])

    return text


start = time.time()

solr = pysolr.Solr('http://localhost:8983/solr/papendex', always_commit=True, timeout=10000000)
response = json.loads(solr.ping())
if response['status'] != 'OK':
    print("Can't enstablish a connection to Solr")
    exit
else:
    print("Connection enstablished to Solr")

crossref_dump_file = "/mie/crossref-data-2020-06.tar.gz"
#crossref_dump_file = "/mie/0.tar.xz"
crossref_dump_compressed = tarfile.open(crossref_dump_file)

_id = 0

print("Extracting Crossref dump... This may take a while.")


# For each json chunk in the crossref compressed dump
for member in tqdm(crossref_dump_compressed.getmembers()):
        
    # Extract a single file from the dump
    f=crossref_dump_compressed.extractfile(member)

    # When extracting the dump, it may happen that is read something that isn't a file 
    if f is None:
        continue

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
                         "original": json.dumps(element)
                         })

        _id += 1

    # Upload in Solr the list of elements
    solr.add(elements)
    time.sleep(10)

end = time.time()
print("Time ETL: {}".format((end-start)))

# Example of query for a single DOI
start_query = time.time()
results = solr.search('doi:"10.1002/14651858.cd005055.pub2"')
end_query = time.time()
print("Time for query: {}".format((end_query-start_query)))

print("Saw {0} result(s).".format(len(results)))
for result in results:
    print("'{0}'.".format(result))
