from lxml import etree
from tqdm import tqdm
import json
import pysolr
import time
import gzip
from os import listdir
from os.path import isfile, join, basename, splitext
import re
import tarfile
import collections
import zipfile
import os
import shutil
from io import BytesIO, StringIO
import gc

# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files

def get_author_list(solr, doi):
    query = solr.search(q='id:"{}"'.format(doi))
    author_list = [q['authors'] for q in query]
    if len(author_list) == 0:
        return  json.loads('[]')
    else:
        return json.loads(author_list[0])




def orcid_ETL(source='compressed', solr_address='http://localhost:8983/solr/orcid'):
    try:
        solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=1000)
        solr.ping()
        print("Connection enstablished to Solr")

    except:
        print("Can't enstablish a connection to Solr")
        exit()

    if source=='path':
        inpath = '/mie/orcid/000/'
        file_list = get_files_in_dir(inpath)
    else:
        print("Extracting Orcid dump... This may take a while.")
        orcid_dump_compressed = zipfile.ZipFile('/mie/orcid/orcid.zip')
        #orcid_dump_compressed = zipfile.ZipFile("/home/gabriele/Universita/Ricerca/OpenCitations CCC/progetti/indexer/papendex/0.zip")
        activities_dir = [x for x in orcid_dump_compressed.namelist() if 'summaries' in x]
        start = time.time()
        to_commit = []

        for a in activities_dir:
            print(a)
            print("Extracting {}".format(a))

            orcid_dump_compressed.extract(a)

            with tarfile.open(a, 'r:gz') as extracted_archive:

                dir_in_extracted_archive = extracted_archive.getmembers()
                for f in tqdm(dir_in_extracted_archive):
                    f = extracted_archive.extractfile(f)
                    # When extracting the dump, it may happen that is read something that isn't a file
                    if f is None:
                        continue

                    parser = etree.XMLParser()

                    try:
                        tree = etree.parse(f, parser)
                        root = tree.getroot()

                        orcid = root.find('{http://www.orcid.org/ns/common}orcid-identifier') \
                            .find('{http://www.orcid.org/ns/common}path') \
                            .text

                        if orcid is None:
                            continue

                        name = root.find('{http://www.orcid.org/ns/person}person') \
                            .find('{http://www.orcid.org/ns/person}name')

                        given_names = name.find('{http://www.orcid.org/ns/personal-details}given-names').text
                        family_name = name.find('{http://www.orcid.org/ns/personal-details}family-name').text

                        groups = root.find('{http://www.orcid.org/ns/activities}activities-summary') \
                            .find('{http://www.orcid.org/ns/activities}works') \
                            .findall('{http://www.orcid.org/ns/activities}group')

                        dois = []
                        for g in groups:
                            if g is not None:
                                try:
                                    a1 = g.find('{http://www.orcid.org/ns/common}external-ids')
                                    if a1 is not None:
                                        b1 = a1.find('{http://www.orcid.org/ns/common}external-id')
                                        if b1 is not None:
                                            c1 = b1.find('{http://www.orcid.org/ns/common}external-id-value')
                                            if c1 is not None:
                                                dois.append(c1.text.lower().strip())

                                except AttributeError as ex:
                                    for xx in to_commit:
                                        print(xx)
                                    print(ex)
                                    continue

                        for doi in dois:

                            # Get the author list object
                            authors = get_author_list(solr, doi)

                            # Check if author is not present
                            if len(list(filter(lambda x: x["orcid"] == orcid, authors))) == 0:
                                authors.append({
                                    'orcid': orcid,
                                    'given_names': given_names,
                                    'family_name': family_name
                                })

                                # Update it
                                to_commit.append({
                                    'id': doi,
                                    'authors': json.dumps(authors)
                                })

                        if len(to_commit) > 100000:
                            solr.add(to_commit)
                            to_commit.clear()
                            gc.collect()


                    except Exception as ex:
                        tree.write('out.xml', pretty_print = True)
                        #print(ex)
                        continue

                # Flush...
                if len(to_commit) != 0:
                    solr.add(to_commit)
                    to_commit.clear
                os.remove(a)

    end = time.time()
    print("Processed in {:.3f}s".format((end-start)))

if __name__ == '__main__':
    orcid_ETL()
