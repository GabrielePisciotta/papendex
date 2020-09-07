from lxml import etree
from tqdm import tqdm
import json
import pysolr
import time
from os import listdir
from os.path import isfile, join
import re
import tarfile
import zipfile
import os
from re import sub, match
from urllib.parse import unquote
import gc
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files

def is_valid(id_string):
    doi = normalise(id_string, include_prefix=False)

    if doi is None or match("^10\\..+/.+$", doi) is None:
        return None
    else:
        return doi

def normalise(id_string, include_prefix=False):
    try:
        id_string = id_string.replace("doi:", "")
        id_string = id_string.replace("DOI:", "")
        id_string = id_string.replace("https://doi.org/", "")
        id_string = id_string.replace("https://doi.org/", "")
        id_string = id_string.replace("http://dx.doi.org/", "")

        doi_string = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("10."):])))
        return doi_string.lower().strip()
    except:  # Any error in processing the DOI will return None
        return None

class Worker():

    @staticmethod
    def query_and_deduplicate(to_store_items):
        try:
            solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=1000)
            solr.ping()

        except:
            print("Can't enstablish a connection to Solr")
            exit()

        doi, authors_to_store = to_store_items

        # Get the author list object
        query = solr.search(q='id:"{}"'.format(doi))
        author_list = [q['authors'] for q in query]
        if len(author_list) == 0:
            authors = json.loads('[]')
        else:
            authors = json.loads(author_list[0])

        solr.get_session().close()

        # Combine the stored authors with the discovered authors in the batch
        authors += authors_to_store

        # Deduplicate them
        authors = [dict(t) for t in {tuple(d.items()) for d in authors}]

        return {
            'id': doi,
            'authors': json.dumps(authors)
        }


def store_data(solr, to_store):
    w = Worker()

    with multiprocessing.Pool(8) as executor:
        future_results = executor.map(w.query_and_deduplicate , to_store.items())
        to_commit = [result for result in future_results]

    solr.add(to_commit)
    to_commit.clear()
    gc.collect()

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
        orcid_dump_compressed = zipfile.ZipFile("/home/gabriele/Universita/Ricerca/OpenCitations CCC/progetti/indexer/papendex/0.zip")
        activities_dir = [x for x in orcid_dump_compressed.namelist()]

        #orcid_dump_compressed = zipfile.ZipFile('/mie/orcid/orcid.zip')
        #activities_dir = [x for x in orcid_dump_compressed.namelist() if 'summaries' in x]

        start = time.time()
        to_store = {}

        for a in activities_dir:
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

                        # It's possible that are listed multiple works for each author.
                        # This part is to extract each DOI, check if is valid and in the end
                        # save it as normalised DOI.
                        for g in groups:
                            if g is not None:
                                try:
                                    a1 = g.find('{http://www.orcid.org/ns/common}external-ids')
                                    if a1 is not None:
                                        b1 = a1.find('{http://www.orcid.org/ns/common}external-id')
                                        if b1 is not None:
                                            c1 = b1.find('{http://www.orcid.org/ns/common}external-id-value')

                                            if c1 is not None:
                                                normalised_doi = is_valid(c1.text)
                                                if normalised_doi is not None:
                                                    dois.append(normalised_doi)

                                except AttributeError as ex:
                                    print(ex.with_traceback())
                                    continue

                        for doi in dois:

                            # If the doi is already present in the local batch
                            if to_store.__contains__(doi):
                                actual_values = to_store[doi]

                                # if the author does not exist
                                if len(list(filter(lambda x: x["orcid"] == orcid, actual_values))) == 0:
                                    actual_values.append({
                                    'orcid': orcid,
                                    'given_names': given_names,
                                    'family_name': family_name
                                })
                                to_store[doi] = actual_values

                            else:
                                to_store[doi] = [{
                                    'orcid': orcid,
                                    'given_names': given_names,
                                    'family_name': family_name
                                }]

                        if len(to_store) > 100000:
                            store_data(solr, to_store)


                    except Exception as ex:
                        tree.write('out.xml', pretty_print = True)
                        #print(ex)
                        continue

                # Flush...
                if len(to_store) != 0:
                    store_data(solr, to_store)
                os.remove(a)

    end = time.time()
    print("Processed in {:.3f}s".format((end-start)))

if __name__ == '__main__':
    orcid_ETL()
