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
import multiprocessing
from threading import Thread

# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files

def is_valid_doi(id_string):
    try:
        id_string = id_string.replace("doi:", "")
        id_string = id_string.replace("DOI:", "")
        id_string = id_string.replace("https://doi.org/", "")
        id_string = id_string.replace("http://doi.org/", "")
        id_string = id_string.replace("http://dx.doi.org/", "")
        id_string = id_string.replace("https://dx.doi.org/", "")
        if id_string[-1] == '\\': id_string = id_string[:-1]  # remove the backslash found at the end of some DOI

        doi = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("10."):])))

        doi = doi.lower().strip()
    except:
        return None

    if doi is None or match("^10\\..+/.+$", doi) is None:
        return None
    else:
        return doi

def is_valid_orcid(orcid):
    if orcid is not None and match("^([0-9]{4}-){3}[0-9]{3}[0-9X]$", orcid):
        return True
    else:
        return None


class Worker():

    @staticmethod
    def query_and_deduplicate(to_store_items):
        try:
            local_solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=50)
            local_solr.ping()

        except:
            print("Can't enstablish a connection to Solr")
            return None

        doi, authors_to_store = to_store_items

        # Get the authors list
        query = local_solr.search(q='id:"{}"'.format(doi))
        author_list = [q['authors'] for q in query]

        # Close the socket
        local_solr.get_session().close()

        # Check if it's empty
        if len(author_list) == 0:
            authors = []
        else:
            authors = json.loads(author_list[0])

        # Combine the stored authors with the discovered authors in the batch
        authors += authors_to_store

        # Deduplicate them
        authors = [dict(t) for t in {tuple(d.items()) for d in authors}]

        return {
            'id': doi,
            'authors': json.dumps(authors)
        }



def thread_parallel(func):
    def parallel_func(*args, **kw):
        p = Thread(target=func, args=args, kwargs=kw)
        p.daemon = False
        p.start()
    return parallel_func

#@thread_parallel
def store_data(to_store):
    solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=50)

    print("Len of to_store: {}".format(len(to_store)))

    w = Worker()

    with multiprocessing.Pool(processes=8) as executor:
        future_results = executor.map(w.query_and_deduplicate , to_store.items())
        to_add = [result for result in future_results]

    # Add it
    try:
        response = solr.add(to_add)

        # Get response
        response = json.loads(response)

        # If something goes wrong, then print to file
        if response['responseHeader']['status'] != 0:
            raise Exception

    except:
        # If something goes wrong, then print to file
        if response['responseHeader']['status'] != 0:
            with open('error_commit.txt', 'a') as f:
                    f.write("%s\n" % to_add)


def orcid_ETL():

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

                    if orcid is None or is_valid_orcid(orcid) is None:
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
                                            normalised_doi = is_valid_doi(c1.text)
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

                    if len(to_store) > 5_000:
                        store_data(to_store)
                        to_store.clear()


                except Exception as ex:
                    tree.write('out.xml', pretty_print = True)
                    continue

            # Flush...
            if len(to_store) != 0:
                store_data(to_store)
                to_store.clear()

            os.remove(a)

    end = time.time()
    print("Processed in {:.3f}s".format((end-start)))

if __name__ == '__main__':
    orcid_ETL()
