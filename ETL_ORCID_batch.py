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
    return id_string
    """
    try:
        doi = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("10."):])))
        doi = doi.lower().strip()
    except:
        return None

    if doi is None or match("^10\\..+/.+$", doi) is None:
        return None
    else:
        return doi
    """

def is_valid_orcid(orcid):
    return True
    """
    if orcid is not None and match("^([0-9]{4}-){3}[0-9]{3}[0-9X]$", orcid):
        return True
    else:
        return None
    """

class Worker():
    def __init__(self):
        self.list = multiprocessing.Manager().list()

    def query_and_deduplicate(self, to_store_items):

        doi, authors_to_store = to_store_items
        local_solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=100)

        # Get the authors list
        query = local_solr.search(q='id:"{}"'.format(doi))

        # Check if it's empty
        if len(query) == 0:
            authors = []
        else:
            authors = json.loads([q['authors'] for q in query][0])

        # Close the socket
        local_solr.get_session().close()

        # Combine the stored authors with the discovered authors in the batch
        authors += authors_to_store

        # Deduplicate them
        authors = [dict(t) for t in {tuple(d.items()) for d in authors}]

        self.list.append({
            "id": doi,
            "authors": json.dumps(authors)
        })



def thread_parallel(func):
    def parallel_func(*args, **kw):
        p = Thread(target=func, args=args, kwargs=kw)
        p.daemon = False
        p.start()
    return parallel_func

def write_to_file(to_store):
    file_id = 0
    print("Writing to file...")
    to_write = []

    for doi, authors_to_store in tqdm(to_store.items()):

        # Deduplicate them
        authors = [dict(t) for t in {tuple(d.items()) for d in authors_to_store}]

        to_write.append({
            "id": "{}".format(json.dumps(doi)),
            "authors": json.dumps(authors)
        })

        if len(to_write) == 10_000:
            with open(os.path.join('docs', '{}.json'.format(file_id)), 'w') as f:
                json.dump(to_write, f)
                file_id += 1
                to_write.clear()
                gc.collect()

    if len(to_write) > 0:
        with open(os.path.join('docs', '{}.json'.format(file_id)), 'w') as f:
            json.dump(to_write, f)
            file_id += 1
            to_write.clear()


#@thread_parallel
def store_data():
    print("Storing data in SOLR...")
    try:
        solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=100)
        import os
        for file in tqdm(os.listdir("docs")):
            with open('docs/{}'.format(file), "r") as f:
                to_add = json.load(f)

                # Add it
                response = solr.add(to_add)

                # Get response
                response = json.loads(response)
                solr.get_session().close()
                gc.collect()

                # If something goes wrong, then print to file
                if response['responseHeader']['status'] != 0:
                    raise Exception
                to_add.clear()


    except Exception as e:
        print(e)



def orcid_ETL():

    print("Extracting Orcid dump... This may take a while.")

    #orcid_dump_compressed = zipfile.ZipFile("/home/gabriele/Universita/Ricerca/OpenCitations CCC/progetti/indexer/papendex/0.zip")
    #activities_dir = [x for x in orcid_dump_compressed.namelist()]

    orcid_dump_compressed = zipfile.ZipFile('/mie/orcid/orcid.zip')
    activities_dir = [x for x in orcid_dump_compressed.namelist() if 'summaries' in x]

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

                    if orcid is None: #or is_valid_orcid(orcid) is None:
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
                                        type = b1.find('{http://www.orcid.org/ns/common}external-id-type')

                                        if type is not None and type.text == 'doi':
                                            c1 = b1.find('{http://www.orcid.org/ns/common}external-id-normalized')
                                            if c1 is not None:
                                                normalised_doi = is_valid_doi(c1.text)
                                                if normalised_doi is not None:
                                                    dois.append(normalised_doi)
                                                else:
                                                    print("Found non-normalised DOI: {}".format(normalised_doi))

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

                except Exception as ex:
                    tree.write('out.xml', pretty_print = True)
                    continue


            # Flush...
            if len(to_store) != 0:
                write_to_file(to_store)
                to_store = {}
                gc.collect()
                store_data()
            os.remove(a)

    end = time.time()
    print("Processed in {:.3f}s".format((end-start)))

def load_error_commit():
    solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=1000)

    with open('error_commit.txt', 'r') as f:
        lines = f.read().split('}],')
        for line in lines:
            line += "}]}"
            line = line.replace("'", '"')
            print(line)
            obj = json.loads(line)
            w = Worker()
            try:
                response = solr.add(w.query_and_deduplicate(obj))

                # Get response
                response = json.loads(response)

                # If something goes wrong, then print to file
                if response['responseHeader']['status'] != 0:
                    raise Exception

            except:
                # If something goes wrong, then print to file
                with open('error_commit.txt', 'a') as f:
                    f.write("%s\n" % obj)

if __name__ == '__main__':
    orcid_ETL()
    #load_error_commit()
