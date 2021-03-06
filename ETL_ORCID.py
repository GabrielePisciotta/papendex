from lxml import etree
from tqdm import tqdm
import json
import pysolr
import time
from os import listdir
from os.path import isfile, join
import re
import tarfile
import os
import gc
from threading import Thread
import argparse
from pathlib import Path


# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files

def write_to_file(to_store, output_path):
    file_id = 0
    print("Writing to file...")
    to_write = []
    Path(os.path.join(output_path,'docs')).mkdir(parents=True, exist_ok=True)

    for doi, authors_to_store in tqdm(to_store.items()):

        # Deduplicate them
        authors = [dict(t) for t in {tuple(d.items()) for d in authors_to_store}]

        to_write.append({
            "id": doi,
            "authors": json.dumps(authors)
        })

        if len(to_write) == 20000:
            with open(os.path.join(output_path, 'docs', '{}.json'.format(file_id)), 'w') as f:
                json.dump(to_write, f)
                file_id += 1
                to_write.clear()
                gc.collect()

    if len(to_write) > 0:
        with open(os.path.join(output_path, 'docs', '{}.json'.format(file_id)), 'w') as f:
            json.dump(to_write, f)
            file_id += 1
            to_write.clear()

def store_data(output_path):
    print("Storing data in SOLR...")
    try:
        solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=True, timeout=100)
        import os
        for file in tqdm(os.listdir(os.path.join(output_path, "docs"))):
            with open(os.path.join(output_path,'docs/{}'.format(file)), "r") as f:
                to_add = json.load(f)

                # Add it
                response = solr.add(to_add)

                # Get response
                response = json.loads(response)
                solr.get_session().close()
                gc.collect()

                # If something goes wrong, raise an exception
                if response['responseHeader']['status'] != 0:
                    print("Exception with file {}".format(f))
                    raise Exception
                to_add.clear()


    except Exception as e:
        print(e)

def thread_parallel(func):
    def parallel_func(*args, **kw):
        p = Thread(target=func, args=args, kwargs=kw)
        p.daemon = True
        p.start()
    return parallel_func

@thread_parallel
def save_orcid_to_file(to_save_orcid, output_path):
    for e in to_save_orcid:
        orcid = e['orcid']
        with open(os.path.join(output_path, 'orcid/{}.txt'.format(orcid)), 'w') as author_file:
            json.dump(e, author_file)

@thread_parallel
def save_exception_file(root, orcid, output_path):
    if orcid != None:
        if 'doi' in etree.tounicode(root, pretty_print=True):
            with open(os.path.join(output_path,'exceptions/{}.xml'.format(orcid)), 'w') as f:
                f.write(etree.tounicode(root, pretty_print=True))

def orcid_ETL(summaries_dump, output_path):

    print("Extracting Orcid dump... This may take a while.")

    start = time.time()
    to_store = {}
    to_save_orcid = []
    print("Extracting {}".format(summaries_dump))

    with tarfile.open(summaries_dump, 'r:gz') as extracted_archive:
        dir_in_extracted_archive = extracted_archive.getmembers()
        for f in tqdm(dir_in_extracted_archive):
            f = extracted_archive.extractfile(f)

            # It may happen that is read something that isn't a file, so this is to skip
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
                if name is not None:
                    given_names = name.find('{http://www.orcid.org/ns/personal-details}given-names')
                    if given_names is not None:
                        given_names = given_names.text
                    else:
                        given_names = ""
                    family_name = name.find('{http://www.orcid.org/ns/personal-details}family-name')
                    if family_name is not None:
                        family_name = family_name.text
                    else:
                        family_name = ""
                else:
                    given_names = ""
                    family_name = ""

                works = root.find('{http://www.orcid.org/ns/activities}activities-summary') \
                    .findall('{http://www.orcid.org/ns/activities}works')

                dois = []

                for w in works:
                    groups = w.findall('{http://www.orcid.org/ns/activities}group')

                    # It's possible that are listed multiple works for each author.
                    # This part is to extract each DOI, check if is valid and in the end
                    # save it as normalised DOI.
                    for g in groups:
                        if g is not None:
                            try:
                                a1 = g.findall('{http://www.orcid.org/ns/common}external-ids')
                                for aa1 in a1:
                                    if aa1 is not None:
                                        b1 = aa1.findall('{http://www.orcid.org/ns/common}external-id')
                                        for bb1 in b1:
                                            if bb1 is not None:
                                                t1 = bb1.findall('{http://www.orcid.org/ns/common}external-id-type')
                                                for type in t1:
                                                    if type is not None and type.text == 'doi':
                                                        c1 = bb1.find('{http://www.orcid.org/ns/common}external-id-normalized')
                                                        if c1 is not None:
                                                            normalised_doi = c1.text
                                                            if normalised_doi is not None:
                                                                dois.append(normalised_doi)
                                                        """else:
                                                            c1 = bb1.find('{http://www.orcid.org/ns/common}external-id-value')
                                                            if c1 is not None:
                                                                normalised_doi = c1.text
                                                                if normalised_doi is not None:
                                                                    dois.append(normalised_doi)"""
                            except AttributeError as ex:
                                print(ex.with_traceback())
                                continue

                if len(dois) != 0:
                    to_save_orcid.append({"orcid": orcid,
                                           "given_names": given_names,
                                           "family_name": family_name,
                                           "dois": dois})

                    for doi in dois:

                        # If the doi is already present in the local batch, we append the orcid to its list
                        if to_store.__contains__(doi):
                            to_store[doi].append({
                                'orcid': orcid,
                                'given_names': given_names,
                                'family_name': family_name
                            })

                        else:
                            to_store[doi] = [{
                                'orcid': orcid,
                                'given_names': given_names,
                                'family_name': family_name
                            }]


            except Exception as ex:
                save_exception_file(root, orcid, output_path)
                continue


        # Flush...
        if len(to_store) != 0:
            write_to_file(to_store, output_path)
            gc.collect()
            store_data(output_path)

    end = time.time()
    print("Processed in {:.3f}s".format((end-start)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("output_path", help="Path where will be stored everything")
    parser.add_argument("summaries_dump", help="Summaries dump")
    args = parser.parse_args()
    orcid_ETL(summaries_dump = args.summaries_dump, output_path=args.output_path)
