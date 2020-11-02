from os import listdir, system, remove
from os.path import isfile, join
import re
import multiprocessing
from urllib.parse import unquote
import json
from lxml import etree
import pandas as pd
import tqdm
import time
import httplib2
from bs4 import BeautifulSoup, SoupStrainer
import wget
from multiprocessing.pool import ThreadPool
import os
import uuid
from queue import Queue
from typing import Optional
import csv
from threading import Thread
import pickle

__author__ = "Gabriele Pisciotta"

class EuropePubMedCentralDataset:
    def __init__(self, start_path, writing_multiple_csv = True):
        self.pubmed_file_path = start_path
        self.pubmed_dump_file_path = join(self.pubmed_file_path, 'dump')
        self.articles_path = join(self.pubmed_file_path, 'articles')
        self.csv_file_path = join(self.pubmed_file_path, 'csv')
        
        # We can both exploit a queue in order to write into a single dataset.csv
        # or to save multiple csv and then concatenate them into the final dataset
        self.writing_multiple_csv = writing_multiple_csv
        
        if not self.writing_multiple_csv:
            self.queue = Queue()

        os.makedirs(self.articles_path, exist_ok=True)
        os.makedirs(self.csv_file_path, exist_ok=True)
        os.makedirs(self.pubmed_dump_file_path, exist_ok=True)

    def start(self, skip_download = True):
        if not skip_download:
            # for each file from the pubmed dump
            f = self._get_files_in_dir(self.pubmed_dump_file_path)

            # get the difference between files to download and files that we have
            links = self._get_links_from_pubmed()
            todownload = set(links).difference(set(f))

            if len(todownload):
                print(f"Downloading {len(todownload)} files")
                with multiprocessing.Pool(20) as pool:
                    pool.map(worker_download_links, ((d, self.pubmed_dump_file_path) for d in todownload))

        # Update the file list
        f = self._get_files_in_dir(self.pubmed_dump_file_path)

        # Download articles' IDs --
        if not os.path.isfile(join(self.pubmed_file_path, 'PMC-ids.csv.gz')):
            print("Downloading PMC's IDs dataset")
            wget.download(f'ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz', self.pubmed_file_path)

        # Pickle a dictionary of the dataframe containing only the keys that we care about
        if not os.path.isfile(join(self.pubmed_file_path, 'PMC-ids.pkl')):

            # Read the dataset and create a single big dict having all the needed keys for entity resolution
            articleids = pd.read_csv(join(self.pubmed_file_path, 'PMC-ids.csv.gz'), usecols=['PMCID', 'PMID', 'DOI'],
                                     low_memory=True)

            view = articleids[articleids['PMID'].notna()]
            view['PMID'] = view['PMID'].astype(int)
            dataset = view.set_index('PMID').to_dict('index')
            del view

            view = articleids[articleids['PMCID'].notna()]
            view['PMID'] = view['PMID'].astype('Int64')

            del articleids
            self.articleids = {**dataset, **view.set_index('PMCID').to_dict('index')}
            del view

            pickle.dump(obj=self.articleids, file=open(join(self.pubmed_file_path, 'PMC-ids.pkl'), 'wb'))

        else:
            self.articleids = pickle.load( open(join(self.pubmed_file_path, 'PMC-ids.pkl'), 'rb'))

        # Unzip all the files
        print("Unzipping all the articles")
        s = time.time()
        with ThreadPool(1) as pool:
            list(tqdm.tqdm(pool.imap(self.worker_unzip_files, f), total=len(f)))
        e = time.time()
        print(f"Time: {(e - s)}")

        # process each article
        s = time.time()
        print("Processing the articles")
        self.process_articles()
        e = time.time()
        print(f"Time: {(e - s)}")

        print("Concatenating dataset")
        s = time.time()
        self._concatenate_datasets(self.csv_file_path)
        e = time.time()
        print(f"Time: {(e - s)}")

    def write_to_csv(self):
        keys = ['cur_doi', 'cur_pmid', 'cur_pmcid', 'cur_name', 'references']
        while True:
            if not self.queue.empty():
                row = self.queue.get()
                if row == "STOP":
                    return
                else:
                    row = [v for k,v in row.items()]

                    if not os.path.isfile(join(self.csv_file_path, "dataset.csv")):
                        with open(join(self.csv_file_path, "dataset.csv"), 'w', newline='')  as output_file:
                            dict_writer = csv.writer(output_file, delimiter='\t')
                            dict_writer.writerow(keys)
                            dict_writer.writerow(row)
                    else:
                        with open(join(self.csv_file_path, "dataset.csv"), 'a', newline='')  as output_file:
                            dict_writer = csv.writer(output_file, delimiter='\t')
                            dict_writer.writerow(row)


    def worker_article(self, f: str) -> None:

        # Use the extracted file
        with open(join(self.articles_path, f), 'r') as fi:
            try:
                cur_xml = etree.parse(fi)
            except Exception as e:
                print(e)
                os.makedirs(join(self.articles_path, 'exceptions'), exist_ok=True)
                with open(join(self.articles_path, f), 'w') as fout:
                    fout.write(fi.read())
                os.remove(join(self.articles_path, f))
                return
            cur_pmid = self.get_id_from_xml_source(cur_xml, 'pmid')
            cur_pmcid = self.get_id_from_xml_source(cur_xml, 'pmcid')
            if cur_pmcid is not None and not cur_pmcid.startswith("PMC"):
                    cur_pmcid = f"PMC{cur_pmcid}"
            cur_doi = self.normalise_doi(self.get_id_from_xml_source(cur_xml, 'doi'))

            # If we have no identifier, stop the processing of the article
            if cur_pmid is None and cur_pmcid is None and cur_doi is None:
                os.makedirs(join(self.articles_path, 'exceptions'), exist_ok=True)
                with open(join(self.articles_path, f), 'w') as fout:
                    fout.write(fi.read())
                os.remove(join(self.articles_path, f))
                return

            try:
                # Extract missing metadata from the ID dataset
                if cur_pmid is None or cur_pmcid is None or cur_doi is None:
                    row = None
                    if cur_pmid is not None and self.articleids.__contains__(int(cur_pmid)):
                        row = self.articleids[int(cur_pmid)]
                    elif cur_pmcid is not None and self.articleids.__contains__(cur_pmcid):
                        row = self.articleids[cur_pmcid]

                    if row is not None and len(row):

                        if cur_pmid is None and row['PMID'] is not None and not pd.isna(row['PMID']):
                            cur_pmid = row['PMID']

                        if cur_pmcid is None and row['PMCID'] is not None:
                            cur_pmcid = row['PMCID']

                        if cur_doi is None and row['DOI'] is not None:
                            cur_doi = self.normalise_doi(str(row['DOI']))

                references = cur_xml.xpath(".//ref-list/ref")
                references_list = []

                if len(references):
                    for reference in references:
                        entry_text = self.create_entry_xml(reference)
                        ref_pmid = None
                        ref_doi = None
                        ref_pmcid = None
                        ref_url = None

                        ref_xmlid_attr = reference.get('id')
                        if len(ref_xmlid_attr):
                            ref_xmlid = ref_xmlid_attr
                            if ref_xmlid == "":
                                ref_xmlid = None

                        ref_pmid_el = reference.xpath(".//pub-id[@pub-id-type='pmid']")
                        if len(ref_pmid_el):
                            ref_pmid = etree.tostring(
                                ref_pmid_el[0], method="text", encoding='unicode').strip()

                        ref_doi_el = reference.xpath(".//pub-id[@pub-id-type='doi']")
                        if len(ref_doi_el):
                            ref_doi = self.normalise_doi(etree.tostring(
                                ref_doi_el[0], method="text", encoding='unicode').lower().strip())
                            if ref_doi == "":
                                ref_doi = None

                        ref_pmcid_el = reference.xpath(".//pub-id[@pub-id-type='pmcid']")
                        if len(ref_pmcid_el):
                            ref_pmcid = etree.tostring(
                                ref_pmcid_el[0], method="text", encoding='unicode').strip()
                            if ref_pmcid == "":
                                ref_pmcid = None
                            elif not ref_pmcid.startswith("PMC"):
                                ref_pmcid = f"PMC{ref_pmcid}"

                        ref_url_el = reference.xpath(".//ext-link")
                        if len(ref_url_el):
                            ref_url = etree.tostring(
                                ref_url_el[0], method="text", encoding='unicode').strip()
                            if not ref_url.startswith("http"):
                                ref_url = None

                        # Extract missing metadata from the ID dataset
                        if ref_pmid is None or ref_pmcid is None or ref_doi is None:
                            row = None
                            if ref_pmid is not None and self.articleids.__contains__(int(ref_pmid)):
                                row = self.articleids[int(ref_pmid)]
                            elif ref_pmcid is not None and self.articleids.__contains__(ref_pmcid):
                                row = self.articleids[ref_pmcid]

                            if row is not None and len(row):
                                if ref_pmid is None and row['PMID'] is not None:
                                    ref_pmid = row['PMID']

                                if ref_pmcid is None and row['PMCID'] is not None:
                                    ref_pmcid = row['PMCID']
                                    if not ref_pmcid.startswith("PMC"):
                                        ref_pmcid = f"PMC{ref_pmcid}"

                                if ref_doi is None and row['DOI'] is not None:
                                    ref_doi = self.normalise_doi(str(row['DOI']))

                        # Create an object to store the reference
                        obj = {}
                        if entry_text is not None:
                            obj['entry_text'] = entry_text
                        if ref_pmid is not None:
                            obj['ref_pmid'] = str(ref_pmid)
                        if ref_pmcid is not None:
                            obj['ref_pmcid'] = ref_pmcid
                        if ref_doi is not None:
                            obj['ref_doi'] = ref_doi
                        if ref_url is not None:
                            obj['ref_url'] = ref_url
                        if ref_xmlid is not None:
                            obj['ref_xmlid'] = ref_xmlid
                        references_list.append(obj)

                    if self.writing_multiple_csv:
                        df = pd.DataFrame({
                            'cur_doi': [cur_doi],
                            'cur_pmid': [cur_pmid],
                            'cur_pmcid': [cur_pmcid],
                            'cur_name': [f],
                            'references': [json.dumps(references_list)]
                        })
                        df.to_csv(join(self.csv_file_path, f"{f}.csv"), sep="\t")
                    else:
                        self.queue.put({
                            'cur_doi': cur_doi,
                            'cur_pmid': cur_pmid,
                            'cur_pmcid': cur_pmcid,
                            'cur_name': f,
                            'references': json.dumps(references_list)
                        })

            except Exception as e:
                os.makedirs(join(self.articles_path, 'exceptions'), exist_ok=True)
                with open(join(self.articles_path, f), 'w') as fout:
                    fout.write(fi.read())
                os.remove(join(self.articles_path, f))
                print(f"Exception {e} with file: {f}")

    def process_articles(self):

        f = self._get_files_in_dir(self.articles_path)

        if not self.writing_multiple_csv:
            consumer = Thread(target=self.write_to_csv)
            consumer.setDaemon(True)
            consumer.start()

        with ThreadPool(1000) as pool:
            list(tqdm.tqdm(pool.imap(self.worker_article, (fi for fi in f)), total=len(f)))

        if not self.writing_multiple_csv:
            self.queue.put("STOP")
            consumer.join()

    @staticmethod
    def normalise_doi(doi_string) -> Optional[str]:  # taken from https://github.com/opencitations/index/blob/master/identifier/doimanager.py
        if doi_string is not None:
            try:
                doi_string = re.sub("\0+", "", re.sub("\s+", "", unquote(doi_string[doi_string.index("10."):])))
                return doi_string.lower().strip()
            except ValueError:
                return None
        else:
            return None

    def worker_unzip_files(self, f: str) -> None:
        # Unzip
        system(f"gunzip {join(self.pubmed_dump_file_path, f)}")

        # This is the new filename
        f = f.replace(".gz", "")

        # Create one file for each article, having its named
        tree = etree.parse(join(self.pubmed_dump_file_path, f), etree.XMLParser(remove_blank_text=True))

        # Extract all the article nodes
        articles = tree.findall('article')

        for cur_xml in articles:
            with open(join(self.articles_path, f"{str(uuid.uuid4())}.xml"), 'w') as writefile:
                writefile.write(etree.tostring(cur_xml, pretty_print=True, encoding='unicode'))

        # Remove the downloaded dump
        remove(join(self.pubmed_dump_file_path, f))

    @staticmethod
    def create_entry_xml(xml_ref):  # Taken from CCC
        entry_string = ""

        el_citation = xml_ref.xpath("./element-citation | ./mixed-citation | ./citation")
        if len(el_citation):
            cur_el = el_citation[0]
            is_element_citation = cur_el.tag == "element-citation" or cur_el.tag == "citation"
            has_list_of_people = False
            first_text_passed = False
            for el in cur_el.xpath(".//node()"):
                type_name = type(el).__name__
                if type_name == "_Element":
                    cur_text = el.text
                    if cur_text is not None and " ".join(cur_text.split()) != "":
                        if first_text_passed:
                            is_in_person_group = len(el.xpath("ancestor::person-group")) > 0
                            if is_in_person_group:
                                entry_string += ", "
                                has_list_of_people = True
                            elif not is_in_person_group and has_list_of_people:
                                entry_string += ". "
                                has_list_of_people = False
                            else:
                                if is_element_citation:
                                    entry_string += ", "
                                else:
                                    entry_string += " "
                        else:
                            first_text_passed = True
                    if el.tag == "pub-id":
                        if el.xpath("./@pub-id-type = 'doi'"):
                            entry_string += "DOI: "
                        elif el.xpath("./@pub-id-type = 'pmid'"):
                            entry_string += "PMID: "
                        elif el.xpath("./@pub-id-type = 'pmcid'"):
                            entry_string += "PMC: "
                elif type_name == "_ElementStringResult" or type_name == "_ElementUnicodeResult":
                    entry_string += el
            del cur_el
            del el

        entry_string = " ".join(entry_string.split())
        entry_string = re.sub(" ([,\.!\?;:])", "\\1", entry_string)
        entry_string = re.sub("([\-–––]) ", "\\1", entry_string)
        entry_string = re.sub("[\-–––,\.!\?;:] ?([\-–––,\.!\?;:])", "\\1", entry_string)
        entry_string = re.sub("(\(\. ?)+", "(", entry_string)
        entry_string = re.sub("(\( +)", "(", entry_string)

        del el_citation

        if entry_string is not None and entry_string != "":
            return entry_string
        else:
            return None

    @staticmethod
    def get_id_from_xml_source(cur_xml, id_type):
        """This method extract an id_type from the XML"""

        if id_type not in ["doi", "pmid", "pmcid"]:
            print(f"Wrong id used: {id_type}")
            return None

        id_string = cur_xml.xpath(f".//front/article-meta/article-id[@pub-id-type='{id_type}']")

        if len(id_string):
            id_string = u"" + etree.tostring(id_string[0], method="text", encoding='unicode').strip()
            if id_string != "":
                del cur_xml
                toret = str(id_string)
                del id_string
                return toret

    # Get list of file inside the dir
    def _get_files_in_dir(self, path: str) -> list:
        list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
        return list_of_files

    def _concatenate_datasets(self, path: str) -> str:
        if self.writing_multiple_csv:
            present_files = list(self._get_files_in_dir(path))
            header_saved = False

            with open(join(path, 'dataset.csv'), 'w') as fout:
                for f in tqdm.tqdm(present_files):
                    if f != "dataset.csv":
                        with open(join(path, f)) as fin:
                            header = next(fin)
                            if not header_saved:
                                fout.write(header)
                                header_saved = True
                            for line in fin:
                                fout.write(line)
                        os.remove(join(path, f))

        df = pd.read_csv(join(path, 'dataset.csv'), sep='\t')
        df.drop_duplicates(inplace=True)
        df.to_csv(join(path, 'dataset.csv'), sep='\t')

        return join(path, 'dataset.csv')

    def _get_links_from_pubmed(self) -> list:
        links = []
        http = httplib2.Http()
        status, response = http.request('http://europepmc.org/ftp/oa/')

        for link in BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('a')):
            if link.has_attr('href'):
                if "xml.gz" in link['href']:
                    links.append(link['href'])
        return links

def worker_download_links(args):
    todownload, pubmed_dump_file_path = args
    wget.download(f'http://europepmc.org/ftp/oa/{todownload}', pubmed_dump_file_path)

if __name__ == '__main__':
    e = EuropePubMedCentralDataset('/mie/europepmc.org/ftp/oa')
    e.start()
