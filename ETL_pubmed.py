# Exploit a producer-consumer architecture in order to
# parallel process all the files and then upload them
from os import listdir, system, remove
from os.path import isfile, join
import re
import gzip
import multiprocessing
from urllib.parse import unquote
import xmltodict, json
from lxml import etree
import pandas as pd
import tqdm
import time
import gc
from contextlib import closing
import httplib2
from bs4 import BeautifulSoup, SoupStrainer
import wget
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool

__author__ = "Gabriele Pisciotta"
pubmed_file_path = '/mie/europepmc.org/ftp/oa'
articles_path = join(pubmed_file_path, 'articles')
csv_file_path = join(pubmed_file_path, 'csv')

def concatenate_datasets(path):

    present_files = get_files_in_dir(path)
    header_saved = False

    with open(join(path, 'dataset.csv'), 'w') as fout:
        for f in tqdm.tqdm(present_files):
            with open(join(path, f)) as fin:
                header = next(fin)
                if not header_saved:
                    fout.write(header)
                    header_saved = True
                for line in fin:
                    fout.write(line)

    df = pd.read_csv(join(path, 'dataset.csv'), sep='\t')
    df.drop_duplicates(inplace=True)
    df.to_csv(join(path, 'dataset.csv'), sep='\t')


def get_links_from_pubmed():
    links = []
    http = httplib2.Http()
    status, response = http.request('http://europepmc.org/ftp/oa/')

    for link in BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('a')):
        if link.has_attr('href'):
            if "xml.gz" in link['href']:
                links.append(link['href'])
    return links

def worker_download_links(todownload):
    wget.download(f'http://europepmc.org/ftp/oa/{todownload}', pubmed_file_path)


def normalise_doi(doi_string, include_prefix=False): # taken from https://github.com/opencitations/index/blob/master/identifier/doimanager.py
    if doi_string is not None:
        try:
            doi_string = re.sub("\0+", "", re.sub("\s+", "", unquote(doi_string[doi_string.index("10."):])))
            return doi_string.lower().strip()
        except ValueError:
            return None
    else:
        return None

# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = (f for f in listdir(path) if isfile(join(path, f)))
    #list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files


def create_entry_xml(xml_ref): # Taken from CCC
    entry_string = ""
    cur_el = None
    el = None

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

    entry_string = " ".join(entry_string.split())
    entry_string = re.sub(" ([,\.!\?;:])", "\\1", entry_string)
    entry_string = re.sub("([\-–––]) ", "\\1", entry_string)
    entry_string = re.sub("[\-–––,\.!\?;:] ?([\-–––,\.!\?;:])", "\\1", entry_string)
    entry_string = re.sub("(\(\. ?)+", "(", entry_string)
    entry_string = re.sub("(\( +)", "(", entry_string)

    del el_citation
    del cur_el
    del el

    if entry_string is not None and entry_string != "":
        return entry_string
    else:
        return None

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
            toret=str(id_string)
            del id_string
            return toret

def worker_article(f):
    # Use the extracted file
    with open(join(articles_path, f), 'r') as fi:
        cur_xml = etree.parse(fi)

        cur_doi = normalise_doi(get_id_from_xml_source(cur_xml, 'doi'))
        cur_pmid = get_id_from_xml_source(cur_xml, 'pmid')
        cur_pmcid = get_id_from_xml_source(cur_xml, 'pmcid')

        if cur_pmid is not None:
            cur_id = cur_pmid  # Actually we don't have this value
        elif cur_pmcid is not None:
            cur_id = cur_pmcid
        elif cur_doi is not None:
            cur_id = normalise_doi(cur_doi)
        else:
            cur_id = None

        if cur_pmcid is not None:
            cur_pmcid = cur_pmcid.replace("PMC","")

        references = cur_xml.xpath(".//ref-list/ref")
        references_list = []

        if len(references):
            for reference in references:
                entry_text = "" # temporary disabled for memory issues -> create_entry_xml(reference)
                ref_pmid = None
                ref_doi = None
                ref_pmcid = None
                ref_url = None
                ref_xmlid = None

                ref_pmid_el = reference.xpath(".//pub-id[@pub-id-type='pmid']")
                if len(ref_pmid_el):
                    ref_pmid = etree.tostring(
                        ref_pmid_el[0], method="text", encoding='unicode').strip()

                ref_doi_el = reference.xpath(".//pub-id[@pub-id-type='doi']")
                if len(ref_doi_el):
                    ref_doi = normalise_doi(etree.tostring(
                        ref_doi_el[0], method="text", encoding='unicode').lower().strip())
                    if ref_doi == "":
                        ref_doi = None

                ref_pmcid_el = reference.xpath(".//pub-id[@pub-id-type='pmcid']")
                if len(ref_pmcid_el):
                    ref_pmcid = etree.tostring(
                        ref_pmcid_el[0], method="text", encoding='unicode').strip()
                    if ref_pmcid == "":
                        ref_pmcid = None
                    elif ref_pmcid.startswith("PMC"):
                        ref_pmcid = ref_pmcid.replace("PMC", "")

                ref_url_el = reference.xpath(".//ext-link")
                if len(ref_url_el):
                    ref_url = etree.tostring(
                        ref_url_el[0], method="text", encoding='unicode').strip()
                    if not ref_url.startswith("http"):
                        ref_url = None

                if ref_xmlid is None:
                    ref_xmlid_attr = reference.get('id')
                    if len(ref_xmlid_attr):
                        ref_xmlid = ref_xmlid_attr
                        if ref_xmlid == "":
                            ref_xmlid = None

                # Create an object to store the reference
                references_list.append({
                    #"entry_text": entry_text,
                    "ref_pmid": ref_pmid,
                    "ref_doi": ref_doi,
                    "ref_pmcid": ref_pmcid,
                    "ref_url": ref_url,
                    "ref_xmlid": ref_xmlid,
                })

        df = pd.DataFrame({'cur_doi': [cur_doi],
                      'cur_pmid': [cur_pmid],
                      'cur_pmcid': [cur_pmcid],
                      'cur_name': [f],
                      'references': json.dumps(references_list)
                       })

        df.to_csv(join(csv_file_path, f"{f}.csv"),sep='\t', index=False)



def worker_unzip_files(f):

    # Unzip
    system(f"gunzip {join(pubmed_file_path, f)}")

    # This is the new filename
    f = f.replace(".gz", "")

    # Create one file for each article, having its named
    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(join(pubmed_file_path, f), parser)

    # Extract all the article nodes
    articles = tree.findall('article')

    for cur_xml in articles:
        cur_pmid = get_id_from_xml_source(cur_xml, 'pmid')
        cur_pmcid = get_id_from_xml_source(cur_xml, 'pmcid').replace("PMC","")
        with open(join(articles_path,f"art_PMID{cur_pmid}_PMCID{cur_pmcid}.xml"), 'w') as writefile:
            writefile.write( etree.tostring(cur_xml, pretty_print=True, encoding='unicode') )

    remove(join(pubmed_file_path, f))


def run_in_thread(fn):
    def run(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw)
        t.start()

    return run

#@run_in_thread
def handle_references(args):
    paper, df = args
    references = json.loads(paper['references'])
    if len(references):
        newreferences = []
        for reference in references:
            try:
                obj = {}
                # TODO: uncomment these
                # if reference['entry_text'] is not None and reference['entry_text'] != "":
                #    obj['entry_text'] = reference['entry_text']

                if reference["ref_pmid"] is not None and reference["ref_pmid"] != 0:
                    print("Inserito pmid")
                    ref_pmid = reference["ref_pmid"]
                    obj['ref_pmid'] = ref_pmid

                else:
                    ref_pmid = ""

                if reference["ref_pmcid"] is not None and reference["ref_pmcid"] != 0:
                    print("Inserito pmcid")

                    ref_pmcid = reference["ref_pmcid"]
                    obj['ref_pmcid'] = ref_pmcid

                else:
                    ref_pmcid = ""

                if reference["ref_doi"] is not None and reference["ref_doi"] != "":
                    ref_doi = reference["ref_doi"]
                    obj['ref_doi'] = ref_doi

                else:
                    ref_doi = ""

                ref_url = reference["ref_url"]
                ref_xmlid = reference["ref_xmlid"]
                # Make a kind of "join" between the reference and the dataset
                ref_paper_joined = None
                if ref_pmid != "":
                    ref_paper_joined = df[df['cur_pmid'] == int(ref_pmid)]
                """if ref_paper_joined is None and ref_pmcid != "":
                    ref_paper_joined = df[df['cur_pmcid'] == int(ref_pmcid)]
                if ref_paper_joined is None and ref_doi is not None and ref_doi != "":
                    ref_paper_joined = df[df['cur_doi'] == ref_doi]"""

                # Refill missing values from the joined entity
                if ref_paper_joined is not None:
                    print("Ho fatto il join")
                    if ref_pmid == "" and len(ref_paper_joined['cur_pmid']) and ref_paper_joined["cur_pmid"].values[
                        0] is not None and int(ref_paper_joined["cur_pmid"].values[0]) != 0:
                        ref_pmid = int(ref_paper_joined["cur_pmid"].values[0])
                        obj['ref_pmid'] = ref_pmid

                    if ref_pmcid == "" and len(ref_paper_joined['cur_pmcid']) and ref_paper_joined["cur_pmcid"].values[
                        0] is not None and int(ref_paper_joined["cur_pmcid"].values[0]) != 0:
                        ref_pmcid = int(ref_paper_joined["cur_pmcid"].values[0])
                        obj['ref_pmcid'] = ref_pmcid

                    if ref_doi == "" and len(ref_paper_joined['cur_doi']) and ref_paper_joined["cur_doi"].values[
                        0] is not None and ref_paper_joined["cur_doi"].values[0] != "":
                        ref_doi = ref_paper_joined["cur_doi"].values[0]
                        obj['ref_doi'] = ref_doi

                if ref_url is not None and ref_url.startswith("http"):
                    obj['ref_url'] = ref_url

                if ref_xmlid is not None:
                    obj['ref_xmlid'] = ref_xmlid

                # Append the new reference object to the reference list
                newreferences.append(obj)
            except Exception as e:
                print(f"Exception {e.with_traceback()} with paper {paper['cur_name']}, reference {reference}")
                pass

            paper['references'] = json.dumps(newreferences)

            new_df = pd.DataFrame({
                'cur_doi': [paper['cur_doi']],
                'cur_pmid': [paper['cur_pmid']],
                'cur_pmcid': [paper['cur_pmcid']],
                'cur_name': [paper['cur_name']],
                'references': [paper['references']]
            })

            new_df.to_csv(join(csv_file_path, 'joined', f'{str(paper["cur_name"])}.csv'), sep='\t', index=False)


def join_dataset():
    df = pd.read_csv(open(join(csv_file_path, 'dataset.csv')), sep='\t')
    df['cur_pmid'] = df['cur_pmid'].fillna(0)
    df['cur_pmcid'] = df['cur_pmcid'].fillna(0)

    with ThreadPool(processes=200) as pool:
        list(tqdm.tqdm(pool.imap(handle_references, ((paper, df) for _, paper in df.iterrows())), total=len(df)))


def ETL_pubmed():

    """
    # for each file from the pubmed dump
    f = get_files_in_dir(pubmed_file_path)

    # get the difference between files to download and files that we have
    links = get_links_from_pubmed()
    todownload = set(links).difference(set(f))

    if len(todownload):
        print(f"Downloading {len(todownload)} files")
        with closing(multiprocessing.Pool(8)) as pool:
            list(tqdm.tqdm(pool.imap(worker_download_links, todownload), total=len(todownload)))

    # Update the file list
    f = get_files_in_dir(pubmed_file_path)
    s = time.time()
    """

    # Download articles' IDs
    wget.download(f'ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz', pubmed_file_path)
    articleids = pd.read_csv(join(pubmed_file_path,'PMC-ids.csv.gz'))
    """
    # Unzip all the files
    print("Unzipping all the articles")
    s = time.time()
    with multiprocessing.Pool(1) as pool: # ~2h:30m
        list(tqdm.tqdm(pool.imap(worker_unzip_files, f), total=len(f)))

    e = time.time()
    print(f"Time: {(e-s)}")
    """
    # process each article
    """
    s = time.time()
    print("Processing all the articles")
    f = get_files_in_dir(articles_path)
    with multiprocessing.Pool(8) as pool:
        list(tqdm.tqdm(pool.imap(worker_article, f), total=1247090)) #~1h:45m
    e = time.time()
    print(f"Time: {(e-s)}")

    print("Concatenating dataset")
    s = time.time()
    concatenate_datasets(csv_file_path)
    e = time.time()
    print(f"Time: {(e-s)}")"""


    print("Joining informations between rows")
    s = time.time()
    join_dataset()
    e = time.time()
    print(f"Time: {(e-s)}")

    print("Concatenating joined dataset")
    s = time.time()
    concatenate_datasets(join(csv_file_path, 'joined'))
    e = time.time()

    print(f"Time: {(e - s)}")


ETL_pubmed()
