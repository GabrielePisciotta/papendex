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
import uuid

__author__ = "Gabriele Pisciotta"
pubmed_file_path = '/mie/europepmc.org/ftp/oa'
articles_path = join(pubmed_file_path, 'articles')
csv_file_path = join(pubmed_file_path, 'csv2')

def concatenate_datasets():
    li = []

    present_files = get_files_in_dir(csv_file_path)

    for filename in tqdm.tqdm(present_files):
        csv_file = join(csv_file_path, filename)
        df = pd.read_csv(csv_file, index_col=None, header=0, sep=";")
        li.append(df)
        remove(csv_file)


    df = pd.concat(li, axis=0, ignore_index=True)

    # delete duplicates
    df.drop_duplicates(keep=False, inplace=True)
    df.to_csv(join(csv_file_path, 'dataset.csv.gz'), compression='gzip', sep=';')


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
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
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
            cur_id = cur_doi
        else:
            cur_id = None
        cur_source = "MED"  # Actually we don't have this value

        cur_localid = f"{cur_source}-{cur_id}-{uuid.uuid4()}"

        references = cur_xml.xpath(".//ref-list/ref")
        references_list = []

        if len(references):
            for reference in references:
                entry_text = create_entry_xml(reference)
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
                    "entry_text": entry_text,
                    "ref_pmid": ref_pmid,
                    "ref_doi": ref_doi,
                    "ref_pmcid": ref_pmcid,
                    "ref_url": ref_url,
                    "ref_xmlid": ref_xmlid,
                })
        pd.DataFrame({'cur_doi': cur_doi,
                      'cur_pmid': cur_pmid,
                      'cur_pmcid': cur_pmcid,
                      'cur_id': cur_id,
                      'cur_source': cur_source,
                      'cur_name': f,
                      'references': references_list}).to_csv(join(csv_file_path,f"{f}.csv.gz"),
                                                             compression="gzip", sep=';')

def worker(f): # Partially taken from CCC
    # Unzip
    system(f"gunzip -k {join(pubmed_file_path, f)}")
    print(f"File {f}")
    # Use the extracted file
    f = f.replace(".gz", "")

    # for each article
    with open(join(pubmed_file_path, f), 'r') as input_file:

        # Parse the xml file
        tree = etree.parse(input_file)

        # Extract all the article nodes
        articles = tree.findall('article')
        processed_articles = []
        for cur_xml in articles:

            cur_doi = normalise_doi(get_id_from_xml_source(cur_xml, 'doi'))
            cur_pmid = get_id_from_xml_source(cur_xml, 'pmid')
            cur_pmcid = get_id_from_xml_source(cur_xml, 'pmcid')
            if cur_pmid is not None:
                cur_id = cur_pmid # Actually we don't have this value
            elif cur_pmcid is not None:
                cur_id = cur_pmcid
            elif cur_doi is not None:
                cur_id = cur_doi
            else:
                cur_id = None
            cur_source = "MED" # Actually we don't have this value

            cur_localid = f"{cur_source}-{cur_id}-{uuid.uuid4()}"

            references = cur_xml.xpath(".//ref-list/ref")
            references_list = []

            if len(references):
                for reference in references:
                    entry_text = create_entry_xml(reference)
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
                        "entry_text" : entry_text,
                        "ref_pmid": ref_pmid,
                        "ref_doi": ref_doi,
                        "ref_pmcid": ref_pmcid,
                        "ref_url": ref_url,
                        "ref_xmlid": ref_xmlid,
                    })
            processed_articles.append({'cur_doi':cur_doi, 'cur_pmid':cur_pmid, 'cur_pmcid':cur_pmcid, 'cur_id':cur_id, 'cur_source':cur_source, 'references':references_list})

    df = pd.DataFrame(processed_articles)
    df.to_csv(join(csv_file_path, f"{f}.csv.gz"), compression="gzip", sep=';')

    # Remove the extracted files
    remove(f"{join(pubmed_file_path, f)}")


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
        cur_pmcid = get_id_from_xml_source(cur_xml, 'pmcid')
        with open(join(articles_path,f"art_PMID{cur_pmid}_PMCID{cur_pmcid}.xml"), 'w') as writefile:
            writefile.write( etree.tostring(cur_xml, pretty_print=True, encoding='unicode') )

    remove(join(pubmed_file_path, f))


def ETL_pubmed():


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

    # Unzip all the files
    print("Unzipping all the articles")
    s = time.time()
    with multiprocessing.Pool(1) as pool: # ~2h:30m
        list(tqdm.tqdm(pool.imap(worker_unzip_files, f), total=len(f)))

    e = time.time()
    print(f"Time: {(e-s)}")

    # process each article
    s = time.time()
    print("Processing all the articles")
    f = get_files_in_dir(articles_path)
    with multiprocessing.Pool(8) as pool:
        list(tqdm.tqdm(pool.imap(worker_article, f), total=len(f)))
    e = time.time()
    print(f"Time: {(e-s)}")

    print("Concatenating dataset")
    s = time.time()

    concatenate_datasets()
    e = time.time()

    print(f"Time: {(e-s)}")
ETL_pubmed()
