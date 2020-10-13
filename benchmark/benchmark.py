from tqdm import tqdm
from os.path import join
import random
import pandas as pd
from sklearn.utils import shuffle
import time
import pysolr
import re
import requests
from urllib.parse import quote
import json
import xmltodict
import ast
def crossref_query_bibref(filename='benchmark/benchmark_queries_from_ccc.csv', s=","):
    df = shuffle(pd.read_csv(filename, sep=s))

    print("Starting Crossref benchmark...")
    start = time.time()

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi, be = row["retrieved_doi"], row["query_be"]
        response = requests.get("https://api.crossref.org/works?query.bibliographic=" + quote(be), timeout=100)
    end = time.time()
    print("Time elapsed for queries Bibref: {:.3f}s".format((end - start)))
    print("Mean time query Bibref: {:.5f}".format((end - start) / 200))


def crossref_query_doi(filename='benchmark/benchmark_queries_from_ccc.csv', s=","):
    df = shuffle(pd.read_csv(filename, sep=s))

    print("Starting Crossref benchmark...")
    start = time.time()

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi, be = row["retrieved_doi"], row["query_be"]
        response = requests.get("https://api.crossref.org/works/" + doi, timeout=100)
    end = time.time()
    print("Time elapsed for queries DOI: {:.3f}s".format((end - start)))
    print("Mean time query DOI: {:.5f}".format((end - start) / 200))


def run_benchmark_localcrossref_bibref(filename='benchmark/benchmark_queries_from_ccc.csv'):
    df = shuffle(pd.read_csv(filename, sep=','))
    solr = pysolr.Solr('http://localhost:8983/solr/crossref_without_metadata', always_commit=False, timeout=100)

    accuracy = 0

    print("Running benchmark")

    time_string = 0
    time_query = 0
    time_filtering = 0
    start = time.time()
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):

        time_string_start = time.time()
        str = row['query_be']
        str = ' '.join(item for item in str.split() if not (item.startswith('https://') and len(item) > 7))
        str = ' '.join(item for item in str.split() if not (item.startswith('http://') and len(item) > 7))
        str = str.replace("et al.", "")
        str = str.replace("Available at:", "")
        str = re.sub('\W+', ' ', str)
        str = str.strip()
        time_string_end = time.time()
        time_string += time_string_end-time_string_start

        time_query_start = time.time()
        query = 'bibref:({})'.format(str)
        results = solr.search(fl='*,score', q=query)
        time_query_end = time.time()
        time_query += time_query_end-time_query_start

        if len(results) < 1:
            #print("[len = {}] Error with {}".format(len(results),query))
            continue

        time_filtering_start = time.time()
        r_dois = [r['id'] for r in results]
        if row['retrieved_doi'] not in r_dois:
            print("Results don't contain proper doi.")
            print("Query: {}".format(str))
            print("Query orig: {}".format(row['query_be']))
            print("Results: ")
            r = '\n'.join([x['bibref'][0] for x in results])
            print(r+"\n\n")
        else:
            accuracy += 1
        time_filtering_end = time.time()
        time_filtering += time_filtering_end-time_filtering_start

    end = time.time()

    print("Accuracy: {}".format((accuracy)*100/len(df.index)))
    print("Total time for bibref queries: {:.3f}s".format((end-start)))
    print("Mean time elapsed for a bibref query: {:.3f}s".format((end-start)/200))
    print("Mean time string: {:.5f}s".format(time_string/200))
    print("Mean time query: {:.5f}s".format(time_query/200))
    print("Mean time filtering: {:.5f}s".format(time_filtering/200))

def run_benchmark_localcrossref_doi(filename='benchmark/benchmark_queries_from_ccc.csv'):
    df = shuffle(pd.read_csv(filename, sep=','))
    solr = pysolr.Solr('http://localhost:8983/solr/crossref_without_metadata', always_commit=False, timeout=100)

    accuracy = 0

    print("Running benchmark")

    time_string = 0
    time_query = 0
    time_filtering = 0
    start = time.time()
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi = row['retrieved_doi']
        time_query_start = time.time()
        query = 'id:"{}"'.format(doi)
        results = solr.search(fl='*,score', q=query)
        time_query_end = time.time()
        time_query += time_query_end-time_query_start

        if len(results) != 1:
            #print("[len = {}] Error with {}".format(len(results),query))
            continue

        time_filtering_start = time.time()
        r_doi = [r['id'] for r in results][0]
        if row['retrieved_doi'] != r_doi:
            print("Don't contain proper doi.")
            print("Query: {}".format(str))
            print("Result: {} ".format(r_doi))
        else:
            accuracy += 1
        time_filtering_end = time.time()
        time_filtering += time_filtering_end-time_filtering_start

    end = time.time()

    print("Accuracy: {}".format((accuracy)*100/len(df.index)))
    print("Total time for DOI queries: {:.3f}s".format((end-start)))
    print("Mean time elapsed for a DOI query: {:.3f}s".format((end-start)/200))
    print("Mean time string: {:.5f}s".format(time_string/200))
    print("Mean time query: {:.5f}s".format(time_query/200))
    print("Mean time filtering: {:.5f}s".format(time_filtering/200))

def dict_get(d, key_list): # Took from CCC
    if key_list:
        if type(d) is dict:
            k = key_list[0]
            if k in d:
                return dict_get(d[k], key_list[1:])
            else:
                return None
        elif type(d) is list:
            result = []
            for item in d:
                value = [dict_get(item, key_list)]
                if value is not None:
                    result += value
            return result
        else:
            return None
    else:
        return d


def create_dataset_orcid():
    headers = {"User-Agent": "SPACIN / CrossrefProcessor (via OpenCitations - http://opencitations.net; "
                                         "mailto:contact@opencitations.net)"}
    filename = 'benchmark/benchmark_dois_orcid_accuracy.csv'
    df = pd.read_csv(filename, sep=';')
    __api_url = "https://pub.orcid.org/v2.1/search?q="
    sec_to_wait = 10,
    max_iteration = 6,
    timeout = 30,
    orcids = []
    iter = 0

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi_string = row['known_doi']
        cur_query = "doi-self:\"%s\"" % doi_string.lower()
        """
        doi_string_l = doi_string.lower()
        doi_string_u = doi_string.upper()
        if doi_string_l != doi_string or doi_string_u != doi_string:
            cur_query = "(" + cur_query
            if doi_string_l != doi_string:
                cur_query += " OR doi-self:\"%s\"" % doi_string_l
            if doi_string_u != doi_string:
                cur_query += " OR doi-self:\"%s\"" % doi_string_u
            cur_query += ")"
        """
        response = requests.get("https://pub.orcid.org/v3.0/search?q=" + quote(cur_query), headers={"Accept": "application/json"}, timeout=100)
        if response.status_code != 200:
            retry = 0
            while response.status_code != 200 and retry < 3:
                response = requests.get("https://pub.orcid.org/v3.0/search?q=" + quote(cur_query), timeout=100)
                retry += 1
            if retry == 3:
                print("Cant find {}".format(doi_string))
                orcids.append(json.dumps(['']))

        else:
            resulting_orcid = ['']
            results = json.loads(response.text)['result']
            for orcid in results:
                resulting_orcid.append(orcid['orcid-identifier']['path'])
            orcids.append(json.dumps(resulting_orcid))
        iter +=1
        print(iter, " ", len(orcids))

    df2 = pd.DataFrame({'doi' : df['known_doi'], 'results_from_orcid': orcids})
    df2.to_csv('benchmark/benchmark_dois_orcid_accuracy.csv', index=False)

def run_benchmark_orcid_query_doi(filename='benchmark/benchmark_dois_orcid_accuracy.csv'):
    df = shuffle(pd.read_csv(filename, sep=';'))
    solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=False, timeout=100)
    print("Running benchmark")
    not_found = 0
    start = time.time()
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi = row[0]
        should_be = row[2]
        should_be = should_be.replace("[", "").replace("]", "").replace(" ", "")
        should_be = should_be.split(",")
        if should_be[0] == "":
            should_be = []

        #should_be = json.loads(should_be)

        query = 'id:"{}"'.format(doi)
        results = solr.search(fl='*,score', q=query)

        if len(results) > 0:
            #authors = [r['authors'] for r in results]
            authors_found = json.loads([r['authors'] for r in results][0])
            orcid_found = [a['orcid'] for a in authors_found]
            #print(orcid_found, "\n", should_be, "\n\n")
        else:
            orcid_found = []

        for a in should_be:
            #print(a)
            if a.strip() not in orcid_found:
                not_found +=1
                print("doi: ", doi, "should be: ", should_be, " but found: ", orcid_found )
                break

    accuracy = (len(df)-not_found)*100/len(df)
    print(accuracy)
    end = time.time()
    print("Accuracy: {}".format(accuracy))
    print("Total time for DOI queries (ORCID): {:.3f}s".format((end-start)))
    print("Mean time elapsed for a DOI query (ORCID): {:.3f}s".format((end-start)/200))

def orcid_query_doi(filename='benchmark/benchmark_dois_orcid.csv', s=","):
    df = shuffle(pd.read_csv(filename, sep=s))

    print("Starting ORCID benchmark...")
    start = time.time()

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi_string = row["doi"]
        cur_query = "doi-self:\"%s\"" % doi_string
        doi_string_l = doi_string.lower()
        doi_string_u = doi_string.upper()
        if doi_string_l != doi_string or doi_string_u != doi_string:
            cur_query = "(" + cur_query
            if doi_string_l != doi_string:
                cur_query += " OR doi-self:" + doi_string_l
            if doi_string_u != doi_string:
                cur_query += " OR doi-self:" + doi_string_u
            cur_query += ")"
        response = requests.get("https://pub.orcid.org/v2.1/search?q=" + quote(cur_query), timeout=100)
        if response.status_code != 200:
            retry = 0
            while response.status_code != 200:
                response = requests.get("https://pub.orcid.org/v2.1/search?q=" + quote(cur_query), timeout=100)
                retry += 1

        else:
            continue


    end = time.time()
    print("Time elapsed for queries DOI: {:.3f}s".format((end - start)))
    print("Mean time query DOI: {:.5f}".format((end - start) / 200))

if __name__ == '__main__':
    #crossref_query_bibref()
    #crossref_query_doi()
    #orcid_query_doi()
    run_benchmark_orcid_query_doi()
    #run_benchmark_localcrossref_bibref()
    #run_benchmark_localcrossref_doi()
    #create_dataset_orcid()

