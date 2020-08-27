
from ETL import get_files_in_dir, read_json_file, extract_string_from_metadata
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

def create_dataset(inpath='/mie/scompatt', num_of_queries = 1000, starting_from = 0, ending_to = 2000, samples_from_each_json = 1):

    print("Creating dataset...")
    file_list = get_files_in_dir(inpath)[starting_from:ending_to]
    file_list = random.sample(file_list, num_of_queries)

    dois = []
    query_strings = []

    # For each file in the crossref compressed dump
    for f in tqdm(file_list):

        # Read it as a json object
        content=read_json_file(join(inpath, f))

        # Sample a number of document from the json
        elements = random.sample(content['items'], samples_from_each_json)

        # Append extracted data
        for element in elements:
            dois.append(element['DOI'].lower())
            query_strings.append(extract_string_from_metadata(element).replace('"','\\"'))

    df = pd.DataFrame({'doi': dois, 'query_be': query_strings})
    df.to_csv('benchmark/benchmark_queries.csv', index=False, encoding='utf-8', sep=';')


def run_benchmark(filename='benchmark/benchmark_queries.csv', print_results=False):
    df = shuffle(pd.read_csv(filename, sep=';'))
    solr = pysolr.Solr('http://localhost:8983/solr/crossref', always_commit=False, timeout=100)

    print("Running benchmark")

    start = time.time()
    for _, row in df.iterrows():
        results = solr.search('id:"{}"'.format(row['doi']))
        if len(results) != 1:
            print("Error with {}".format(row['doi']))

    end = time.time()
    print("Total time for DOI queries: {:.3f}s".format((end-start)))
    print("Mean time elapsed for a DOI query: {:.3f}s".format((end-start)/1000))

    start = time.time()
    for _, row in df.iterrows():
        results = solr.search(fl='*,score', q='bibref:"{}"'.format(row['query_be']))

        if len(results) < 1:
            print("[len = {}] Error with {}".format(len(results),row['query_be']))
            continue

        elif print_results:
                if len(results) > 1:
                    print("n_results: {}".format(len(results)))
                    print(row['query_be'])
                    for r in results:
                        print(r)
                    print("\n\n")

        r_dois = [r['id'] for r in results]
        if row['doi'] not in r_dois:
            print("Results don't contain proper doi.")

    end = time.time()
    print("Total time for bibref queries: {:.3f}s".format((end-start)))
    print("Mean time elapsed for a bibref query: {:.3f}s".format((end-start)/1000))

def crossref_query(filename='benchmark/benchmark_queries.csv'):
    df = shuffle(pd.read_csv(filename, sep=';'))

    retrieved_dois = []
    scores = []

    print("Starting Crossref benchmark...")
    start = time.time()

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi, be = row["doi"], row["query_be"]
        response = requests.get("https://api.crossref.org/works?query.bibliographic="+quote(be))
        records = response.json()
        if records["status"] == 'ok':
            best_match_doi , score = records["message"]["items"][0]["DOI"] , records["message"]["items"][0]["score"]

        retrieved_dois.append(best_match_doi)
        scores.append(score)
    end = time.time()

    df_crossref = pd.DataFrame({'known_doi': df['doi'], 'retrieved_doi': retrieved_dois, 'score': scores, 'query_be':df['query_be']})
    df_crossref.to_csv('benchmark/benchmark_queries_results_crossref.csv', index=False, encoding='utf-8', sep=';')
    print("Time elapsed for queries: {:.3f}s".format((end-start)))

def run_benchmark_2(filename='benchmark/benchmark_queries_from_ccc.csv'):
    df = shuffle(pd.read_csv(filename, sep=','))
    solr = pysolr.Solr('http://localhost:8983/solr/crossref_without_metadata', always_commit=False, timeout=100)

    accuracy = 0

    print("Running benchmark")

    start = time.time()
    for _, row in df.iterrows():


        str = row['query_be']
        #str = re.sub(r'^https?:\/\/.*[\r\n]*', '', str)
        str = ' '.join(item for item in str.split() if not (item.startswith('https://') and len(item) > 7))
        str = ' '.join(item for item in str.split() if not (item.startswith('http://') and len(item) > 7))
        str = str.replace("et al.", "")
        str = str.replace("Available at:", "")
        str = re.sub('\W+', ' ', str)
        str = str.strip()

        query = 'bibref:({})'.format(str)
        results = solr.search(fl='*,score', q=query)

        if len(results) < 1:
            print("[len = {}] Error with {}".format(len(results),query))
            continue

        r_dois = [r['id'] for r in results]
        if row['retrieved_doi'] not in r_dois:
            print("Results don't contain proper doi.")
            print("Query: {}".format(str))
            print("Query orig: {}".format(row['query_be']))
            r = "Results: \n"
            r = '\n'.join([x['bibref'][0] for x in results])
            print(r+"\n\n")
        else:
            accuracy += 1

    end = time.time()

    print("Accuracy: {}".format((accuracy)*100/len(df.index)))

    print("Total time for bibref queries: {:.3f}s".format((end-start)))
    print("Mean time elapsed for a bibref query: {:.3f}s".format((end-start)/1000))


if __name__ == '__main__':
    #create_dataset()
    run_benchmark_2()
    #crossref_query()

