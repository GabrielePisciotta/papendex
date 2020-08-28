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

def crossref_query(filename='benchmark/benchmark_queries_from_ccc.csv', s=","):
    df = shuffle(pd.read_csv(filename, sep=s))

    #retrieved_dois = []
    #scores = []

    print("Starting Crossref benchmark...")
    start = time.time()

    for _, row in tqdm(df.iterrows(), total=df.shape[0]):
        doi, be = row["known_doi"], row["query_be"]
        response = requests.get("https://api.crossref.org/works?query.bibliographic="+quote(be), timeout=20)
        #records = response.json()
        #if records["status"] == 'ok':
        #    best_match_doi , score = records["message"]["items"][0]["DOI"] , records["message"]["items"][0]["score"]

        #retrieved_dois.append(best_match_doi)
        #scores.append(score)
    end = time.time()

    #df_crossref = pd.DataFrame({'known_doi': df['doi'], 'retrieved_doi': retrieved_dois, 'score': scores, 'query_be':df['query_be']})
    #df_crossref.to_csv('benchmark/benchmark_queries_results_crossref.csv', index=False, encoding='utf-8', sep=';')
    print("Time elapsed for queries: {:.3f}s".format((end-start)))

def run_benchmark(filename='benchmark/benchmark_queries_from_ccc.csv'):
    df = shuffle(pd.read_csv(filename, sep=','))
    solr = pysolr.Solr('http://localhost:8983/solr/crossref_without_metadata', always_commit=False, timeout=100)

    accuracy = 0

    print("Running benchmark")

    start = time.time()
    for _, row in tqdm(df.iterrows(), total=df.shape[0]):

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
            print("Results: ")
            r = '\n'.join([x['bibref'][0] for x in results])
            print(r+"\n\n")
        else:
            accuracy += 1

    end = time.time()

    print("Accuracy: {}".format((accuracy)*100/len(df.index)))

    print("Total time for bibref queries: {:.3f}s".format((end-start)))
    print("Mean time elapsed for a bibref query: {:.3f}s".format((end-start)/1000))


if __name__ == '__main__':
    crossref_query(s=",")
    run_benchmark()

