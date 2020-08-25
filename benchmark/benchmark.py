
from extract_transform_load_from_extracted import get_files_in_dir, read_json_file, extract_string_from_metadata
from tqdm import tqdm
from os.path import join
import random
import pandas as pd
from sklearn.utils import shuffle
import time
import pysolr


# Extract 1000 query from the first 1000
def create_dataset(inpath='/mie/scompatt', starting_from = 0, ending_to = 1000, samples_from_each_json = 1):

    print("Creating dataset...")
    file_list = get_files_in_dir(inpath)[starting_from:ending_to]

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
    df.to_csv('benchmark_queries.csv', index=False, encoding='utf-8', sep=';')


def run_benchmark(type='doi', filename='benchmark_queries.csv', print_results=False):
    df = shuffle(pd.read_csv(filename, sep=';'))
    solr = pysolr.Solr('http://localhost:8983/solr/ccc', always_commit=True, timeout=100)

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

if __name__ == '__main__':
    #create_dataset()
    run_benchmark()


