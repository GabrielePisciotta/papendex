import pysolr
import time

solr = pysolr.Solr('http://localhost:8983/solr/ccc', always_commit=True, timeout=100)


# Example of query for a single DOI
start_query = time.time()
results = solr.search('bibref:"Twentieth Annual Report of the Ophthalmic Section, Ministry of the Interior, Egypt, 1932."')
end_query = time.time()
print("Time for query: {}".format((end_query-start_query)))

print("Saw {0} result(s).".format(len(results)))
for result in results:
    print("'{0}'.".format(result))
