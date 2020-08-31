from lxml import etree
from tqdm import tqdm
import json
import pysolr
import time
import gzip
from os import listdir
from os.path import isfile, join, basename, splitext
import re
import tarfile



# Get list of file inside the dir
def get_files_in_dir(path):
    list_of_files = [f for f in listdir(path) if isfile(join(path, f))]
    list_of_files.sort(key=lambda f: int(re.sub('\D', '', f)))
    return list_of_files

def get_author_list(solr, doi):
    query = solr.search(q='id:"{}"'.format(doi))
    author_list = [q['authors'] for q in query]
    if len(author_list) == 0:
        return  json.loads('[]')
    else:
        return json.loads(author_list[0])

try:
    solr = pysolr.Solr('http://localhost:8983/solr/orcid', always_commit=False, timeout=1000)
    solr.ping()
    print("Connection enstablished to Solr")

except:
    print("Can't enstablish a connection to Solr")
    exit()

parser = etree.XMLParser()
inpath = '/mie/orcid/000/'

file_list = get_files_in_dir(inpath)
exceptions = 0
orcids = 0

start = time.time()

for f in tqdm(file_list):
    try:
        tree = etree.parse(join(inpath, f), parser)
        root = tree.getroot()
        orcid = root.find('{http://www.orcid.org/ns/common}orcid-identifier')\
                    .find('{http://www.orcid.org/ns/common}path')\
                    .text

        name = root.find('{http://www.orcid.org/ns/person}person')\
                   .find('{http://www.orcid.org/ns/person}name')

        given_names = name.find('{http://www.orcid.org/ns/personal-details}given-names').text
        family_name = name.find('{http://www.orcid.org/ns/personal-details}family-name').text

        groups = root.find('{http://www.orcid.org/ns/activities}activities-summary')\
                     .find('{http://www.orcid.org/ns/activities}works')\
                     .findall('{http://www.orcid.org/ns/activities}group')

        orcids += 1

        dois = []
        for g in groups:
            dois.append(g.find('{http://www.orcid.org/ns/common}external-ids')\
                            .find('{http://www.orcid.org/ns/common}external-id')\
                            .find('{http://www.orcid.org/ns/common}external-id-value').text.lower())

        for doi in dois:
            #print(orcid, '\t', given_names, '\t', family_name, '\t', doi)

            # Get the author list object
            authors = get_author_list(solr, doi)

            # Check if author is not present
            if len(list(filter(lambda x:x["orcid"]==orcid,authors))) == 0:
                #print("ORCID {} not present in DOI {}".format(orcid, doi))
                authors.append({
                      'orcid': orcid,
                      'given_names': given_names,
                      'family_name': family_name
                })

                # Update it
                solr.add({
                    'id':doi,
                    'authors': json.dumps(authors)
                })

        solr.commit()
    except AttributeError:
        pass
    except Exception as ex:
        print(ex)
        pass
    end = time.time()



print("Processed {} files, ({} orcid) in {:.3f}s".format(len(file_list), orcids, (end-start)))