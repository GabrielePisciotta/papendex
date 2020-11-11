# Papendex
This project handles the indexing of Crossref and ORCID dumps.
This is part of the [Open Biomedical Citations in Context Corpus](https://wellcome.ac.uk/grant-funding/people-and-projects/grants-awarded/open-biomedical-citations-context-corpus) research project.

### Solr
In order to speed up the search process we exploit [Solr](https://www.apache.org/dyn/closer.lua/lucene/solr/8.6.1/solr-8.6.1.tgz) as search platform. 

Download and extract it, then go in `solr/bin` and run 
- `$ ./solr start`
- `$ ./solr create -c crossref`
- `$ ./solr create -c orcid`

Now you should have a Solr instance running with two cores named `crossref` and `orcid`, where everything will be stored. 

At this point, copy each schema file contained in the `schemes` directory to `{SOLR PATH}/server/solr/<core name>/conf`,
renaming it to `managed-schema`.

### ETL: Extract Transform Load
This is the procedure of copying data from one or more sources into a destination system which represents the data 
differently from the source(s) or in a different context than the source(s) [1]. 

The Crossref dump is composed of 38096 json file, each one containing metadata about 3000 references.
The ETL script can handle both the compressed file or the path containing the extracted files.  

We need to index these docs, and in order to do that we create a bibref-like string
starting from the informations we have (eg: authors, title, short container title, issued, date, volume, issue, page and doi).
 
The script will create a series of objects having this schema:
```
{
'id': '10.1002/(sici)1097-0231(199705)11:8<875::aid-rcm934>3.0.co;2-k', 
'bibref': 'James L. Stephenson Scott A. McLuckey , Charge Reduction of Oligonucleotide Anions Via Gas-phase Electron Transfer to Xenon Cations, Rapid Commun. Mass Spectrom., 1997 5 , 10.1002/(sici)1097-0231(199705)11:8<875::aid-rcm934>3.0.co;2-k',
'original': '{"indexed": {"date-parts": [[2020, 3, 25]], "date-time": "2020-03-25T13:50:56Z", "timestamp": 1585144256746}, "reference-count": 0, "publisher": "Wiley", "issue": "8", "license": [{"URL": "http://doi.wiley.com/10.1002/tdm_license_1.1", [...]}'
}
``` 
where: 
- `id` is the unique key field and is the lowercased DOI, 
- `bibref` is the textual field that is indexed
- `original` is the original JSON document in Crossref, that will be returned with a query


At the end of the processing of each json file, the related objects are loaded in Solr.

The ORCID dump is composed of many compressed tar.gz files. We're interested only in `ORCID_<year>_summaries.tar.gz`. 
So, after having downloaded the complete dump from the website, the ETL for ORCID will automatically extract only that one.
The schema that we're using for the ORCID is the following:
```
"id":"10.1590/0102-4698186748",
"authors":"[{\"orcid\": \"0000-0003-1789-8243\", \"given_names\": \"Vinicius\", \"family_name\": \"Machado de Oliveira\"}]",
``` 
where `id` is a DOI and authors is a stored field containing a list of authors, each object composed of an `orcid`, a `given_name` and a `family_name`.

### Benchmark
In order to assess the performances achieved using the local indexed Crossref/ORCID data, use the 
`benchmark.py` script. For assess the Crossref performances it's better to use their tool.

### PubMed ETL 
This tool let us create a dataset from the EuropePMC OpenAccess dumps.
The workflow is divided in the following steps:
- download the dumps (skippable)
- download IDs file and generate a pickle dump of it to enable a fast search
- unzip the articles from each dump and store their xml separately, deleting in the end the original dump (concurrently-> specify the number)
- process the article. Each XML is transformed in a row of the dataset having the following fields:
    - `cur_doi`
    - `cur_pmid`
    - `cur_pmcid`
    - `cur_name` (the reference to the XML file needed for BEE/Jats2OC)
    - `references` (json dumped string containing a list of identifiers of the cited documents)
 
    If any of the previous IDs are not contained in the XML, we will exploit the PMID or PMCID to find the missing ones
    in the IDs file. 
    
    If a citing article or a cited one doesn't have any ID, we don't save it. If a citing article doesn't have cited 
    references, we don't save it.
    
    This process is run in parallel (-> specify the number). You can specify to store everything in a single dataset.csv (slow)
    or to store in many CSV files and then concatenate them (fast).
---
### References
[1] https://en.wikipedia.org/wiki/Extract,_transform,_load
 