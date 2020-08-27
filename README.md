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

### Benchmark
In order to assess the performances achieved using the local indexed Crossref data, use the 
`benchmark.py` script.


---
### References
[1] https://en.wikipedia.org/wiki/Extract,_transform,_load
 