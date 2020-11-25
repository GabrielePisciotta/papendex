# Papendex
This project handles the indexing of Crossref and ORCID dumps.

This is part of the [Open Biomedical Citations in Context Corpus](https://wellcome.ac.uk/grant-funding/people-and-projects/grants-awarded/open-biomedical-citations-context-corpus) research project, actually used for speed up OpenCitations SPAR Citation Indexer (SPACIN) process.

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

The ORCID dump is composed of many compressed tar.gz files. We're interested only in `ORCID_<year>_summaries.tar.gz`: get it from its website, and specify the reference to the file when you run the process.

The schema that we're using for the ORCID is the following:
```
"id":"10.1590/0102-4698186748",
"authors":"[{\"orcid\": \"0000-0003-1789-8243\", \"given_names\": \"Vinicius\", \"family_name\": \"Machado de Oliveira\"}]",
``` 
where:
 - `id` is a DOI (unique key)
 - `authors` is a stored field containing a list of authors, each object composed of an `orcid`, a `given_name` and a `family_name`.

### How to start ETL Crossref
First of all, be sure that Solr is up and running.
Then, chose if you want to work with an already extracted dump file or directly with the compressed dump. You'll have to specify some parameters in ETL_Crossref.py: 
- `source`: can be 'path' if you want to specify the extracted path or 'compressed' if you want to specify the compressed filename
- `path`: the path where we will work (e.g.: where the dump has been extracted or where the dump filename is contained)
- `dump_filename`: the file name of the dump
- `solr_address`: the address of the Solr server (if running in local, keep it as it is),

Run it with `$ python3 ETL_Crossref.py <parameters>` and have a break, it may be a long procedure.

### How to start ETL Orcid
As for the ETL Crossref, be sure that Solr is up and running.

You'll have to specify some parameters ETL_Orcid, setting the reference to the «SUMMARIES» dump downloaded from Orcid.

Run it with `$ python3 ETL_Orcid.py <parameters>` and have another big break. Better to run this overnight.


---
### References
[1] https://en.wikipedia.org/wiki/Extract,_transform,_load
 