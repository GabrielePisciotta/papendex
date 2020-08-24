# Papendex
This project handles the indexing of Crossref dump.
First of all you need Solr binaries. 
After having downloaded them, go in `solr/bin` and run 
- `./solr start`
- `./solr create -c papendex`
Now you should have a Solr instance running with a core named 'papendex', where everything 
will be stored. 

At this point you can run `extract_transform_load.py`, specifying the path of the Crossref 
compressed dump and the URL of the Solr instance running. This script will take each 
JSON set of docs, and for each of them it will extract a string representing it (eg: authors, title, doi...) and its DOI in lowercase. 
It will save in the Solr instance an object of the form:
```
{
'id': '10.1002/(sici)1097-0231(199705)11:8<875::aid-rcm934>3.0.co;2-k', 
'bibref': 'James L. Stephenson Scott A. McLuckey , Charge Reduction of Oligonucleotide Anions Via Gas-phase Electron Transfer to Xenon Cations, Rapid Commun. Mass Spectrom., 1997 5 , 10.1002/(sici)1097-0231(199705)11:8<875::aid-rcm934>3.0.co;2-k',
'original': '{"indexed": {"date-parts": [[2020, 3, 25]], "date-time": "2020-03-25T13:50:56Z", "timestamp": 1585144256746}, "reference-count": 0, "publisher": "Wiley", "issue": "8", "license": [{"URL": "http://doi.wiley.com/10.1002/tdm_license_1.1", [...]}'
}

``` 
where `id` is the lowercased DOI and `original` is the original JSON document in Crossref. More in details: id is the unique key field, bibref is a textual field that is also indexed,
original is a string field that is only stored.

Run `_extracted` if you need to add already extracted files, otherwise you can run `_compressed` in order to extract and load.


---

Other scripts for serialization (the Crossref dump must be already extracted in a dir):

1)  `expander.py`. It takes each json dump in a folder where has been extracted. 

2) Run `linearizer.py`. It extracts a string from each structured document.

Every produced output is compressed (gzip) and saved. 