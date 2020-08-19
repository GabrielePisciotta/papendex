# Papendex

Actually this project handles the indexing of Crossref dump.

1) Run `expander.py`. It takes each json dump in a folder where has been decompressed. 

2) Run `linearizer.py`. It extracts a string from each structured document.

Every produced output is compressed and saved. 