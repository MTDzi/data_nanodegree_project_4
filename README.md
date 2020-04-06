## Introduction 

In this project (*Data Lake*) the purpose is to read from S3 a nested directory struture containing JSON files with song and log data, to transform them (mainly, columns related to datetimes), and to finally load them back to S3, as parquet directories.

All operations are being done in the `etl.py` script, meaning: using `pyspark`. Also, I'm only using the high-level Spark API, i.e. `DataFrame`s.


## Project setup

First, you'll need a `dl.cfg` file that looks like this
```txt
[AWS]
AWS_ACCESS_KEY=<PASTE_YOUR_ACCESS_KEY_WITHOUT_QUOTATION_MARKS>
AWS_SECRET_ACCESS_KEY=<ANALOGOUSLY_YOUR_SECRET_ACCESS_KEY>
```

Then, run the following command on an EMR cluster:
```
spark-submit --master yarn etl.py
```
or locally (the number "4" is arbitrary, choose the number of worker threads according to your capabilities):
```
spark-submit --master local[4] etl.py  
```
