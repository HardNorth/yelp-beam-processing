# yelp-beam-processing
Yelp Dataset Big Data process and analysis with Apache Beam and Google Cloud integration

# Usage:
```
gradle execute \ 
    -Dexec.mainClass=net.hardnorth.yelp.ingest.bigquery.IngestBusiness \ 
    -Dexec.args="--project=<PROJECT_ID> --runner=org.apache.beam.runners.dataflow.DataflowRunner --stagingLocation=gs://yelp-dataset/staging/ --tempLocation=gs://yelp-dataset/temp/ --datasetId=yelp --tableName=business --dataSourceReference=gs://yelp-dataset/business.json"
```
