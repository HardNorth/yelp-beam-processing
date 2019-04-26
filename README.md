# yelp-beam-processing
Yelp Dataset Big Data process and analysis with Apache Beam and Google Cloud integration

# Usage:
```
gradle execute \ 
    -Dexec.mainClass=net.hardnorth.yelp.ingest.bigquery.IngestBusiness \ 
    -Dexec.args="--project=mbt-ml --runner=org.apache.beam.runners.dataflow.DataflowRunner --stagingLocation=gs://vh-yelp-dataset/staging/ --tempLocation=gs://vh-yelp-dataset/temp/ --datasetId=yelp --tableName=business --dataSourceReference=gs://vh-yelp-dataset/business.json"
```
