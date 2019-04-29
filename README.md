# yelp-beam-processing
Yelp Dataset Big Data process and analysis with Apache Beam and Google Cloud integration

# Usage:

Export to BigQuery:
```shell
gradle execute \
    -Dexec.mainClass=net.hardnorth.yelp.ingest.bigquery.IngestBusiness \
    -Dexec.args="--project=<PROJECT_ID> --runner=org.apache.beam.runners.dataflow.DataflowRunner --stagingLocation=gs://yelp-dataset/staging/ --tempLocation=gs://yelp-dataset/temp/ --datasetId=yelp --tableName=business --dataSourceReference=gs://yelp-dataset/business.json --syncExecution=false"
```
Export to CSV:
```shell
gradle execute \
    -Dexec.mainClass=net.hardnorth.yelp.ingest.csv.IngestBusiness \
    -Dexec.args="--project=<PROJECT_ID> --runner=org.apache.beam.runners.dataflow.DataflowRunner --stagingLocation=gs://yelp-dataset/staging/ --tempLocation=gs://yelp-dataset/temp/ --dataSourceReference=gs://yelp-dataset/business.json --dataOutputReference=gs://yelp-dataset/business-processed.csv --syncExecution=false"

# Compose CSV after export
# Unfortunately leave CSV headers of composing files in the result file
gsutil compose \
    gs://yelp-dataset/business-processed.csv-* \
    gs://yelp-dataset/business-processed.csv
```
