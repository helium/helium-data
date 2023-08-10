# IoT-CC-Ranked

k8s cron job which will process S3 table data and generate a table of country-codes (CC) ranked by data transfer usage.
The table's data is published to the S3 Metrics bucket.
It is currently used by the IoT-CC-Ranked dashboard:
https://observablehq.com/@mv-workspace/iot-by-cc

The k8s cron job runs once daily and parses yesterday's data.

## Input

S3 Bucket

s3a://foundation-data-lake-requester-pays

Delta Lake Parquet Tables

* bronze/iot_gateways_geo/
* silver/iot-reward-share/

## Output

s3a://foundation-iot-metrics/iot-cc-ranked.parquet


