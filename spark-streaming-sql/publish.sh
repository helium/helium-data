#!/bin/bash

sbt assembly

aws s3 cp --profile data-prod target/scala-2.13/spark-streaming-sql-assembly*.jar s3://foundation-data-lake-requester-pays/jars/
