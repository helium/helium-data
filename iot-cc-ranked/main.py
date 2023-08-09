import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import sum
from pyspark.sql.functions import lit


from delta import *
from delta.tables import DeltaTable

import shutil
import pandas as pd
import numpy as np

from datetime import datetime, timedelta


N_DAYS_AGO = 1

today = datetime.now()    
n_days_ago = today - timedelta(days=N_DAYS_AGO)
print(f"today is {today}")
print(f"24 hours past is {n_days_ago}")
yesterday = n_days_ago.strftime('%Y-%m-%d')
print(f"yesterday is {yesterday}")

builder = SparkSession.builder.appName("DeltaLakeApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.jars.packages","io.delta:delta-core_2.12:2.0.0")

my_packages = ["org.apache.hadoop:hadoop-aws:3.3.4"]
spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#spark.conf.set("spark.hadoop.fs.s3a.requester-pays.enabled", "true")
spark.conf.set("spark.hadoop.delta.enableFastS3AListFrom", "true")

# Spark 3.4.2 or higher is required
print(f"Spark version = {spark.version}")

# hadoop 3.3.6 or higher is required to support S3 requester pays
print(f"Hadoop version = {spark._sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

gateways_uri="s3a://foundation-data-lake-requester-pays/bronze/iot_gateways_geo/"
geos = spark.read.format("delta").load(gateways_uri)

rewards_uri="s3a://foundation-data-lake-requester-pays/silver/iot-reward-share/"
partition = f"date = '{yesterday}'"
rewards = spark.read.format("delta").load(rewards_uri).where(partition).select("date", "gateway", "dc_transfer_amount_iot")
rewards.show(10)
print(f"rewards rows count : {rewards.count()}")

def apply_logic(df: DataFrame) -> DataFrame:
    return df.withColumn("data_iot",  when(df.dc_transfer_amount_iot > 0, df.dc_transfer_amount_iot))

r2 = rewards.transform(apply_logic)
r3 = r2.drop("dc_transfer_amount_iot").dropna()

print(f"r3 rows count : {r3.count()}")
r3.show(10)

cond = [geos.gateway == r3.gateway]
joined = r3.join(geos, cond, 'inner').select(r3.date, r3.gateway, r3.data_iot, geos.country)
joined.show()

percentile999 = joined.agg(expr("percentile(data_iot, 0.999)")).head()[0]
print(f"99.9th percentile is {percentile999}")

def apply_outlier_logic(df: DataFrame) -> DataFrame:
    return df.withColumn("data_iot_2",  when(df.data_iot < percentile999, df.data_iot))

f2 = joined.transform(apply_outlier_logic)
f3 = f2.drop("data_iot").dropna()
joined = f3.withColumnRenamed('data_iot_2', 'data_iot')
joined.describe().show()

stats = joined.select("date", "country", "data_iot").groupBy("country").sum("data_iot")
stats = stats.withColumn("date", lit(yesterday))
stats.show()

order = stats.orderBy(col("sum(data_iot)").desc())
order = order.withColumnRenamed('sum(data_iot)', 'sum')
order.show(15)

sum_total = order.select(sum(order.sum).alias("the_sum")).head()[0]
print(f"sum_total is {sum_total}")

def apply_percent_logic(df):
    return df.withColumn("percent",  df.sum * 100 / sum_total)

o2 = order.transform(apply_percent_logic)
o2.show(40)

iot_ranked_uri="s3a://foundation-iot-metrics/iot-cc-ranked.parquet"

# o3 = spark.read.parquet(iot_ranked_uri)
# yesterday_date_rows = o3.filter(o3.date.contains(yesterday))
# yesterday_count = yesterday_date_rows.count()
# if (yesterday_count <= 0):
#     o4 = o3.union(o2);
# #    o4.write.mode("overwrite").parquet(iot_ranked_uri)
# else:
#     print(f"yesterday already written count is {yesterday_count}")
#     exit()

o4 = o2

import logging
import boto3
from botocore.exceptions import ClientError
import os

iot_ranked_uri="s3a://foundation-iot-metrics/iot-cc-trial.parquet"

o5 = o4.toPandas()
ranked_name = "iot-cc-ranked.parquet"
o5.to_parquet(ranked_name)

# Copied from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
def upload_file(file_name, bucket, object_name=None):

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

upload_file(ranked_name, "foundation-iot-metrics", ranked_name)


