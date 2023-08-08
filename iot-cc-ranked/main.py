import h3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when

from delta import *
from delta.tables import DeltaTable

import shutil
import pandas as pd
import numpy as np


x1 = "631714807764183039"
x2 = 631714807764183039
h = '89283082837ffff'
geo = h3.h3_to_geo(hex(x2))

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
partition = "date = '2023-07-25'"
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

stats = joined.groupBy("country").sum("data_iot")
stats.show()

order = stats.orderBy(col("sum(data_iot)").desc())
order = order.withColumnRenamed('sum(data_iot)', 'sum')
order.show(15)

sum1 = order.select(sum("sum"))
sum1.show()
sum_total = sum1.head()[0]
print(f"total is {sum_total}")

def apply_percent_logic(df):
    return df.withColumn("percent",  df.sum * 100 / sum_total)

o2 = order.transform(apply_percent_logic)
o2.show(80)

print(geo)