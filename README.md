# Community Data

The Helium Foundation will soon publish all oracle S3 data files in the Delta Lake and Parquet format. Delta Lake, Parquet and Spark are modern standards for accessing and querying massive data tables. Delta Lake is easy to use after you've installed the right tools. With Spark, Jupyter and Delta Lake you can efficiently query Helium network data using standard SQL commands.

Using Parquet, the Helium Foundation will be able to provide data which is 30 to 50% smaller than the equivalent raw data in protobuf format.  Additionally Parquet data has integrated schemas which make parsing and querying the data much simpler.  Parquet is the data format of choice by data scientists.

We hope the community enjoys these new data tools and look forward to collaborating and integrating future community contributions.

The following Jupyter notebook shows how to access Helium Foundations's S3 files and SQL query the S3 files using Spark DataFrames.

## Prerequisites

### Python and SQL

Users should have at least a beginner level of experience with Python and SQL.  A good understanding of SQL is the most important skill.

### Amazon AWS

Helium Network data is streamed into Amazon AWS S3 data storage.  It is available to the public on a 'requestor pays' model. To access the S3 buckets you'll need an [Amazon AWS](https://aws.amazon.com/) account.

Helium currently publishes about 3TB of data per month.  At Amazon's standard egress rate of $0.09 per GB it will cost $270 to download a single month's worth of Helium data.  We caution users to download data with care to avoid large AWS bills.  Delta Lake allows users to query the data very efficiently and will only download necessary data.

### Docker

A Docker container image is a lightweight, standalone, executable package of software that includes everything needed to run an application: code, runtime, system tools, system libraries and settings. To install docker, follow the official instructions [here](https://docs.docker.com/get-docker/).

### Jupyter

The [Jupyter](https://jupyter.org/) Notebook is an open source web application that you can use to create and share documents that contain live code, equations, visualizations and SQL.  We'll use Jupyter to access Helium Network data.

You do not need to install Jupyter.  It is available in the Jupyter docker image.  But please read the Jupyter documentation.

### Delta Lake

[Delta Lake](https://delta.io/) is a modern storage framework designed to handle massive amounts of data and make that data available to compute engines such as  Spark where it becomes queryable via languages such as Python, SQL and Scala.

Your Jupyter notebook will use Spark and Delta Lake to access Helium's S3 data lake.  Delta Lake will need to be installed in the Jupyter notebook.  We recommend browsing Delta Lake's documenation to familiarize yourself with Delta Lake's features.

## Instructions

### AWS Account credentials

Login into your AWS account and retrieve your access_key, secret_access_key and session_token.  You'll use these credentials to access Helium's S3 data bucket.

### Launch Jupyter

Change into the jupyter-community folder and launch the Jupyter application.

```
git clone --branch opendata https://github.com/helium/helium-data.git
cd ./helium-data/jupyter-community
docker run -it -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/all-spark-notebook
```

### Open Jupyter in Browser

In the Docker logs look for a line with the following pattern

```
http://127.0.0.1:8888/lab?token=<my_token>
```

Copy & paste the URL from the Docker log and open from your Browser.

### Open the DeltaLake notebook

From within Jupyter find the Work folder.  Open it.

Within Work you'll find a notebaook named DeltaLake.ipynb.  Click and open the notebook.

You can now view individual Jupyter cells.  The Jupyter notebook requires some pre-configuration.  Open a 'Terminal' and follow the instructions in the Notebook to install delta-lake and create your aws credentials file.

### Run DeltaLake notebook cells.

Click the 'run' button to run individual notebook code snippets.


## References

### Delta Lake

* https://delta.io/
* https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/?expand_article=1
* https://confluence.technologynursery.org/display/TDA/Jupyter+Notebook+for+Playing+with+Spark%2C+Python%2C+R%2C+Scala
* https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook

### Spark

* https://nbviewer.org/github/almond-sh/examples/blob/master/notebooks/spark.ipynb
