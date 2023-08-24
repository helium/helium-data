# Community Data

The Helium Foundation will soon publish all oracle S3 data files in the Delta Lake and Parquet format. Delta Lake, Parquet and Spark are modern standards for accessing and querying massive data tables. Delta Lake is easy to use after you've installed the right tools. With Spark, Jupyter and Delta Lake you can efficiently query Helium network data using standard SQL commands.

Using Parquet, the Helium Foundation will be able to provide data which is 30 to 50% smaller than the equivalent raw data in protobuf format.  Additionally Parquet data has integrated schemas which make parsing and querying the data much simpler.  Parquet is the data format of choice by data scientists.

We hope the community enjoys these new data tools and look forward to collaborating and integrating future community contributions.

The following Jupyter notebook shows how to access Helium Foundations's S3 files and SQL query the S3 files using Spark DataFrames.

Here is a [preview](https://github.com/helium/helium-data/blob/main/jupyter-community/deltalake.ipynb) of the Jupyter notebook.

## Prerequisites

### Python and SQL

Users should have at least a beginner level of experience with Python and SQL.  A good understanding of SQL is the most important skill.

### Amazon AWS

Helium Network data is streamed into Amazon AWS S3 data storage.  It is available to the public on a 'requestor pays' model. To access the S3 buckets you'll need an [Amazon AWS](https://aws.amazon.com/) account.

Helium currently publishes about 3TB of data per month.  At Amazon's standard egress rate of $0.09 per GB it will cost $270 to download a single month's worth of Helium data.  We caution users to download data with care to avoid large AWS bills.  Delta Lake allows users to query the data very efficiently and will only download necessary data.

> **Warning**
> Large queries carry the risk of high egress fees when the data is transfered outside of us-west-2.  
> As a matter of practice, it is recommended to keep the following in mind:
> * Specify a start date on all queries.
> * Repeated queries will incur additional cost.
> * Syncing the entire bucket is not advised (`aws s3 sync`).

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
git clone https://github.com/helium/helium-data.git
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

## Known Issues

The AWS x-amz-request-payer [Requester Pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ObjectsinRequesterPaysBuckets.html) REST headers required to access Helium's S3 requester-pays buckets are currently not working for Jupyter's Delta Lake library.  There is a [patch](https://issues.apache.org/jira/secure/attachment/12877218/HADOOP-14661.patch) which fixes the Spark [issue](https://issues.apache.org/jira/browse/HADOOP-14661) for Spark's 3.4.2 release.

We are awaiting a Jupyter container image update to use Spark 3.4.2 / Hadoop 3.3.6 which fixes the issue.
Spark's latest release is 3.4.1.  The ETA for release 3.4.2 is unknown.

[Jupyter Issue 1937](https://github.com/jupyter/docker-stacks/issues/1937)

In the meantime, as a work-around, users can copy the S3 files to their local file system.  This method is described in the 'Working with Local Files' notebook cell.

We caution that Helium's Delta Lake bucket contains nearly 4TB of data.  At current AWS S3 egress rates of $0.09 per GB it will cost $360 to sync this entire bucket to your local system.  However users can greatly reduce their expenses by filtering the data sync to specific date ranges.

Advanced use of 'AWS S3 sync' with filters is beyond the scope of this discussion.


## Local Development
### Install minio and create buckets
1. Install the local minio server and visit localhost:9000 in your browser
2. Create an `iot-ingest` and `delta` bucket
3. Put your gzip files in `iot-ingest` and you can now run the `protobuf-delta-lake-sink` tool

### Set up scala in vscode
1. Open the scala related subdirectory in a new vscode. E.g. open spark-streaming-sql in vscode
2. Install Metals vscode extension
3. Import build under build commands in the extension page
4. Run the project from the vscode run menu

## References

### Delta Lake

* https://delta.io/
* https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/?expand_article=1
* https://confluence.technologynursery.org/display/TDA/Jupyter+Notebook+for+Playing+with+Spark%2C+Python%2C+R%2C+Scala
* https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook

### Spark

* https://nbviewer.org/github/almond-sh/examples/blob/master/notebooks/spark.ipynb
