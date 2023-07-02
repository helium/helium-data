# Community Jupyter

Helium will now publish all Oracle S3 data files in the Delta Lake format. Delta Lake and Spark are modern standards for accessing and querying massive data tables. Delta Lake is easy to use after you've installed the right tools. With Spark, Jupyter and Delta Lake you can efficiently query Helium data using standard SQL commands.

We hope the community enjoys these new data tools and look forward to future community contributions.

The following Jupyter notebook shows how to access Helium's S3 files and SQL query the S3 files using Spark DataFrames.

## Prerquisites

Install Docker

https://docs.docker.com/get-docker/

To launch Jupyter

```
git clone --branch opendata https://github.com/helium/helium-data.git
cd ./helium-data/opendata/jupyter-community
docker run -it -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/all-spark-notebook
```

## Open Jupyter in Browser

In the Docker logs look for a line with the following pattern
http://127.0.0.1:8888/lab?token=<my_token>
Copy&paste the URL from the Docker log and open from your Browser.

## References
Delta Lake
https://delta.io/
https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/?expand_article=1
https://confluence.technologynursery.org/display/TDA/Jupyter+Notebook+for+Playing+with+Spark%2C+Python%2C+R%2C+Scala
https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook
Spark Examples
https://nbviewer.org/github/almond-sh/examples/blob/master/notebooks/spark.ipynb
