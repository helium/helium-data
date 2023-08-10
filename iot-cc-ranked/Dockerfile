FROM apache/spark-py:latest
#FROM python:3.11.4-slim-bookworm

WORKDIR /opt/spark/work-dir

USER root

RUN python3 -m pip install --upgrade pip

RUN pip install --no-cache-dir pyspark==3.4.1
RUN pip install --no-cache-dir delta-spark
RUN pip install --no-cache-dir h3
RUN pip install --no-cache-dir pyogrio
RUN pip install --no-cache-dir pycountry
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir pyarrow
RUN pip install --no-cache-dir fastparquet
RUN pip install --no-cache-dir boto3

ADD main.py .

CMD [ "/bin/python3", "./main.py" ]

