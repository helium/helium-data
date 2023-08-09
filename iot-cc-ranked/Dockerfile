FROM apache/spark-py:latest
#FROM python:3.11.4-slim-bookworm

WORKDIR /opt/spark/work-dir

#COPY . .

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

#RUN mkdir -p /root/.ivy2/jars

#RUN wget https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar
#RUN cp wildfly-openssl-1.0.7.Final.jar /root/.ivy2/jars
#RUN cp wildfly-openssl-1.0.7.Final.jar /opt/spark/jars
#RUN mv wildfly-openssl-1.0.7.Final.jar /usr/local/lib/python3.10/dist-packages/pyspark/jars/
#RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
#RUN cp aws-java-sdk-bundle-1.12.262.jar /root/.ivy2/jars
#RUN cp aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars
#RUN mv aws-java-sdk-bundle-1.12.262.jar /usr/local/lib/python3.10/dist-packages/pyspark/jars/
#RUN wget https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar
#RUN cp antlr4-runtime-4.9.3.jar /root/.ivy2/jars
#RUN cp antlr4-runtime-4.9.3.jar /opt/spark/jars
#RUN mv antlr4-runtime-4.9.3.jar /usr/local/lib/python3.10/dist-packages/pyspark/jars/

ADD main.py .

CMD [ "/bin/python3", "./main.py" ]

