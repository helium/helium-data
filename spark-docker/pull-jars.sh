#!/bin/bash

set -e
cd jars
curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar --output hadoop-aws-3.3.1.jar
curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.874/aws-java-sdk-bundle-1.11.874.jar --output aws-java-sdk-bundle-1.11.874.jar 