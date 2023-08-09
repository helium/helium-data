#!/bin/bash -x

aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/k0m1p4t7
docker build -t iot-cc-ranked .
docker tag iot-cc-ranked public.ecr.aws/k0m1p4t7/iot-cc-ranked:0.0.1
docker push public.ecr.aws/k0m1p4t7/iot-cc-ranked:0.0.1
