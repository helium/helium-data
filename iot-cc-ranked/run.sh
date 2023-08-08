#!/bin/bash -x

docker run -it --env-file=awskeys.env iot-cc-ranked:latest
