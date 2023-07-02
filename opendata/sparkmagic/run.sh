#!/bin/bash -x

docker build -t jupyter-boto .
docker run -it -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter-boto
