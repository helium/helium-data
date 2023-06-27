# Building

First, clone the spark repo.

```
./dev/change-scala-version.sh 2.13

./bin/docker-image-tool.sh -r public.ecr.aws/k0m1p4t7 -t v3.4.0 build

./bin/docker-image-tool.sh -r public.ecr.aws/k0m1p4t7 -t v3.4.0 push

./pull-jars
 docker build . -t public.ecr.aws/k0m1p4t7/spark:v3.4.0-aws && docker push public.ecr.aws/k0m1p4t7/spark:v3.4.0-aws

```