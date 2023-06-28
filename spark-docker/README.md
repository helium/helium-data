# Building

First, clone the spark repo.

```
./dev/change-scala-version.sh 2.13

./build/mvn -Pscala-2.13 -Pkubernetes -DskipTests -Dhadoop.version=3.3.1 clean package

./bin/docker-image-tool.sh -r public.ecr.aws/k0m1p4t7 -t v3.4.0 build

./bin/docker-image-tool.sh -r public.ecr.aws/k0m1p4t7 -t v3.4.0 -p resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile build

./bin/docker-image-tool.sh -r public.ecr.aws/k0m1p4t7 -t v3.4.0 


./pull-jars
 docker build . -t public.ecr.aws/k0m1p4t7/spark:v3.4.0-aws && docker push public.ecr.aws/k0m1p4t7/spark:v3.4.0-aws
 docker build . --build-arg="BASE=spark-py" -t public.ecr.aws/k0m1p4t7/spark-py:v3.4.0-aws && docker push public.ecr.aws/k0m1p4t7/spark-py:v3.4.0-aws

```