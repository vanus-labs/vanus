#!/bin/bash
# this script isn't completed now

# variables
GIT_HASH=$(shell git log -1 --format='%h' | awk '{print $0}')
gateway=$1
# shellcheck disable=SC2034
number=$2
# shellcheck disable=SC2034
parallelism=$3
localIp=$4

# build main branch
make docker-push
cp deploy/all-in-one.yaml ./"${GIT_HASH}".yml
sed -i "s/v0.1.2/$GIT_HASH/g"

# deploy cluster
kubectl apply -f  "${GIT_HASH}".yml

# create resources
export VANUS_GATEWAY=$gateway
vsctl eventbus create --name performance-1
vsctl subscription create --sink http://172.31.59.20:8088 --eventbus performance-1

# run benchmark
go build -o vanus-bench test/benchmark/main.go
nohup ./vanus-bench performance run --number 100000 --parallelism 1 \
 --endpoint 172.31.54.156:8080 --payload-size 1024 > send.log &
nohup ./vanus-bench performance-1 receive --endpoint 172.31.54.156:8080 --port 8088 > receive.log &

# analyse results
## ./vanus-bench performance analyse --benchmark-id bLztrvZHG9qkTNNY --benchmark-type produce
## ./vanus-bench performance analyse --benchmark-id NZOL3c4gRxpb4bWL --benchmark-type consume

# clean resources
#vsctl eventbus delete performance-1
#vsctl subscription delete --id 1234
kubectl delete -f  "${GIT_HASH}".yml