#!/bin/bash
set -ex

vsctl eventbus create --name "benchmark" --eventlog "64" --namespace default

/tmp/vanus-bench send \
  --eventbus "benchmark" \
  --number "5000000" \
  --parallelism "64"  \
  --batch-size "32" \
  --endpoint "vanus-gateway.vanus:8080" \
  --payload-size "1024"

/tmp/vanus-bench receive --number 5000000

vsctl subscription create --name "benchmark" \
  --sink "172.31.41.195:8080" \
  --eventbus "benchmark" \
  --protocol "grpc" \
  --from latest

vsctl eventbus delete --name "benchmark"
vsctl subscription delete --id 00006E7481000010