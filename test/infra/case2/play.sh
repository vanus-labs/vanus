#!/bin/bash
set -ex

go build -o vanus-bench ../../benchmark
mv vanus-bench ../../../bin/

nohup vanus-bench component store run --volume-id 1 --name e2e-component-store-3replicas --without-mongodb > node1.log &
nohup vanus-bench component store run --volume-id 2 --name e2e-component-store-3replicas --without-mongodb > node2.log &
nohup vanus-bench component store run --volume-id 3 --name e2e-component-store-3replicas --without-mongodb > node3.log &

# The maximum TPS between 12K to 14K with high shake, and TPS isn't sensitive with payload-size [10B, 1024B]
# BUG: it need to wait a seconds to make sure write successfully after a Segment is activated
# vanus-bench component store create-block --name e2e-component-store-1replicas --without-mongodb --replicas 1 --block-size 512 --store-address 127.0.0.1:2149 --number 16
# vanus-bench component store send --name e2e-component-store-1replicas --without-mongodb  --no-clean-cache --total-number 1000000 --parallelism 1 --payload-size 1024

# vanus-bench component store create-block --name e2e-component-store-3replicas --without-mongodb --replicas 3  --block-size 512 --number 16
# vanus-bench component store send --total-number 1000000 --parallelism 16 --no-clean-cache --payload-size 1024 --name e2e-component-store-3replicas --without-mongodb --batch-size 32
# ps -ef | grep bench | grep -v "auto" | awk '{print $2}' | xargs kill && rm -rf /Users/wenfeng/tmp/data/test/vanus && rm *.log
# redis-cli LTRIM /vanus/test/store/block_records 1 0