#!/bin/bash
set -ex

go build -o vanus-bench ../../benchmark
mv vanus-bench ../../../bin/

nohup vanus-bench component store run --volume-id 1 --name e2e-component-store-3replicas --without-mongodb > node1.log &
#nohup vanus-bench component store run --volume-id 2 --name e2e-component-store-3replicas --without-mongodb > node2.log &
#nohup vanus-bench component store run --volume-id 3 --name e2e-component-store-3replicas --without-mongodb > node3.log &

# The maximum TPS between 12K to 14K with high shake, and TPS isn't sensitive with payload-size [10B, 1024B]
# BUG: it need to wait a seconds to make sure write successfully after a Segment is activated
# vanus-bench component store create-block --number 32 --replicas 1 --name e2e-component-store-1replicas --without-mongodb
# vanus-bench component store send --total-number 1000000 --parallelism 8 --payload-size 1024 --name e2e-component-store-1replicas --without-mongodb

# vanus-bench component store create-block --number 16 --replicas 3 --name e2e-component-store-3replicas --without-mongodb
# vanus-bench component store send --total-number 100000 --parallelism 16 --no-clean-cache --payload-size 1024 --name e2e-component-store-3replicas --without-mongodb
# ps -ef | grep bench | grep -v "auto" | awk '{print $2}' | xargs kill && rm -rf /Users/wenfeng/tmp/data/test/vanus && rm *.log
# redis-cli LTRIM /vanus/test/store/block_records 1 0