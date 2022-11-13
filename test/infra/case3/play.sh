#!/bin/bash
set -ex

go build -o vanus-bench ../../benchmark
mv vanus-bench ../../../bin/

nohup vanus-bench local store run --volume-id 1 > node1.log &
nohup vanus-bench local store run --volume-id 2 > node2.log &
nohup vanus-bench local store run --volume-id 3 > node3.log &

# The maximum TPS between 12K to 14K with high shake, and TPS isn't sensitive with payload-size [10B, 1024B]
# BUG: it need to wait a seconds to make sure write successfully after a Segment is activated
# vanus-bench local store create-block --number 16
# vanus-bench local store send --total-number 100000 --parallelism 8 --no-clean-cache --payload-size 1024
# ps -ef | grep bench | grep -v "auto" | awk '{print $2}' | xargs kill && rm -rf /Users/wenfeng/tmp/data/test/vanus && rm *.log