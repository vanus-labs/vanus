#!/bin/bash
set -ex

go build -o vanus-bench ../../benchmark
mv vanus-bench ../../../bin/
nohup vanus-bench local store run --volume-id 1 > node1.log &
nohup vanus-bench local store run --volume-id 2 > node2.log &
nohup vanus-bench local store run --volume-id 3 > node3.log &

vanus-bench local store create-block --number 10
vanus-bench local store send

# ps -ef | grep bench | grep -v "auto" | awk '{print $2}' | xargs kill && rm -rf /Users/wenfeng/tmp/data/test/vanus && rm *.log