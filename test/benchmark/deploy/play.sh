#!/bin/bash
set -ex

# send
vsctl eventbus create --name "send-only-eventbus" --eventlog "16"

vanus-bench e2e run \
  --name "send-only" \
  --eventbus "send-only-eventbus" \
  --number "10000000" \
  --parallelism "16"  \
  --endpoint "vanus-gateway.vanus:8080" \
  --payload-size "1024" \
  --batch-size "128" \
  --redis-addr "redis-service.vanus:6379" \
  --begin

vanus-bench e2e analyse \
  --name "send-only" \
  --benchmark-type produce \
  --redis-addr "redis-service.vanus:6379" \
  --end

vanus-bench e2e receive \
    --name "send-only" \
    --redis-addr "redis-service.vanus:6379"

vsctl eventbus delete --name "send-only-eventbus"