#!/bin/bash
set -ex

vsctl eventbus create --name "${JOB_NAME}"
vanus-bench run --name \
  "${JOB_NAME}" \
  --eventbus "${EVENTBUS}" \
  --number "${TOTAL_NUMBER}" \
  --parallelism "${PARALLELISM}"  \
  --endpoint "${VANUS_GATEWAY}" \
  --payload-size "${PAYLOAD_SIZE}" \
  --redis-addr "${REDIS_ADDR}" \
  --mongodb-password "${MONGODB_PASSWORD}" \
  --begin

vanus-bench analyse \
  --name "${JOB_NAME}" \
  --benchmark-type produce \
  --redis-addr "${REDIS_ADDR}" \
  --mongodb-password "${MONGODB_PASSWORD}" \
  --end

vsctl eventbus delete --name "${JOB_NAME}"