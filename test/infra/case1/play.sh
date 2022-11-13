#!/bin/bash
set -ex

vsctl eventbus create --name "${JOB_NAME}"
vanus-bench e2e run \
  --name "${JOB_NAME}" \
  --eventbus "${JOB_NAME}" \
  --number "${TOTAL_NUMBER}" \
  --parallelism "${PARALLELISM}"  \
  --endpoint "${VANUS_GATEWAY}" \
  --payload-size "${PAYLOAD_SIZE}" \
  --redis-addr "${REDIS_ADDR}" \
  --mongodb-password "${MONGODB_PASSWORD}" \
  --begin

vanus-bench e2e analyse \
  --name "${JOB_NAME}" \
  --benchmark-type produce \
  --redis-addr "${REDIS_ADDR}" \
  --mongodb-password "${MONGODB_PASSWORD}" \
  --end

vsctl eventbus delete --name "${JOB_NAME}"