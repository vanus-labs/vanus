#!/bin/bash
set -ex

vanus-bench e2e run \
  --name "${JOB_NAME}" \
  --eventbus "${JOB_NAME}" \
  --number "${TOTAL_NUMBER}" \
  --parallelism "${PARALLELISM}"  \
  --endpoint "${VANUS_GATEWAY}" \
  --payload-size "${PAYLOAD_SIZE}" \
  --redis-addr "${REDIS_ADDR}" \
  --mongodb-password "${MONGODB_PASSWORD}" \
  --batch-size "${BATCH_SIZE}" \
  --begin
