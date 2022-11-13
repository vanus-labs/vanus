#!/bin/bash
set -ex

vsctl eventbus create --name "${CASE_NAME}" --eventlog 16

vanus-bench run --name "${CASE_NAME}" --group 1 --eventbus "${CASE_NAME}" \
  --number "${UNIT1_NUMBER}" \
  --parallelism 16  \
  --endpoint "${VANUS_GATEWAY}" \
  --redis-addr "${REDIS_ADDR}" \
  --payload-size "${PAYLOAD_SIZE}"

vanus-bench analyse --name "${CASE_NAME}" --group 1  --benchmark-type produce \
  --redis-addr "${REDIS_ADDR}"

vsctl eventbus delete --name "${CASE_NAME}"