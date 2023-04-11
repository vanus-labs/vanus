#!/bin/bash
set -ex

vanus-bench e2e receive \
    --name "${JOB_NAME}" \
    --redis-addr "${REDIS_ADDR}" \
    --mongodb-password "${MONGODB_PASSWORD}"