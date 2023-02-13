#!/bin/bash
set -ex

vsctl eventbus create eventbus --name "regression_pubsub" --eventlog "4"
vsctl subscription create --name "regression_pubsub" \
  --eventbus "regression_pubsub" \
  --sink "grpc://host.docker.internal:18080" \
  --protocol "grpc"

# start publish and subscribe case
/vanus/test/regression/bin/pubsub

vsctl eventbus create eventbus --name "regression_time" --eventlog "4"
vsctl subscription create --name "regression_time" \
  --eventbus "regression_time" \
  --sink "grpc://host.docker.internal:18081" \
  --protocol "grpc"

/vanus/test/regression/bin/timer

# vsctl eventbus delete "${EVENTBUS}"
# vsctl eventbus delete "${NOTIFICATION_EVENTBUS}"