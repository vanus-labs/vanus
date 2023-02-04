#!/bin/bash
set -ex

vsctl eventbus create eventbus --name "${EVENTBUS}" --eventlog "${EVENTLOG_NUMBER}"
vsctl subscription create --name "${EVENTBUS}" \
  --eventbus "${EVENTBUS}" \
  --sink "grpc://host.docker.internal:18080" \
  --procotol "grpc"

# start publish and subscribe case
/vanus/regression/pubsub

# vsctl eventbus delete "${EVENTBUS}"
# vsctl eventbus delete "${NOTIFICATION_EVENTBUS}"