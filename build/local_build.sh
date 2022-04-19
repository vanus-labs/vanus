#!/bin/bash

if [ $# != 1 ]; then
  echo "USAGE $0 build_name"
  echo "e.g.: $0 build-trigger"
  exit 1
fi
build_name=$1
make ${build_name} GOOS=$(go env GOOS) GOARCH=$(go env GOARCH)