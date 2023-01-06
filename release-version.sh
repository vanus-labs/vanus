#!/usr/bin/env sh
set -ex

VERSION=$1

ROOT_PATH=/var/vanus-k8s

make docker-push IMAGE_TAG="${VERSION}"
make docker-push IMAGE_TAG="${VERSION}" DOCKER_REGISTRY=linkall.tencentcloudcr.com

sudo mkdir -p "${ROOT_PATH}"/vsctl/"${VERSION}"/linux-amd64
make build-cmd GOOS=linux GOARCH=amd64 VERSION="$VERSION"
sudo mv bin/vsctl "${ROOT_PATH}"/vsctl/"${VERSION}"/linux-amd64

sudo mkdir -p "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-amd64
make build-cmd GOOS=darwin GOARCH=amd64 VERSION="$VERSION"
sudo mv bin/vsctl "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-amd64

sudo mkdir -p "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-arm64
make build-cmd GOOS=darwin GOARCH=arm64 VERSION="$VERSION"
sudo mv bin/vsctl "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-arm64

sudo cp "${ROOT_PATH}"/all-in-one/template.yml "${ROOT_PATH}"/all-in-one/"$VERSION".yml
sudo sed -i "s/:<version>/:${VERSION}/g" "${ROOT_PATH}"/all-in-one/"$VERSION".yml
sudo cp "${ROOT_PATH}"/all-in-one/template.cn.yml "${ROOT_PATH}"/all-in-one/"$VERSION".cn.yml
sudo sed -i "s/:<version>/:${VERSION}/g" "${ROOT_PATH}"/all-in-one/"$VERSION".cn.yml
