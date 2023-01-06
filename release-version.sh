#!/usr/bin/env sh
set -e

VERSION=$1

if [ "$VERSION" = "" ]
then
  echo "please specify version"
  exit 1
else
  echo "Version=$VERSION"
fi

ROOT_PATH=/var/vanus-k8s

# build images
make docker-push IMAGE_TAG="${VERSION}"
make docker-push IMAGE_TAG="${VERSION}" DOCKER_REGISTRY=linkall.tencentcloudcr.com

# build vsctl
echo "Building vsctl for linux/amd64..."
sudo rm -r "${ROOT_PATH}"/vsctl/"${VERSION}"
sudo mkdir -p "${ROOT_PATH}"/vsctl/"${VERSION}"/linux-amd64
make build-cmd GOOS=linux GOARCH=amd64 VERSION="$VERSION"
sudo mv bin/vsctl "${ROOT_PATH}"/vsctl/"${VERSION}"/linux-amd64

echo "Building vsctl for darwin/amd64..."
sudo mkdir -p "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-amd64
make build-cmd GOOS=darwin GOARCH=amd64 VERSION="$VERSION"
sudo mv bin/vsctl "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-amd64

echo "Building vsctl for darwin/arm64..."
sudo mkdir -p "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-arm64
make build-cmd GOOS=darwin GOARCH=arm64 VERSION="$VERSION"
sudo mv bin/vsctl "${ROOT_PATH}"/vsctl/"${VERSION}"/macos-arm64

# update latest
echo "Updating latest vsctl"
sudo rm -r "${ROOT_PATH}"/vsctl/latest
sudo cp -r "${ROOT_PATH}"/vsctl/"${VERSION}" "${ROOT_PATH}"/vsctl/latest

echo "Building vsctl is done"

# build k8s yaml file
echo "Generating Kubernetes YAML files"
sudo cp "${ROOT_PATH}"/all-in-one/template.yml "${ROOT_PATH}"/all-in-one/"$VERSION".yml
sudo sed -i "s/:<version>/:${VERSION}/g" "${ROOT_PATH}"/all-in-one/"$VERSION".yml
sudo cp "${ROOT_PATH}"/all-in-one/template.cn.yml "${ROOT_PATH}"/all-in-one/"$VERSION".cn.yml
sudo sed -i "s/:<version>/:${VERSION}/g" "${ROOT_PATH}"/all-in-one/"$VERSION".cn.yml

# update latest
echo "Updating Kubernetes YAML files"
sudo rm "${ROOT_PATH}"/all-in-one/latest.yml
sudo cp "${ROOT_PATH}"/all-in-one/"$VERSION".yml "${ROOT_PATH}"/all-in-one/latest.yml
sudo rm "${ROOT_PATH}"/all-in-one/latest.cn.yml
sudo cp "${ROOT_PATH}"/all-in-one/"$VERSION".cn.yml "${ROOT_PATH}"/all-in-one/latest.cn.yml
echo "Generating is done"

echo "Release Vanus $VERSION is completed."