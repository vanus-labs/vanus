#!/usr/bin/env sh
set -e

VERSION=$1

if [ "$VERSION" = "" ]; then
  echo "please specify version"
  exit 1
else
  echo "Version=$VERSION"
fi

DIR=$(dirname $(realpath $0))
BIN="${DIR}/bin"

# build images
# aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/vanus
# make docker-push IMAGE_TAG="${VERSION}"
# make docker-push IMAGE_TAG="${VERSION}" DOCKER_REGISTRY=linkall.tencentcloudcr.com

# build vsctl
echo "Building vsctl..."
rm -rf "${BIN}/vsctl/${VERSION}"

for os in "linux" "darwin"; do
  for arch in "amd64" "arm64"; do
    echo "Building vsctl for ${os}/${arch}..."
    mkdir -p "${BIN}/vsctl/${VERSION}/${os}-${arch}"
    make build-vsctl GOOS="${os}" GOARCH="${arch}" VERSION="${VERSION}" CMD_OUTPUT_DIR="${BIN}/vsctl/${VERSION}/${os}-${arch}"
  done
done

# update latest
echo "Updating latest vsctl"
rm -rf "${BIN}/vsctl/latest"
cp -r "${BIN}/vsctl/${VERSION}" "${BIN}/vsctl/latest"

echo "Building vsctl is done"

# build k8s yaml file
# echo "Generating Kubernetes YAML files"
# cp "${BIN}"/all-in-one/template.yml "${BIN}"/all-in-one/"$VERSION".yml
# sed -i "s/:<version>/:${VERSION}/g" "${BIN}"/all-in-one/"$VERSION".yml
# cp "${BIN}"/all-in-one/template.cn.yml "${BIN}"/all-in-one/"$VERSION".cn.yml
# sed -i "s/:<version>/:${VERSION}/g" "${BIN}"/all-in-one/"$VERSION".cn.yml

# update latest
# echo "Updating Kubernetes YAML files"
# rm "${BIN}"/all-in-one/latest.yml
# cp "${BIN}"/all-in-one/"$VERSION".yml "${BIN}"/all-in-one/latest.yml
# rm "${BIN}"/all-in-one/latest.cn.yml
# cp "${BIN}"/all-in-one/"$VERSION".cn.yml "${BIN}"/all-in-one/latest.cn.yml
# echo "Generating is done"

echo "Release Vanus $VERSION is completed."
