VANUS_ROOT = $(shell pwd)
GIT_COMMIT = $(shell git log -1 --format='%h' | awk '{print $0}')
DATE = $(shell date +%Y-%m-%d_%H:%M:%S%z)
GO_VERSION = $(shell go version)

export VANUS_LOG_LEVEL=info

DOCKER_REGISTRY ?= public.ecr.aws
DOCKER_REPO ?= ${DOCKER_REGISTRY}/vanus
IMAGE_PREFIX ?=
IMAGE_TAG ?= ${GIT_COMMIT}

#os linux or darwin
GOOS ?= linux
#arch amd64 or arm64
GOARCH ?= amd64

BIN_DIR = ${VANUS_ROOT}/bin
CMD_DIR = ${VANUS_ROOT}/cmd
CMD_OUTPUT_DIR ?= ${BIN_DIR}

VERSION ?= ${IMAGE_TAG}

GO_BUILD = GO111MODULE=on CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath
DOCKER_BUILD_ARG = --build-arg TARGETARCH=$(GOARCH) --build-arg TARGETOS=$(GOOS)
DOCKER_PLATFORM ?= linux/amd64,linux/arm64

clean:
	rm -rf bin

LD_FLAGS = -X 'main.Version=${VERSION}'
LD_FLAGS += -X 'main.GitCommit=${GIT_COMMIT}'
LD_FLAGS += -X 'main.BuildDate=${DATE}'
LD_FLAGS += -X 'main.GoVersion=${GO_VERSION}'
LD_FLAGS += -X 'main.Platform=${GOOS}/${GOARCH}'

build-vsctl:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/vsctl ${CMD_DIR}/vsctl/

build-vsrepair:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/vsrepair ${CMD_DIR}/vsrepair/

.PHONY: build-cmd
build-cmd: build-vsctl

.PHONY: docker-push
docker-push: docker-push-root docker-push-controller docker-push-timer docker-push-trigger docker-push-gateway docker-push-store

.PHONY: docker-build
docker-build: docker-build-root docker-build-controller docker-build-timer docker-build-trigger docker-build-gateway docker-build-store

.PHONY: build
build: build-root build-controller build-store build-timer build-trigger build-gateway

docker-push-root:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}root-controller:${IMAGE_TAG} -f build/images/root-controller/Dockerfile . --push
docker-build-root:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}root-controller:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/root-controller/Dockerfile .
build-root:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/root-controller ${CMD_DIR}/vs-root/

docker-push-controller:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}controller:${IMAGE_TAG} -f build/images/controller/Dockerfile . --push
docker-build-controller:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}controller:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/controller/Dockerfile .
build-controller:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/controller ${CMD_DIR}/vs-controller/

docker-push-store:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}store:${IMAGE_TAG} -f build/images/store/Dockerfile . --push
docker-build-store:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}store:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/store/Dockerfile .
build-store:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/store ${CMD_DIR}/vs-store/

docker-push-timer:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}timer:${IMAGE_TAG} -f build/images/timer/Dockerfile . --push
docker-build-timer:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}timer:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/timer/Dockerfile .
build-timer:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/timer ${CMD_DIR}/vs-timer/

docker-push-trigger:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}trigger:${IMAGE_TAG} -f build/images/trigger/Dockerfile . --push
docker-build-trigger:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}trigger:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/trigger/Dockerfile .
build-trigger:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/trigger ${CMD_DIR}/vs-trigger/

docker-push-gateway:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}gateway:${IMAGE_TAG} -f build/images/gateway/Dockerfile . --push
docker-build-gateway:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/${IMAGE_PREFIX}gateway:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/gateway/Dockerfile .
build-gateway:
	$(GO_BUILD) -ldflags "${LD_FLAGS}" -o ${CMD_OUTPUT_DIR}/gateway ${CMD_DIR}/vs-gateway/

docker-push-aio:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/all-in-one:${IMAGE_TAG} -f build/all-in-one/Dockerfile . --push

docker-push-test-infra:
	docker build -t ${DOCKER_REPO}/test-infra:${IMAGE_TAG} -f test/infra/Dockerfile .
	docker push ${DOCKER_REPO}/test-infra:${IMAGE_TAG}

docker-push-test-regression:
	docker build -t ${DOCKER_REPO}/test-regression:${IMAGE_TAG} -f test/regression/Dockerfile .
	docker push ${DOCKER_REPO}/test-regression:${IMAGE_TAG}

docker-push-toolbox:
	docker buildx build --no-cache --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/toolbox:${IMAGE_TAG} -f build/toolbox/Dockerfile . --push
