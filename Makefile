VANUS_ROOT=$(shell pwd)
VSPROTO_ROOT=$(VANUS_ROOT)/proto
GIT_COMMIT=$(shell git log -1 --format='%h' | awk '{print $0}')
DATE=$(shell date +%Y-%m-%d_%H:%M:%S%z)
GO_VERSION=$(shell go version)

export VANUS_LOG_LEVEL=info

DOCKER_REGISTRY ?= public.ecr.aws
DOCKER_REPO ?= ${DOCKER_REGISTRY}/vanus
IMAGE_TAG ?= ${GIT_COMMIT}
#os linux or darwin
GOOS ?= linux
#arch amd64 or arm64
GOARCH ?= amd64

VERSION ?= ${IMAGE_TAG}

GO_BUILD= GO111MODULE=on CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath
DOCKER_BUILD_ARG= --build-arg TARGETARCH=$(GOARCH) --build-arg TARGETOS=$(GOOS)
DOCKER_PLATFORM ?= linux/amd64,linux/arm64

clean :
	rm -rf bin

docker-push: docker-push-root docker-push-controller docker-push-timer docker-push-trigger docker-push-gateway docker-push-store
docker-build: docker-build-root docker-build-controller docker-build-timer docker-build-trigger docker-build-gateway docker-build-store
build: build-root build-controller build-timer build-trigger build-gateway build-store

docker-push-store:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/store:${IMAGE_TAG} -f build/images/store/Dockerfile . --push
docker-build-store:
	docker build -t ${DOCKER_REPO}/store:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/store/Dockerfile .
build-store:
	$(GO_BUILD)  -o bin/store cmd/store/main.go

docker-push-gateway:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/gateway:${IMAGE_TAG} -f build/images/gateway/Dockerfile . --push
docker-build-gateway:
	docker build -t ${DOCKER_REPO}/gateway:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/gateway/Dockerfile .
build-gateway:
	$(GO_BUILD)  -o bin/gateway cmd/gateway/main.go

docker-push-test-infra:
	docker build -t ${DOCKER_REPO}/test-infra:${IMAGE_TAG} -f test/infra/Dockerfile .
	docker push ${DOCKER_REPO}/test-infra:${IMAGE_TAG}

docker-push-test-regression:
	docker build -t ${DOCKER_REPO}/test-regression:${IMAGE_TAG} -f test/regression/Dockerfile .
	docker push ${DOCKER_REPO}/test-regression:${IMAGE_TAG}

t1=-X 'main.Version=${VERSION}'
t2=${t1} -X 'main.GitCommit=${GIT_COMMIT}'
t3=${t2} -X 'main.BuildDate=${DATE}'
t4=${t3} -X 'main.GoVersion=${GO_VERSION}'
LD_FLAGS=${t4} -X 'main.Platform=${GOOS}/${GOARCH}'

build-cmd:
	$(GO_BUILD)  -ldflags "${LD_FLAGS}" -o bin/vsctl ./vsctl/

docker-push-root:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/root-controller:${IMAGE_TAG} -f build/images/root-controller/Dockerfile . --push
docker-build-root:
	docker build -t ${DOCKER_REPO}/root-controller:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/root-controller/Dockerfile .
build-root:
	$(GO_BUILD) -o bin/root-controller cmd/root/main.go

docker-push-controller:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/controller:${IMAGE_TAG} -f build/images/controller/Dockerfile . --push
docker-build-controller:
	docker build -t ${DOCKER_REPO}/controller:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/controller/Dockerfile .
build-controller:
	$(GO_BUILD) -o bin/controller cmd/controller/main.go

docker-push-trigger:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/trigger:${IMAGE_TAG} -f build/images/trigger/Dockerfile . --push
docker-build-trigger:
	docker build -t ${DOCKER_REPO}/trigger:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/trigger/Dockerfile .
build-trigger:
	$(GO_BUILD)  -o bin/trigger cmd/trigger/main.go

docker-push-timer:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/timer:${IMAGE_TAG} -f build/images/timer/Dockerfile . --push
docker-build-timer:
	docker build -t ${DOCKER_REPO}/timer:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/timer/Dockerfile .
build-timer:
	$(GO_BUILD)  -o bin/timer cmd/timer/main.go

docker-push-aio:
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/all-in-one:${IMAGE_TAG} -f build/all-in-one/Dockerfile . --push