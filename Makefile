VANUS_ROOT=$(shell pwd)
VSPROTO_ROOT=$(VANUS_ROOT)/proto
GIT_COMMIT=$(shell git log -1 --format='%h' | awk '{print $0}')
DATE=$(shell date +%Y-%m-%d_%H:%M:%S%z)
GO_VERSION=$(shell go version)

export VANUS_LOG_LEVEL=debug

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

docker-push: docker-push-controller docker-push-timer docker-push-trigger docker-push-gateway docker-push-store
docker-build: docker-build-controller docker-build-timer docker-build-trigger docker-build-gateway docker-build-store
build: build-controller build-timer build-trigger build-gateway build-store

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

t1=-X 'main.Version=${VERSION}'
t2=${t1} -X 'main.GitCommit=${GIT_COMMIT}'
t3=${t2} -X 'main.BuildDate=${DATE}'
t4=${t3} -X 'main.GoVersion=${GO_VERSION}'
LD_FLAGS=${t4} -X 'main.Platform=${GOOS}/${GOARCH}'

build-cmd:
	$(GO_BUILD)  -ldflags "${LD_FLAGS}" -o bin/vsctl ./vsctl/

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

docker-push-timer: docker-build-timer
	docker buildx build --platform ${DOCKER_PLATFORM} -t ${DOCKER_REPO}/timer:${IMAGE_TAG} -f build/images/timer/Dockerfile . --push
docker-build-timer:
	docker build -t ${DOCKER_REPO}/timer:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/timer/Dockerfile .
build-timer:
	$(GO_BUILD)  -o bin/timer cmd/timer/main.go

controller-start:
	go run ${VANUS_ROOT}/cmd/controller/main.go

build-ctrl-bin:
	$(GO_BUILD) -o bin/ctrl cmd/controller/main.go

build-gw-util:
	go build -o bin/gw-util test/gateway/main.go

build-e2e:
	go build -o bin/e2e test/e2e/quick-start/main.go

build-destruct:
	go build -o bin/destruct test/e2e/destruct/main.go

controller-start:
	go run ${VANUS_ROOT}/cmd/controller/${module}/main.go

controller-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=controller.proto 127.0.0.1:2048

store-start:
	go run ${VANUS_ROOT}/cmd/store/main.go

store-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=segment.proto 127.0.0.1:11831

trigger-start:
	go run ${VANUS_ROOT}/cmd/trigger/main.go
