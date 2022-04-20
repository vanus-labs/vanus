VANUS_ROOT=$(shell pwd)
VSPROTO_ROOT=$(VANUS_ROOT)/../vsproto

export VANUS_LOG_LEVEL=debug

DOCKER_REGISTRY ?= linkall.cloud
DOCKER_REPO ?= ${DOCKER_REGISTRY}/vanus
IMAGE_TAG ?= latest
#os linux or darwin
GOOS ?= linux
#arch amd64 or arm64
GOARCH ?= amd64

GO_BUILD= GO111MODULE=on CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath
DOCKER_BUILD_ARG= --build-arg=TARGETARCH=$(GOARCH) --build-arg=TARGETOS=$(GOOS)

clean :
	rm -rf bin

docker-build: docker-build-controller docker-build-trigger docker-build-gateway docker-build-store

build: build-controller build-trigger build-gateway build-store

docker-build-store:
	docker build -t ${DOCKER_REPO}/store:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/store/Dockerfile ../

build-store:
	$(GO_BUILD)  -o bin/store cmd/store/main.go

docker-build-gateway:
	docker build -t ${DOCKER_REPO}/gateway:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/gateway/Dockerfile ../

build-gateway:
	$(GO_BUILD)  -o bin/gateway cmd/gateway/main.go

docker-build-controller:
	docker build -t ${DOCKER_REPO}/controller:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/controller/Dockerfile ../

build-controller:
	$(GO_BUILD) -o bin/controller cmd/controller/main.go

docker-build-trigger:
	docker build -t ${DOCKER_REPO}/trigger:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/trigger/Dockerfile ../

build-trigger:
	$(GO_BUILD)  -o bin/trigger cmd/trigger/main.go

controller-start:
	go run ${VANUS_ROOT}/cmd/controller/main.go

build-ctrl-bin:
	$(GO_BUILD) -o bin/ctrl cmd/controller/eventbus/main.go

controller-start: check-module-env
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
           --proto=segment.proto 127.0.0.1:11811

trigger-start:
	go run ${VANUS_ROOT}/cmd/trigger/main.go