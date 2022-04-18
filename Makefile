VANUS_ROOT=$(shell pwd)
VSPROTO_ROOT=$(VANUS_ROOT)/../vsproto

export VANUS_LOG_LEVEL=debug

DOCKER_REGISTRY ?= linkall.cloud
DOCKER_REPO ?= ${DOCKER_REGISTRY}/vanus
IMAGE_TAG ?= latest

GOOS ?=darwin
GOARCH ?=arm64

GO_BUILD= GO111MODULE=on CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath
check-module-env:
ifndef module
	$(error module is undefined)

endif

docker-build: docker-build-controller docker-build-trigger

clean :
	rm -rf bin
build: build-controller build-trigger

docker-build-controller:
	docker build -t ${DOCKER_REPO}/controller:${IMAGE_TAG} -f build/images/controller/Dockerfile ../

build-controller:
	$(GO_BUILD)  -o bin/controller cmd/controller/main.go

docker-build-trigger:
	docker build -t ${DOCKER_REPO}/trigger:${IMAGE_TAG} -f build/images/trigger/Dockerfile ../

build-trigger:
	$(GO_BUILD)  -o bin/trigger cmd/trigger/main.go

controller-start: check-module-env
	go run ${VANUS_ROOT}/cmd/controller/${module}/main.go

controller-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=controller.proto127.0.0.1:2048

trigger-controller-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=controller.proto 127.0.0.1:2049

store-start:
	go run ${VANUS_ROOT}/cmd/store/main.go

store-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=segment.proto 127.0.0.1:11811

trigger-start:
	go run ${VANUS_ROOT}/cmd/trigger/main.go