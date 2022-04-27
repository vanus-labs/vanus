VANUS_ROOT=$(shell pwd)
VSPROTO_ROOT=$(VANUS_ROOT)/../vsproto

export VANUS_LOG_LEVEL=debug

DOCKER_REGISTRY ?= public.ecr.aws
DOCKER_REPO ?= ${DOCKER_REGISTRY}/t8a4l2d7
IMAGE_TAG ?= latest
#os linux or darwin
GOOS ?= linux
#arch amd64 or arm64
GOARCH ?= amd64

GO_BUILD= GO111MODULE=on CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath
DOCKER_BUILD_ARG= --build-arg TARGETARCH=$(GOARCH) --build-arg TARGETOS=$(GOOS)

clean :
	rm -rf bin

docker-push: docker-push-controller docker-push-trigger docker-push-gateway docker-push-store
docker-build: docker-build-controller docker-build-trigger docker-build-gateway docker-build-store
build: build-controller build-trigger build-gateway build-store

docker-push-store: docker-build-store
	docker push ${DOCKER_REPO}/store:${IMAGE_TAG}
docker-build-store:
	docker build -t ${DOCKER_REPO}/store:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/store/Dockerfile ../
build-store:
	$(GO_BUILD)  -o bin/store cmd/store/main.go

docker-push-gateway: docker-build-gateway
	docker push ${DOCKER_REPO}/gateway:${IMAGE_TAG}
docker-build-gateway:
	docker build -t ${DOCKER_REPO}/gateway:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/gateway/Dockerfile ../
build-gateway:
	$(GO_BUILD)  -o bin/gateway cmd/gateway/main.go

docker-push-controller: docker-build-controller
	docker push ${DOCKER_REPO}/controller:${IMAGE_TAG}
docker-build-controller:
	docker build -t ${DOCKER_REPO}/controller:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/controller/Dockerfile ../
build-controller:
	$(GO_BUILD) -o bin/controller cmd/controller/main.go

docker-push-trigger: docker-build-trigger
	docker push ${DOCKER_REPO}/trigger:${IMAGE_TAG}
docker-build-trigger:
	docker build -t ${DOCKER_REPO}/trigger:${IMAGE_TAG} $(DOCKER_BUILD_ARG) -f build/images/trigger/Dockerfile ../
build-trigger:
	$(GO_BUILD)  -o bin/trigger cmd/trigger/main.go

controller-start:
	go run ${VANUS_ROOT}/cmd/controller/main.go

build-ctrl-bin:
	$(GO_BUILD) -o bin/ctrl cmd/controller/main.go

build-gw-util:
	go build -o bin/gw-util test/gateway/main.go

controller-start:
	go run ${VANUS_ROOT}/cmd/controller/${module}/main.go

controller-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=controller.proto 192.168.1.120:2048

store-start:
	go run ${VANUS_ROOT}/cmd/store/main.go

store-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=segment.proto 127.0.0.1:2148

trigger-start:
	go run ${VANUS_ROOT}/cmd/trigger/main.go