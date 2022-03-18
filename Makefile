VANUS_ROOT=$(shell pwd)
VSPROTO_ROOT=$(VANUS_ROOT)/../vsproto

export VANUS_LOG_LEVEL=debug

check-module-env:
ifndef module
	$(error module is undefined)
endif

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