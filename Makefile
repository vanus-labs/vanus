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
           --proto=controller.proto 192.168.1.111:2048

store-start:
	go run ${VANUS_ROOT}/cmd/store/main.go

store-api-test:
	grpcui --import-path=${VSPROTO_ROOT}/include \
           --import-path=${VSPROTO_ROOT}/proto \
           --plaintext \
           --proto=segment.proto 0.0.0.0:11811

trigger-start:
	go run ${VANUS_ROOT}/cmd/trigger/main.go