
VSPROTO_ROOT=$(shell pwd )

check-package-env:
ifndef package
	$(error package is undefined)
endif

generate-pb: check-package-env
	mkdir -p ${VSPROTO_ROOT}/pkg/${package}
	protoc -I=${VSPROTO_ROOT}/include \
           -I=${VSPROTO_ROOT}/proto \
           --go_out=plugins=grpc,paths=source_relative:${VSPROTO_ROOT}/pkg/${package} \
           ${VSPROTO_ROOT}/proto/${package}.proto
