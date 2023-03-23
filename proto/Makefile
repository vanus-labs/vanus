VSPROTO_ROOT=$(shell pwd)

check-package-env:
ifndef package
	$(error package is undefined)
endif

generate-pb: check-package-env
	mkdir -p ${VSPROTO_ROOT}/pkg/${package}
	protoc -I=${VSPROTO_ROOT}/include \
          -I=${VSPROTO_ROOT}/proto \
          --go_out=${VSPROTO_ROOT}/pkg/${package} \
	  --go_opt=paths=source_relative \
	  --go-grpc_out=${VSPROTO_ROOT}/pkg/${package} \
	  --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false \
	  --go-grpc-mock_out=${VSPROTO_ROOT}/pkg/${package} \
	  --go-grpc-mock_opt=paths=source_relative \
	  ${VSPROTO_ROOT}/proto/${package}.proto
