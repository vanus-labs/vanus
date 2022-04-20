FROM golang:1.17 as builder
WORKDIR /workspace

COPY vanus/ vanus/
COPY embed-etcd/ embed-etcd/
COPY vsproto/ vsproto/
WORKDIR /workspace/vanus
RUN go env -w GOPROXY=https://goproxy.cn,direct
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-controller

FROM alpine:3.15.4
RUN mkdir -p /server
COPY --from=builder /workspace/vanus/bin/controller /server/bin/controller
WORKDIR /server
ENTRYPOINT ["bin/controller","--config=config/controller.yaml"]

