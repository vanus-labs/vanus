FROM golang:1.17 as builder
WORKDIR /workspace

COPY vanus/ vanus/
COPY eventbus-go/ eventbus-go/
COPY vsproto/ vsproto/
WORKDIR /workspace/vanus
RUN go env -w GOPROXY=https://goproxy.cn,direct
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-gateway

FROM centos:8.4.2105
RUN mkdir -p /server
COPY --from=builder /workspace/vanus/bin/gateway /server/bin/gateway
WORKDIR /server
ENTRYPOINT ["bin/gateway","--config=config/gateway.yaml"]

