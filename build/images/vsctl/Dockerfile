FROM golang:1.17 as builder
WORKDIR /workspace

COPY vanus/ vanus/
COPY embed-etcd/ embed-etcd/
COPY vsproto/ vsproto/
WORKDIR /workspace/vanus
RUN go env -w GOPROXY=https://goproxy.cn,direct
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-cmd

FROM centos:8.4.2105
RUN mkdir -p /server
COPY --from=builder /workspace/vanus/bin/vsctl /server/bin/vsctl
WORKDIR /server

