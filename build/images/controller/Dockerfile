FROM golang:1.17 as builder
WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum
COPY Makefile Makefile
COPY cmd/controller cmd/controller
COPY internal internal
COPY observability observability
COPY proto proto

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-controller

FROM centos:8.4.2105
RUN mkdir -p /vanus
COPY --from=builder /workspace/bin/controller /vanus/bin/controller
WORKDIR /vanus
ENTRYPOINT ["bin/controller"]

