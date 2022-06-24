FROM golang:1.17 as builder
WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum
COPY Makefile Makefile
COPY vsctl vsctl
COPY proto proto

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-cmd

FROM centos:8.4.2105
RUN mkdir -p /vanus
COPY --from=builder /workspace/bin/vsctl /vanus/bin/vsctl
WORKDIR /vanus

