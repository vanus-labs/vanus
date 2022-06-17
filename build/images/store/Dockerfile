FROM golang:1.17 as builder
WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum
COPY Makefile Makefile
COPY cmd/store cmd/store
COPY internal internal
COPY observability observability
COPY proto proto
COPY raft raft

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-store

FROM alpine:3.15.4
RUN mkdir -p /vanus
COPY --from=builder /workspace/bin/store /vanus/bin/store
WORKDIR /vanus
ENTRYPOINT ["bin/store"]

