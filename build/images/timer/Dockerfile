FROM golang:1.17 as builder
WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum
COPY Makefile Makefile
COPY cmd/timer cmd/timer
COPY internal internal
COPY observability observability
COPY proto proto
COPY client client

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-timer

FROM centos:8.4.2105
RUN mkdir -p /vanus
COPY --from=builder /workspace/bin/timer /vanus/bin/timer
WORKDIR /vanus
ENTRYPOINT ["bin/timer"]

