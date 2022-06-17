FROM golang:1.17 as builder
WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum
COPY Makefile Makefile
COPY cmd/trigger cmd/trigger
COPY internal internal
COPY observability observability
COPY proto proto
COPY client client

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-trigger

FROM alpine:3.15.4
RUN mkdir -p /vanus
COPY --from=builder /workspace/bin/trigger /vanus/bin/trigger
WORKDIR /vanus
ENTRYPOINT ["bin/trigger"]

