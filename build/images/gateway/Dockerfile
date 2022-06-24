FROM golang:1.17 as builder
WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum
COPY Makefile Makefile
COPY cmd/gateway cmd/gateway
COPY internal internal
COPY observability observability
COPY proto proto
COPY client client

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-gateway

FROM alpine:3.15.4
RUN mkdir -p /vanus
COPY --from=builder /workspace/bin/gateway /vanus/bin/gateway
WORKDIR /vanus
ENTRYPOINT ["bin/gateway"]

