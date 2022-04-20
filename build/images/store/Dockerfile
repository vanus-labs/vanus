FROM golang:1.17 as builder
WORKDIR /workspace

COPY vanus/ vanus/
COPY raft/ raft/
COPY vsproto/ vsproto/
WORKDIR /workspace/vanus
RUN go env -w GOPROXY=https://goproxy.cn,direct
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-store

FROM alpine:3.15.4
RUN mkdir -p /server
COPY --from=builder /workspace/vanus/bin/store /server/bin/store
WORKDIR /server
ENTRYPOINT ["bin/store","--config=config/store.yaml"]

