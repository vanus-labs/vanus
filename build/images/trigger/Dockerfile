FROM golang:1.17 as builder
WORKDIR /workspace

COPY vanus/ vanus/
COPY eventbus-go/ eventbus-go/
COPY vsproto/ vsproto/
WORKDIR /workspace/vanus
RUN go env -w GOPROXY=https://goproxy.cn,direct
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-trigger

FROM alpine:3.15.4
RUN mkdir -p /server
COPY --from=builder /workspace/vanus/bin/trigger /server/bin/trigger
WORKDIR /server
ENTRYPOINT ["bin/trigger","--config=config/trigger.yaml"]

