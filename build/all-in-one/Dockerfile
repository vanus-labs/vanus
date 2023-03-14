FROM --platform=$BUILDPLATFORM golang:1.18 as builder
WORKDIR /workspace

COPY . .
RUN go mod download

ARG TARGETOS
ARG TARGETARCH

RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-root
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-controller
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-store
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-trigger
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-timer
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-gateway


FROM ubuntu:22.04
WORKDIR /vanus

ARG VERSION
ARG TARGETOS
ARG TARGETARCH
COPY --from=builder /workspace/bin/root-controller bin/root-controller
COPY --from=builder /workspace/bin/controller bin/controller
COPY --from=builder /workspace/bin/store bin/store
COPY --from=builder /workspace/bin/trigger bin/trigger
COPY --from=builder /workspace/bin/timer bin/timer
COPY --from=builder /workspace/bin/gateway bin/gateway
COPY --from=builder /workspace/build/all-in-one/config config
COPY --from=builder /workspace/build/all-in-one/run.sh run.sh

RUN apt update -y
RUN apt install wget -y
RUN wget https://github.com/etcd-io/etcd/releases/download/v3.5.7/etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz
RUN tar -xzvf etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz
RUN mv etcd-v3.5.7-$TARGETOS-$TARGETARCH etcd
RUN rm -rf etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz

RUN chmod a+x /vanus/run.sh
RUN chmod a+x /vanus/etcd/etcd
RUN mkdir /vanus/logs
ENV VERSION=$VERSION
ENV VANUS_LOG_LEVEL=debug

ENTRYPOINT ["/vanus/run.sh"]

