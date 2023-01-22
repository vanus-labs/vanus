FROM --platform=$BUILDPLATFORM golang:1.18 as builder
WORKDIR /workspace

COPY . .
RUN go mod download

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-controller
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-store
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-trigger
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-timer
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-gateway

FROM centos:latest
WORKDIR /vanus

ARG VERSION
COPY --from=builder /workspace/bin/controller bin/controller
COPY --from=builder /workspace/bin/store bin/store
COPY --from=builder /workspace/bin/trigger bin/trigger
COPY --from=builder /workspace/bin/timer bin/timer
COPY --from=builder /workspace/bin/gateway bin/gateway
COPY --from=builder /workspace/build/all-in-one/config config
COPY --from=builder /workspace/build/all-in-one/run.sh run.sh

RUN chmod a+x /vanus/run.sh
RUN mkdir /vanus/logs

ENV VERSION=$VERSION

ENTRYPOINT ["/vanus/run.sh"]

