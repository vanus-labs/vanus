FROM --platform=$BUILDPLATFORM golang:1.18 as builder
WORKDIR /workspace

COPY . .
RUN go mod download

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-timer

FROM ubuntu:22.10
WORKDIR /vanus
COPY --from=builder /workspace/bin/timer bin/timer
ENTRYPOINT ["bin/timer"]

