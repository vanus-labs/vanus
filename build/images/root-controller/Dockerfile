FROM --platform=$BUILDPLATFORM golang:1.18 as builder
WORKDIR /workspace

COPY . .
RUN go mod download

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-root

FROM ubuntu:22.10
WORKDIR /vanus
COPY --from=builder /workspace/bin/root-controller bin/root-controller
ENTRYPOINT ["bin/root-controller"]
