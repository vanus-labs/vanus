FROM --platform=$BUILDPLATFORM golang:1.18 as builder
WORKDIR /workspace

COPY . .
RUN go mod download

ARG TARGETOS
ARG TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-store

FROM alpine:3.15.4
WORKDIR /vanus
COPY --from=builder /workspace/bin/store bin/store
ENTRYPOINT ["bin/store"]

