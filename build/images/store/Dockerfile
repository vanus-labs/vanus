FROM --platform=$BUILDPLATFORM golang:1.19 as builder

WORKDIR /workspace

# pre-copy/cache go.mod for pre-downloading dependencies and only redownloading them in subsequent builds if they change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

ARG TARGETOS
ARG TARGETARCH

COPY . .
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build-store && \
        GOOS=$TARGETOS GOARCH=$TARGETARCH make build-vsrepair

FROM --platform=$TARGETPLATFORM ubuntu:22.04

WORKDIR /vanus

RUN apt-get update && apt-get install -y \
        ca-certificates \
        tzdata \
        && update-ca-certificates \
        && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/bin/store bin/store
COPY --from=builder /workspace/bin/vsrepair bin/vsrepair

CMD ["bin/store"]
