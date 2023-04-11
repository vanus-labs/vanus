FROM golang:1.18 as builder
WORKDIR /workspace

COPY . .
RUN go mod tidy

RUN GOOS=linux GOARCH=amd64 go build -o bin/vanus-bench ./test/benchmark

FROM ubuntu:latest

COPY --from=builder /workspace/bin/vanus-bench /usr/bin/vanus-bench

WORKDIR /vanus-bench

RUN apt-get update && apt-get install -y curl
RUN curl -O https://dl.vanus.ai/vsctl/latest/linux-amd64/vsctl && \
    mv vsctl /usr/bin/vsctl
RUN chmod a+x /usr/bin/vsctl
RUN chmod a+x /usr/bin/vanus-bench
