FROM golang:1.18 as builder
WORKDIR /workspace

COPY . .
RUN go mod tidy

RUN GOOS=linux GOARCH=amd64 go build -o bin/vanus-bench ./test/benchmark

FROM ubuntu:latest

COPY ./test/infra /vanus-bench
COPY --from=builder /workspace/bin/vanus-bench /usr/bin/vanus-bench

WORKDIR /vanus-bench

RUN apt-get update && apt-get install -y curl
RUN curl -O https://download.linkall.com/vsctl/v0.5.4/linux-amd64/vsctl && \
    mv vsctl /usr/bin/vsctl
RUN chmod a+x /usr/bin/vsctl
RUN chmod a+x /usr/bin/vanus-bench
RUN chmod a+x /vanus-bench/run.sh

ENTRYPOINT ["/vanus-bench/run.sh"]