FROM golang:1.19 as builder
WORKDIR /workspace

COPY . .
RUN go mod tidy

RUN GOOS=linux GOARCH=amd64 go build -o bin/pubsub ./test/regression/pubsub

FROM ubuntu:latest
COPY --from=builder /workspace/test/regression/run.sh /vanus/test/regression/run.sh
COPY --from=builder /workspace/bin/pubsub /vanus/test/regression/bin/pubsub
RUN chmod a+x /vanus/test/regression/bin/pubsub
RUN chmod a+x /vanus/test/regression/run.sh

RUN apt-get update && apt-get install -y curl
RUN curl -O https://dl.vanus.ai/vsctl/latest/linux-amd64/vsctl
RUN mv vsctl /usr/bin/vsctl
RUN chmod a+x /usr/bin/vsctl

