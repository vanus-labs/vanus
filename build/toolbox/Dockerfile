FROM --platform=$BUILDPLATFORM alpine

ARG TARGETOS
ARG TARGETARCH

RUN apt --update add curl
RUN wget https://github.com/etcd-io/etcd/releases/download/v3.5.7/etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz
RUN tar -xzvf etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz
RUN mv etcd-v3.5.7-$TARGETOS-$TARGETARCH etcd
RUN rm -rf etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz

RUN alias vsctl=vsctl --endpoint vanus-gateway:8080 --operator-endpoint vanus-operator:8080/api