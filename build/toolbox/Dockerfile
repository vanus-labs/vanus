FROM --platform=$BUILDPLATFORM ubuntu:22.10

ARG TARGETOS
ARG TARGETARCH

WORKDIR /vanus/toolbox

RUN apt update -y && apt upgrade -y
# install tools
RUN apt install -y wget curl ncat vim

# install etcdctl
RUN wget https://github.com/etcd-io/etcd/releases/download/v3.5.7/etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz
RUN tar -xzvf etcd-v3.5.7-$TARGETOS-$TARGETARCH.tar.gz

# install vsctl
RUN wget https://dl.vanus.ai/vsctl/latest/$TARGETOS-$TARGETARCH/vsctl

# mv to /vanus/toolbox/bin
RUN mkdir -p /vanus/toolbox/bin
RUN mv etcd-v3.5.7-$TARGETOS-$TARGETARCH/etcdctl /vanus/toolbox/bin
RUN mv vsctl /vanus/toolbox/bin
RUN chmod a+x /vanus/toolbox/bin/*

# clean
RUN rm -rf etcd-v3.5.7-$TARGETOS-$TARGETARCH.*

RUN echo 'alias vsctl="vsctl --endpoint vanus-gateway:8080 --operator-endpoint vanus-operator:8089"' >> /root/.bashrc
ENV PATH=$PATH:/vanus/toolbox/etcd:/vanus/toolbox/bin