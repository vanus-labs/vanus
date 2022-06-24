# Deploy Vanus

Vanus is runtime in Kubernetes cluster

## Declare

File structure:

> ```text
> ├── all-in-one.yaml
> └── yaml
>     ├── controller.yaml
>     ├── gateway.yaml
>     ├── namespace.yaml
>     ├── store.yaml
>     ├── trigger.yaml
>     └── vsctl.yaml
> 
> ```

- yaml folder is yaml file of vanus core components

  - controller.yaml is yaml file of vanus controller which is responsible for service discovery, metadata management, and resource scheduling
  
  - gateway.yaml is yaml file of vanus gateway which receive CloudEvent and write them to SegmentServer
  
  - store.yaml is yaml file of vanus segmentServer which store CloudEvent data
  
  - trigger.yaml is yaml file of vanus triggerWorker which process events and route them to user workload or Sink Connector

- all-in-one.yaml is yaml file auto generate by [kustomize]  use below command

```shell
kubectl kustomize deploy > deploy/all-in-one.yaml
```

## Installation

### Pre-requisites

- [Kubernetes cluster](https://kubernetes.io/docs/setup/)

### Install

```shell
kubectl apply -f all-in-one.yml
```

### Verifying the installation

When all resources creating done, the result will be liking below:

```shell
kubectl get po -n vanus
```

Output format:

> ```text
> vanus-controller-0                  1/1     Running   0             30s
> vanus-controller-1                  1/1     Running   0             30s
> vanus-controller-2                  1/1     Running   0             30s
> vanus-gateway-5fd85c7c-vnzcw        1/1     Running   0             30s
> vanus-store-0                       1/1     Running   0             30s
> vanus-store-1                       1/1     Running   0             30s
> vanus-store-2                       1/1     Running   0             30s
> vanus-trigger-75cb74dbbf-k8jsm      1/1     Running   0             30s
> ```

### Uninstall

```shell
kubectl delete -f all-in-one.yml
```

## Install step by step

Install vanus use step by step

### Create namespace vanus

```shell
kubectl create namespace vanus
```

### Install Controller

```shell
kubectl apply -f yaml/controller.yml
```

### Install GateWay

```shell
kubectl apply -f yaml/gateway.yml
```

### Install SegmentServer

```shell
kubectl apply -f yaml/store.yml
```

### Install TriggerWorker

```shell
kubectl apply -f yaml/trigger.yml
```

[kustomize]: https://kustomize.io/
