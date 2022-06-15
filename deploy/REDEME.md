# Vanus Installation

Vanus is runtime in Kubernetes cluster

## Pre-requisites
- [Kubernetes cluster]( https://kubernetes.io/docs/setup/)

## Install by all-in-one

### Install

```shell
~ > kubectl apply -f all-in-one.yml
```
### Verifying the installation
when all resources creating done, the result will be liking below:
```shell
~ > kubectl get po -n vanus
vanus-controller-0                  1/1     Running   0             30s
vanus-controller-1                  1/1     Running   0             30s
vanus-controller-2                  1/1     Running   0             30s
vanus-gateway-5fd85c7c-vnzcw        1/1     Running   0             30s
vanus-store-0                       1/1     Running   0             30s
vanus-store-1                       1/1     Running   0             30s
vanus-store-2                       1/1     Running   0             30s
vanus-trigger-75cb74dbbf-k8jsm      1/1     Running   0             30s
```

### Uninstall

```shell
~ > kubectl delete -f all-in-one.yml
```

## Install step by step

install vanus use step by step,first need create namespace: vanus

### Install Controller
```shell
~ > kubectl apply -f yaml/controller.yml
```

### Install GateWay
```shell
~ > kubectl apply -f yaml/gateway.yml
```

### Install Store
```shell
~ > kubectl apply -f yaml/store.yml
```

### Install Trigger
```shell
~ > kubectl apply -f yaml/trigger.yml
```
