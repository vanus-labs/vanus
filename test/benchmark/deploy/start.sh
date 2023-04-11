#!/bin/bash

# prerequisite: install Vanus cluster first

kubectl apply -f redis.yaml
kubectl apply -f secret.yaml

# wait to vanus is ready
kubectl apply -f case1/job.yaml
kubectl apply -f case2/job.yaml

kubectl delete -f case1/job.yaml
kubectl delete -f case2/job.yaml
kubectl delete -f secret.yaml
kubectl delete -f benchmark.yaml