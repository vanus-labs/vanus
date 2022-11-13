#!/bin/bash

kubectl apply -f vanus.yml
kubectl apply -f benchmark.yml
kubectl apply -f secret.yml

# wait to vanus is ready
kubectl apply -f case1/job.yml
kubectl apply -f case2/job.yml

kubectl delete -f vanus.yml
kubectl delete -f case1/job.yml
kubectl delete -f case2/job.yml
kubectl delete -f benchmark.yml