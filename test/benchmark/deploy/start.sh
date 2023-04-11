#!/bin/bash

# prerequisite: install Vanus cluster first

kubectl apply -f redis.yaml
kubectl apply -f secret.yaml

kubectl apply -f job.yaml

# run play.sh by hand

kubectl delete -f job.yaml
kubectl delete -f secret.yaml
kubectl delete -f redis.yaml