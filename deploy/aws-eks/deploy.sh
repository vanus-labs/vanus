#!/usr/bin/env sh
kubectl apply -f sc.yml
kubectl apply -f controller.yml
kubectl apply -f store.yml
kubectl apply -f trigger.yml
kubectl apply -f timer.yml
