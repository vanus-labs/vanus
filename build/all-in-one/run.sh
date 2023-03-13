#!/usr/bin/env bash
nohup /vanus/etcd/etcd > /vanus/logs/etcd.log 2>&1 &
sleep 2

nohup /vanus/bin/root-controller --config /vanus/config/root.yaml > /vanus/logs/root.log 2>&1 &
nohup /vanus/bin/controller --config /vanus/config/controller.yaml > /vanus/logs/controller.log 2>&1 &
nohup /vanus/bin/store --config /vanus/config/store.yaml > /vanus/logs/store.log 2>&1 &
nohup /vanus/bin/trigger --config /vanus/config/trigger.yaml > /vanus/logs/trigger.log 2>&1 &
nohup /vanus/bin/timer --config /vanus/config/timer.yaml > /vanus/logs/timer.log 2>&1 &
nohup /vanus/bin/gateway --config /vanus/config/gateway.yaml > /vanus/logs/gateway.log 2>&1 &

sleep 999999

pkill -f vanus
