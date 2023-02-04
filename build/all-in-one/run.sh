#!/usr/bin/env bash
nohup grafana-server web
nohup /vanus/bin/controller --config /vanus/config/controller.yaml > /vanus/logs/controller.log &
nohup /vanus/bin/store --config /vanus/config/store.yaml > /vanus/logs/store.log &
nohup /vanus/bin/trigger --config /vanus/config/trigger.yaml > /vanus/logs/trigger.log &
nohup /vanus/bin/timer --config /vanus/config/timer.yaml > /vanus/logs/timer.log &
nohup /vanus/bin/gateway --config /vanus/config/gateway.yaml > /vanus/logs/gateway.log &

sleep 3600

pkill -f vanus
