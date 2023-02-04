#!/usr/bin/env sh
set -ex

# install grafana
apt update
apt install -y software-properties-common wget
wget -q -O /usr/share/keyrings/grafana.key https://apt.grafana.com/gpg.key
echo "deb [signed-by=/usr/share/keyrings/grafana.key] https://apt.grafana.com stable main" | tee -a /etc/apt/sources.list.d/grafana.list
apt update
apt install -y grafana

# install provisioning
apt install -y git
cd /vanus
git clone https://github.com/linkall-labs/observability.git
sed -i 's/\/home\/grafana/\/vanus\/observability\/grafana/g' /vanus/observability/grafana/provisioning/dashboards/vanus.yaml

