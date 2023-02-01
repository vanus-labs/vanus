// Copyright 2023 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

type ClusterCreate struct {
	ControllerReplicas    int32  `json:"controller_replicas,omitempty"`
	ControllerStorageSize string `json:"controller_storage_size,omitempty"`
	StoreReplicas         int32  `json:"store_replicas,omitempty"`
	StoreStorageSize      string `json:"store_storage_size,omitempty"`
	Version               string `json:"version,omitempty"`
}

type ClusterDelete struct {
	Force *bool `json:"force,omitempty"`
}

type ClusterPatch struct {
	ControllerReplicas int32  `json:"controller_replicas,omitempty"`
	StoreReplicas      int32  `json:"store_replicas,omitempty"`
	TriggerReplicas    int32  `json:"trigger_replicas,omitempty"`
	Version            string `json:"version,omitempty"`
}

type ClusterInfo struct {
	Status  string `json:"status,omitempty"`
	Version string `json:"version,omitempty"`
}

type GetClusterOKBody struct {
	Code    *int32       `json:"code"`
	Data    *ClusterInfo `json:"data"`
	Message *string      `json:"message"`
}

func NewClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster sub-command ",
		Short: "vanus cluster operations",
	}
	cmd.AddCommand(createClusterCommand())
	cmd.AddCommand(deleteClusterCommand())
	cmd.AddCommand(upgradeClusterCommand())
	cmd.AddCommand(scaleClusterCommand())
	cmd.AddCommand(getClusterCommand())
	return cmd
}

func createClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a cluster",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				cmdFailedf(cmd, "get operator endpoint failed: %s", err)
			}

			if !operatorIsDeployed(cmd, operator_endpoint) {
				cmdFailedWithHelpNotice(cmd, "The vanus operator has not been deployed. Please use the following command to deploy: \n\n    kubectl apply -f https://download.linkall.com/vanus/operator/latest.yml")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operator_endpoint, BaseUrl)
			cluster := ClusterCreate{
				ControllerReplicas:    controllerReplicas,
				ControllerStorageSize: controllerStorageSize,
				StoreReplicas:         storeReplicas,
				StoreStorageSize:      storeStorageSize,
				Version:               clusterVersion,
			}
			dataByte, err := json.Marshal(cluster)
			if err != nil {
				cmdFailedf(cmd, "json marshal cluster failed: %s", err)
			}
			bodyReader := bytes.NewReader(dataByte)
			req, err := http.NewRequest("POST", url, bodyReader)
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "create cluster failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Create Cluster Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Create Cluster Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&clusterVersion, "version", "v0.6.0", "cluster version")
	cmd.Flags().Int32Var(&controllerReplicas, "controller-replicas", 3, "controller replicas")
	cmd.Flags().StringVar(&controllerStorageSize, "controller-storage-size", "1Gi", "controller storage size")
	cmd.Flags().Int32Var(&storeReplicas, "store-replicas", 3, "store replicas")
	cmd.Flags().StringVar(&storeStorageSize, "store-storage-size", "1Gi", "store storage size")
	return cmd
}

func deleteClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a cluster",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				cmdFailedf(cmd, "get operator endpoint failed: %s", err)
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operator_endpoint, BaseUrl)
			force := false
			cluster := ClusterDelete{
				Force: &force,
			}
			dataByte, err := json.Marshal(cluster)
			if err != nil {
				cmdFailedf(cmd, "json marshal cluster failed: %s", err)
			}
			bodyReader := bytes.NewReader(dataByte)
			req, err := http.NewRequest("DELETE", url, bodyReader)
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "delete cluster failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Delete Cluster Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Delete Cluster Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	return cmd
}

func upgradeClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "upgeade cluster",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				cmdFailedf(cmd, "get operator endpoint failed: %s", err)
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operator_endpoint, BaseUrl)
			cluster := ClusterPatch{
				Version: clusterVersion,
			}
			dataByte, err := json.Marshal(cluster)
			if err != nil {
				cmdFailedf(cmd, "json marshal cluster failed: %s", err)
			}
			bodyReader := bytes.NewReader(dataByte)
			req, err := http.NewRequest("PATCH", url, bodyReader)
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "upgrade cluster failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Upgrade Cluster Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Upgrade Cluster Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&clusterVersion, "version", "v0.6.0", "cluster version")
	return cmd
}

func scaleClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "scale",
		Short: "scale cluster components",
	}
	cmd.AddCommand(scaleControllerReplicas())
	cmd.AddCommand(scaleStoreReplicas())
	cmd.AddCommand(scaleTriggerReplicas())
	return cmd
}

func scaleControllerReplicas() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "controller",
		Short: "scale controller replicas",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				cmdFailedf(cmd, "get operator endpoint failed: %s", err)
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operator_endpoint, BaseUrl)
			cluster := ClusterPatch{
				ControllerReplicas: controllerReplicas,
			}
			dataByte, err := json.Marshal(cluster)
			if err != nil {
				cmdFailedf(cmd, "json marshal cluster failed: %s", err)
			}
			bodyReader := bytes.NewReader(dataByte)
			req, err := http.NewRequest("PATCH", url, bodyReader)
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "scale controller failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Scale Controller Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Scale Controller Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().Int32Var(&controllerReplicas, "replicas", 3, "replicas")
	return cmd
}

func scaleStoreReplicas() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "scale store replicas",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				cmdFailedf(cmd, "get operator endpoint failed: %s", err)
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operator_endpoint, BaseUrl)
			cluster := ClusterPatch{
				StoreReplicas: storeReplicas,
			}
			dataByte, err := json.Marshal(cluster)
			if err != nil {
				cmdFailedf(cmd, "json marshal cluster failed: %s", err)
			}
			bodyReader := bytes.NewReader(dataByte)
			req, err := http.NewRequest("PATCH", url, bodyReader)
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "scale Store failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Scale Store Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Scale Store Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().Int32Var(&storeReplicas, "replicas", 3, "replicas")
	return cmd
}

func scaleTriggerReplicas() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "scale trigger replicas",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				cmdFailedf(cmd, "get operator endpoint failed: %s", err)
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operator_endpoint, BaseUrl)
			cluster := ClusterPatch{
				TriggerReplicas: triggerReplicas,
			}
			dataByte, err := json.Marshal(cluster)
			if err != nil {
				cmdFailedf(cmd, "json marshal cluster failed: %s", err)
			}
			bodyReader := bytes.NewReader(dataByte)
			req, err := http.NewRequest("PATCH", url, bodyReader)
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "scale trigger failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Scale Trigger Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Scale Trigger Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().Int32Var(&triggerReplicas, "replicas", 3, "replicas")
	return cmd
}

func getClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "get cluster status",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				cmdFailedf(cmd, "get operator endpoint failed: %s", err)
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operator_endpoint, BaseUrl)
			req, err := http.NewRequest("GET", url, &bytes.Reader{})
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "get cluster failed: %s", err)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &GetClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Name", "Result"})
				t.AppendRows([]table.Row{
					{"Gateway Endpoints", mustGetGatewayEndpoint(cmd)},
					{"CloudEvents Endpoints", mustGetGatewayCloudEventsEndpoint(cmd)},
					{"Cluster Version", info.Data.Version},
					{"Cluster Status", info.Data.Status},
				})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				})
				t.SetStyle(table.StyleLight)
				t.Style().Options.SeparateRows = true
				t.Style().Box = table.StyleBoxDefault
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	return cmd
}
