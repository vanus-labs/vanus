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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	clusterVersionList = []string{"v0.7.0"}
)

type ClusterCreate struct {
	Version            string `json:"version,omitempty"`
	ControllerReplicas int32  `json:"controller_replicas,omitempty"`
	StoreReplicas      int32  `json:"store_replicas,omitempty"`
	GatewayReplicas    int32  `json:"gateway_replicas,omitempty"`
	TriggerReplicas    int32  `json:"trigger_replicas,omitempty"`
	TimerReplicas      int32  `json:"timer_replicas,omitempty"`
	StoreStorageSize   string `json:"store_storage_size,omitempty"`
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

type ClusterOKBody struct {
	Code    *int32       `json:"code"`
	Data    *ClusterInfo `json:"data"`
	Message *string      `json:"message"`
}

type ClusterSpec struct {
	Version    string      `yaml:"version"`
	Controller *Controller `yaml:"controller"`
	Store      *Store      `yaml:"store"`
	Gateway    *Gateway    `yaml:"gateway"`
	Trigger    *Trigger    `yaml:"trigger"`
	Timer      *Timer      `yaml:"timer"`
}

type Controller struct {
	Replicas int32 `yaml:"replicas"`
}

type Store struct {
	Replicas    int32  `yaml:"replicas"`
	StorageSize string `yaml:"storage_size"`
}

type Gateway struct {
	Replicas int32 `yaml:"replicas"`
}

type Trigger struct {
	Replicas int32 `yaml:"replicas"`
}

type Timer struct {
	Replicas int32 `yaml:"replicas"`
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
	cmd.AddCommand(genClusterCommand())
	return cmd
}

func createClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a cluster",
		Run: func(cmd *cobra.Command, args []string) {
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if showInstallableList {
				if IsFormatJSON(cmd) {
					color.Yellow("WARN: this command doesn't support --output-format\n")
				} else {
					t := table.NewWriter()
					t.AppendHeader(table.Row{"Version"})
					row := make([]table.Row, len(clusterVersionList))
					for idx := range clusterVersionList {
						row[idx] = table.Row{clusterVersionList[idx]}
					}
					t.AppendRows(row)
					t.SetColumnConfigs([]table.ColumnConfig{
						{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 3, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					})
					t.SetStyle(table.StyleLight)
					t.Style().Options.SeparateRows = true
					t.Style().Box = table.StyleBoxDefault
					t.SetOutputMirror(os.Stdout)
					t.Render()
				}
				return
			}

			if !operatorIsDeployed(cmd, operatorEndpoint) {
				fmt.Print("You haven't deployed the operator yet, but the vanus cluster is managed by operator. Please confirm whether you want to deploy the operator(y/n):")
				reader := bufio.NewReader(os.Stdin)
				ack, err := reader.ReadString('\n')
				if err != nil {
					cmdFailedf(cmd, "read failed: %s", err)
				}
				ack = strings.ToLower(strings.Trim(ack, "\n"))
				if ack != "y" {
					fmt.Println("Exit operator deployment...")
					return
				}
				fmt.Println("Start deploy operator...")
				operator := exec.Command("kubectl", "apply", "-f",
					"https://dl.vanus.ai/vanus/operator/latest.yml")
				err = operator.Run()
				if err != nil {
					cmdFailedf(cmd, "deploy operator failed: %s", err)
				}
				result := "failure"
				for i := 0; i < retryTime; i++ {
					time.Sleep(time.Second)
					if operatorIsDeployed(cmd, operatorEndpoint) {
						result = "success"
						break
					}
				}
				if result == "failure" {
					cmdFailedf(cmd, "deploy operator not finished.")
				}
				fmt.Println("Deploy operator finish, and then start create vanus cluster...")
			}

			if clusterConfigFile == "" {
				cmdFailedf(cmd, "the --config-file flag MUST be set")
			}

			c := new(ClusterSpec)
			err = LoadConfig(clusterConfigFile, c)
			if err != nil {
				cmdFailedf(cmd, "load cluster config file failed: %s", err)
			}

			clusterspec := table.NewWriter()
			clusterspec.AppendHeader(table.Row{"Cluster", "Version", "Component", "Replicas", "StorageSize"})
			clusterspec.AppendRow(table.Row{"vanus", c.Version, "controller", c.Controller.Replicas, "-"})
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", c.Version, "store", c.Store.Replicas, c.Store.StorageSize})
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", c.Version, "gateway", c.Gateway.Replicas, "-"})
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", c.Version, "trigger", c.Trigger.Replicas, "-"})
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", c.Version, "timer", c.Timer.Replicas, "-"})
			clusterspec.AppendSeparator()
			clusterspec.SetColumnConfigs(clusterColConfigs())
			fmt.Println(clusterspec.Render())
			fmt.Print("The cluster specifications are shown in the above table. Please confirm whether you want to create the cluster(y/n):")
			reader := bufio.NewReader(os.Stdin)
			ack, err := reader.ReadString('\n')
			if err != nil {
				cmdFailedf(cmd, "read failed: %s", err)
			}
			ack = strings.ToLower(strings.Trim(ack, "\n"))
			if ack != "y" {
				fmt.Println("Exit vanus cluster deployment...")
				return
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operatorEndpoint, BaseUrl)
			cluster := ClusterCreate{
				Version:            c.Version,
				ControllerReplicas: c.Controller.Replicas,
				GatewayReplicas:    c.Gateway.Replicas,
				StoreReplicas:      c.Store.Replicas,
				TriggerReplicas:    c.Trigger.Replicas,
				TimerReplicas:      c.Timer.Replicas,
				StoreStorageSize:   c.Store.StorageSize,
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Create Cluster Failed: %s", resp.Status)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Create Cluster Failed: %s", *info.Message)
			}

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
	cmd.Flags().StringVar(&clusterConfigFile, "config-file", "", "cluster config file")
	cmd.Flags().BoolVar(&showInstallableList, "list", false, "if show all version of installable")
	return cmd
}

func deleteClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a cluster",
		Run: func(cmd *cobra.Command, args []string) {
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			fmt.Print("Deleting a cluster will lose all cluster data and can't be recovered, do you still want to delete the vanus cluster(y/n):")
			reader := bufio.NewReader(os.Stdin)
			ack, err := reader.ReadString('\n')
			if err != nil {
				cmdFailedf(cmd, "read failed: %s", err)
			}
			ack = strings.ToLower(strings.Trim(ack, "\n"))
			if ack != "y" {
				fmt.Println("Exit vanus cluster deleting...")
				return
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operatorEndpoint, BaseUrl)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Delete Cluster Failed: %s", resp.Status)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Delete Cluster Failed: %s", *info.Message)
			}

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

			fmt.Print("Do you want to delete the operator(y/n):")
			ack, err = reader.ReadString('\n')
			if err != nil {
				cmdFailedf(cmd, "read failed: %s", err)
			}
			ack = strings.ToLower(strings.Trim(ack, "\n"))
			if ack != "y" {
				return
			}
			fmt.Println("Start delete operator...")
			operator := exec.Command("kubectl", "delete", "-f",
				"https://dl.vanus.ai/vanus/operator/latest.yml")
			err = operator.Run()
			if err != nil {
				cmdFailedf(cmd, "delete operator failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Delete Operator Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Delete Operator Success"})
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
		Short: "upgrade cluster",
		Run: func(cmd *cobra.Command, args []string) {
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if showUpgradeableList {
				info, err := getCluster(cmd, operatorEndpoint)
				if err != nil {
					cmdFailedf(cmd, "get the current version of the cluster failed: %s", err)
				}
				if *info.Code != RespCodeOK {
					cmdFailedf(cmd, "get cluster failed: %s", *info.Message)
				}
				result := getUpgradableVersionList(info.Data.Version)
				if IsFormatJSON(cmd) {
					color.Yellow("WARN: this command doesn't support --output-format\n")
				} else {
					t := table.NewWriter()
					t.AppendHeader(table.Row{"Version"})
					if len(result) == 0 {
						t.AppendRow(table.Row{"No Upgradeable Version"})
					} else {
						row := make([]table.Row, len(result))
						for idx := range result {
							row[idx] = table.Row{result[idx]}
						}
						t.AppendRows(row)
					}
					t.SetColumnConfigs([]table.ColumnConfig{
						{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 3, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					})
					t.SetStyle(table.StyleLight)
					t.Style().Options.SeparateRows = true
					t.Style().Box = table.StyleBoxDefault
					t.SetOutputMirror(os.Stdout)
					t.Render()
				}
				return
			}

			if clusterVersion == "" {
				cmdFailedf(cmd, "the --version flag MUST be set")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operatorEndpoint, BaseUrl)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Upgrade Cluster Failed: %s", resp.Status)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Upgrade Cluster Failed: %s", *info.Message)
			}

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
	cmd.Flags().StringVar(&clusterVersion, "version", "", "cluster version")
	cmd.Flags().BoolVar(&showUpgradeableList, "list", false, "if show all version of upgradeable")

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
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operatorEndpoint, BaseUrl)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Scale Controller Failed: %s", resp.Status)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Scale Controller Failed: %s", *info.Message)
			}

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
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operatorEndpoint, BaseUrl)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Scale Store Failed: %s", resp.Status)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Scale Store Failed: %s", *info.Message)
			}

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
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operatorEndpoint, BaseUrl)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Scale Trigger Failed: %s", resp.Status)
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Scale Trigger Failed: %s", *info.Message)
			}

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
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, operatorEndpoint, BaseUrl)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Get Cluster Failed: %s", resp.Status)
			}

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ClusterOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Get Cluster Failed: %s", *info.Message)
			}

			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"NodeName", "Result"})
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

func genClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "generate cluster config file template",
		Run: func(cmd *cobra.Command, args []string) {
			cluster := &ClusterSpec{
				Version: "v0.7.0",
				Controller: &Controller{
					Replicas: 3,
				},
				Store: &Store{
					Replicas:    3,
					StorageSize: "10Gi",
				},
				Gateway: &Gateway{
					Replicas: 1,
				},
				Trigger: &Trigger{
					Replicas: 1,
				},
				Timer: &Timer{
					Replicas: 2,
				},
			}
			data, err := yaml.Marshal(cluster)
			if err != nil {
				cmdFailedf(cmd, "yaml marshal failed: %s", err)
			}

			fileName := "cluster.yaml.example"
			err = ioutil.WriteFile(fileName, data, 0o644)
			if err != nil {
				cmdFailedf(cmd, "generate cluster config file template failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "Output"})
				t.AppendRow(table.Row{"Generate Cluster Config File Template Success", "cluster.yaml.example"})
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

func getCluster(cmd *cobra.Command, endpoint string) (*ClusterOKBody, error) {
	client := &http.Client{}
	url := fmt.Sprintf("%s%s%s/cluster", HttpPrefix, endpoint, BaseUrl)
	req, err := http.NewRequest("GET", url, &bytes.Reader{})
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	info := &ClusterOKBody{}
	err = json.Unmarshal(body, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func clusterColConfigs() []table.ColumnConfig {
	return []table.ColumnConfig{
		{Number: 1, AutoMerge: true, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 2, AutoMerge: true, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 3, AutoMerge: false, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 4, AutoMerge: false, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 5, AutoMerge: false, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
	}
}

func getUpgradableVersionList(curVersion string) []string {
	var curIdx int
	for idx := range clusterVersionList {
		if strings.Compare(curVersion, clusterVersionList[idx]) == 0 {
			curIdx = idx
			break
		}
	}
	return clusterVersionList[curIdx+1:]
}
