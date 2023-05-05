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
)

const (
	DefaultInitialVersion    = "v0.8.0"
	DefaultImagePullPolicy   = "Always"
	DefaultResourceLimitsCpu = "500m"
	DefaultResourceLimitsMem = "1Gi"
)

var (
	clusterVersionList = []string{DefaultInitialVersion}
)

type ClusterCreate struct {
	Version     string            `json:"version,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type ClusterDelete struct {
	Force *bool `json:"force,omitempty"`
}

type ClusterPatch struct {
	Version     string            `json:"version,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
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
	Version         *string     `yaml:"version"`
	ImagePullPolicy *string     `yaml:"image_pull_policy"`
	Etcd            *Etcd       `yaml:"etcd"`
	Controller      *Controller `yaml:"controller"`
	Store           *Store      `yaml:"store"`
	Gateway         *Gateway    `yaml:"gateway"`
	Trigger         *Trigger    `yaml:"trigger"`
	Timer           *Timer      `yaml:"timer"`
}

type Etcd struct {
	Ports        *EtcdPorts `yaml:"ports"`
	Replicas     *int32     `yaml:"replicas"`
	StorageSize  *string    `yaml:"storage_size"`
	StorageClass *string    `yaml:"storage_class"`
	Resources    *Resources `yaml:"resources"`
}

type Controller struct {
	Ports           *ControllerPorts `yaml:"ports"`
	Replicas        *int32           `yaml:"replicas"`
	SegmentCapacity *string          `yaml:"segment_capacity"`
	Resources       *Resources       `yaml:"resources"`
}

type Store struct {
	Replicas     *int32     `yaml:"replicas"`
	StorageSize  *string    `yaml:"storage_size"`
	StorageClass *string    `yaml:"storage_class"`
	Resources    *Resources `yaml:"resources"`
}

type Gateway struct {
	Ports     *GatewayPorts     `yaml:"ports"`
	NodePorts *GatewayNodePorts `yaml:"nodeports"`
	Replicas  *int32            `yaml:"replicas"`
	Resources *Resources        `yaml:"resources"`
}

type Trigger struct {
	Replicas  *int32     `yaml:"replicas"`
	Resources *Resources `yaml:"resources"`
}

type Timer struct {
	Replicas    *int32       `yaml:"replicas"`
	TimingWheel *TimingWheel `yaml:"timingwheel"`
	Resources   *Resources   `yaml:"resources"`
}

type EtcdPorts struct {
	Client *int32 `yaml:"client"`
	Peer   *int32 `yaml:"peer"`
}

type ControllerPorts struct {
	Controller     *int32 `yaml:"controller"`
	RootController *int32 `yaml:"root_controller"`
}

type GatewayPorts struct {
	Proxy       *int32 `yaml:"proxy"`
	CloudEvents *int32 `yaml:"cloudevents"`
	SinkProxy   *int32 `yaml:"sink_proxy"`
}

type GatewayNodePorts struct {
	Proxy       *int32 `yaml:"proxy"`
	CloudEvents *int32 `yaml:"cloudevents"`
}

type TimingWheel struct {
	Tick   *int32 `yaml:"tick"`
	Size   *int32 `yaml:"wheel_size"`
	Layers *int32 `yaml:"layers"`
}

type Resources struct {
	LimitsCpu *string `yaml:"limits_cpu"`
	LimitsMem *string `yaml:"limits_mem"`
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
					"https://dl.vanus.ai/vanus/operator/latest/vanus-operator.yml")
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

			if !clusterIsVaild(c) {
				cmdFailedf(cmd, "cluster config invaild")
			}

			clusterspec := table.NewWriter()
			clusterspec.AppendHeader(table.Row{"Cluster", "Version", "Component", "Replicas", "StorageSize", "StorageClass"})
			if c.Etcd.StorageClass == nil {
				clusterspec.AppendRow(table.Row{"vanus", *c.Version, "etcd", *c.Etcd.Replicas, *c.Etcd.StorageSize, "-"})
			} else {
				clusterspec.AppendRow(table.Row{"vanus", *c.Version, "etcd", *c.Etcd.Replicas, *c.Etcd.StorageSize, *c.Etcd.StorageClass})
			}
			clusterspec.AppendSeparator()
			if c.Store.StorageClass == nil {
				clusterspec.AppendRow(table.Row{"vanus", *c.Version, "store", *c.Store.Replicas, *c.Store.StorageSize, "-"})
			} else {
				clusterspec.AppendRow(table.Row{"vanus", *c.Version, "store", *c.Store.Replicas, *c.Store.StorageSize, *c.Store.StorageClass})
			}
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", *c.Version, "controller", *c.Controller.Replicas, "-", "-"})
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", *c.Version, "gateway", *c.Gateway.Replicas, "-", "-"})
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", *c.Version, "trigger", *c.Trigger.Replicas, "-", "-"})
			clusterspec.AppendSeparator()
			clusterspec.AppendRow(table.Row{"vanus", *c.Version, "timer", *c.Timer.Replicas, "-", "-"})
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
				Version:     *c.Version,
				Annotations: genClusterAnnotations(c),
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
	cmd.Flags().StringVar(&clusterVersion, "version", DefaultInitialVersion, "cluster version")
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
				"https://dl.vanus.ai/vanus/operator/latest/vanus-operator.yml")
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
	cmd.AddCommand(scaleStoreReplicas())
	cmd.AddCommand(scaleTriggerReplicas())
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
			annotations := make(map[string]string)
			annotations[CoreComponentStoreReplicasAnnotation] = fmt.Sprintf("%d", storeReplicas)
			cluster := ClusterPatch{
				Annotations: annotations,
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
			annotations := make(map[string]string)
			annotations[CoreComponentTriggerReplicasAnnotation] = fmt.Sprintf("%d", triggerReplicas)
			cluster := ClusterPatch{
				Annotations: annotations,
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
			template := bytes.Buffer{}
			template.WriteString(fmt.Sprintf("version: %s\n", DefaultInitialVersion))
			template.WriteString("# Image pull policy, one of Always, Never, IfNotPresent. Defaults to Always.\n")
			template.WriteString(fmt.Sprintf("# image_pull_policy: %s\n", DefaultImagePullPolicy))
			template.WriteString("etcd:\n")
			template.WriteString("  # etcd service ports\n")
			template.WriteString("  ports:\n")
			template.WriteString("    client: 2379\n")
			template.WriteString("    peer: 2380\n")
			template.WriteString("  # etcd replicas is 3 by default, modification not supported\n")
			template.WriteString("  replicas: 3\n")
			template.WriteString("  # etcd storage size is 10Gi by default, supports both Gi and Mi units\n")
			template.WriteString("  storage_size: 10Gi\n")
			template.WriteString("  # specify the pvc storageclass of the etcd, use the cluster default storageclass by default\n")
			template.WriteString("  # storage_class: gp3\n")
			template.WriteString("  # resources:\n")
			template.WriteString(fmt.Sprintf("    # limits_cpu: %s\n", DefaultResourceLimitsCpu))
			template.WriteString(fmt.Sprintf("    # limits_mem: %s\n", DefaultResourceLimitsMem))
			template.WriteString("controller:\n")
			template.WriteString("  # controller service ports\n")
			template.WriteString("  ports:\n")
			template.WriteString("    controller: 2048\n")
			template.WriteString("    root_controller: 2021\n")
			template.WriteString("  # controller replicas is 2 by default, modification not supported\n")
			template.WriteString("  replicas: 2\n")
			template.WriteString("  # segment capacity is 64Mi by default, supports both Gi and Mi units\n")
			template.WriteString("  segment_capacity: 64Mi\n")
			template.WriteString("  # resources:\n")
			template.WriteString(fmt.Sprintf("    # limits_cpu: %s\n", DefaultResourceLimitsCpu))
			template.WriteString(fmt.Sprintf("    # limits_mem: %s\n", DefaultResourceLimitsMem))
			template.WriteString("store:\n")
			template.WriteString("  replicas: 3\n")
			template.WriteString("  # store storage size is 10Gi by default, supports both Gi and Mi units\n")
			template.WriteString("  storage_size: 10Gi\n")
			template.WriteString("  # specify the pvc storageclass of the store, use the cluster default storageclass by default\n")
			template.WriteString("  # storage_class: io2\n")
			template.WriteString("  # resources:\n")
			template.WriteString(fmt.Sprintf("    # limits_cpu: %s\n", DefaultResourceLimitsCpu))
			template.WriteString(fmt.Sprintf("    # limits_mem: %s\n", DefaultResourceLimitsMem))
			template.WriteString("gateway:\n")
			template.WriteString("  # gateway service ports\n")
			template.WriteString("  # gateway.ports.cloudevents specify the cloudevents port, the default value is gateway.ports.proxy+1 and customization is not supported\n")
			template.WriteString("  ports:\n")
			template.WriteString("    proxy: 8080\n")
			template.WriteString("    cloudevents: 8081\n")
			template.WriteString("  nodeports:\n")
			template.WriteString("    proxy: 30001\n")
			template.WriteString("    cloudevents: 30002\n")
			template.WriteString("  # gateway replicas is 1 by default, modification not supported\n")
			template.WriteString("  replicas: 1\n")
			template.WriteString("  # resources:\n")
			template.WriteString(fmt.Sprintf("    # limits_cpu: %s\n", DefaultResourceLimitsCpu))
			template.WriteString(fmt.Sprintf("    # limits_mem: %s\n", DefaultResourceLimitsMem))
			template.WriteString("trigger:\n")
			template.WriteString("  replicas: 1\n")
			template.WriteString("  # resources:\n")
			template.WriteString(fmt.Sprintf("    # limits_cpu: %s\n", DefaultResourceLimitsCpu))
			template.WriteString(fmt.Sprintf("    # limits_mem: %s\n", DefaultResourceLimitsMem))
			template.WriteString("timer:\n")
			template.WriteString("  # timer replicas is 2 by default, modification not supported\n")
			template.WriteString("  replicas: 2\n")
			template.WriteString("  timingwheel:\n")
			template.WriteString("    tick: 1\n")
			template.WriteString("    wheel_size: 32\n")
			template.WriteString("    layers: 4\n")
			template.WriteString("  # resources:\n")
			template.WriteString(fmt.Sprintf("    # limits_cpu: %s\n", DefaultResourceLimitsCpu))
			template.WriteString(fmt.Sprintf("    # limits_mem: %s\n", DefaultResourceLimitsMem))
			fileName := "cluster.yaml.example"
			err := ioutil.WriteFile(fileName, template.Bytes(), 0o644)
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
		{Number: 6, AutoMerge: false, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
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

func clusterIsVaild(c *ClusterSpec) bool {
	return c.Version != nil
}

func genClusterAnnotations(c *ClusterSpec) map[string]string {
	annotations := make(map[string]string)
	if c.ImagePullPolicy != nil {
		annotations[CoreComponentImagePullPolicyAnnotation] = *c.ImagePullPolicy
	}
	// Etcd
	annotations[CoreComponentEtcdReplicasAnnotation] = fmt.Sprintf("%d", *c.Etcd.Replicas)
	annotations[CoreComponentEtcdStorageSizeAnnotation] = *c.Etcd.StorageSize
	annotations[CoreComponentEtcdPortClientAnnotation] = fmt.Sprintf("%d", *c.Etcd.Ports.Client)
	annotations[CoreComponentEtcdPortPeerAnnotation] = fmt.Sprintf("%d", *c.Etcd.Ports.Peer)
	if c.Etcd.StorageClass != nil {
		annotations[CoreComponentEtcdStorageClassAnnotation] = *c.Etcd.StorageClass
	}
	if c.Etcd.Resources != nil {
		if c.Etcd.Resources.LimitsCpu != nil {
			annotations[CoreComponentEtcdResourceLimitsCpuAnnotation] = *c.Etcd.Resources.LimitsCpu
		}
		if c.Etcd.Resources.LimitsMem != nil {
			annotations[CoreComponentEtcdResourceLimitsMemAnnotation] = *c.Etcd.Resources.LimitsMem
		}
	}
	// Controller
	annotations[CoreComponentControllerSvcPortAnnotation] = fmt.Sprintf("%d", *c.Controller.Ports.Controller)
	annotations[CoreComponentRootControllerSvcPortAnnotation] = fmt.Sprintf("%d", *c.Controller.Ports.RootController)
	annotations[CoreComponentControllerSegmentCapacityAnnotation] = *c.Controller.SegmentCapacity
	if c.Controller.Resources != nil {
		if c.Controller.Resources.LimitsCpu != nil {
			annotations[CoreComponentControllerResourceLimitsCpuAnnotation] = *c.Controller.Resources.LimitsCpu
		}
		if c.Controller.Resources.LimitsMem != nil {
			annotations[CoreComponentControllerResourceLimitsMemAnnotation] = *c.Controller.Resources.LimitsMem
		}
	}
	// Store
	annotations[CoreComponentStoreReplicasAnnotation] = fmt.Sprintf("%d", *c.Store.Replicas)
	annotations[CoreComponentStoreStorageSizeAnnotation] = *c.Store.StorageSize
	if c.Store.StorageClass != nil {
		annotations[CoreComponentStoreStorageClassAnnotation] = *c.Store.StorageClass
	}
	if c.Store.Resources != nil {
		if c.Store.Resources.LimitsCpu != nil {
			annotations[CoreComponentStoreResourceLimitsCpuAnnotation] = *c.Store.Resources.LimitsCpu
		}
		if c.Store.Resources.LimitsMem != nil {
			annotations[CoreComponentStoreResourceLimitsMemAnnotation] = *c.Store.Resources.LimitsMem
		}
	}
	// Gateway
	annotations[CoreComponentGatewayPortProxyAnnotation] = fmt.Sprintf("%d", *c.Gateway.Ports.Proxy)
	annotations[CoreComponentGatewayPortCloudEventsAnnotation] = fmt.Sprintf("%d", *c.Gateway.Ports.CloudEvents)
	annotations[CoreComponentGatewayNodePortProxyAnnotation] = fmt.Sprintf("%d", *c.Gateway.NodePorts.Proxy)
	annotations[CoreComponentGatewayNodePortCloudEventsAnnotation] = fmt.Sprintf("%d", *c.Gateway.NodePorts.CloudEvents)
	if c.Gateway.Resources != nil {
		if c.Gateway.Resources.LimitsCpu != nil {
			annotations[CoreComponentGatewayResourceLimitsCpuAnnotation] = *c.Gateway.Resources.LimitsCpu
		}
		if c.Gateway.Resources.LimitsMem != nil {
			annotations[CoreComponentGatewayResourceLimitsMemAnnotation] = *c.Gateway.Resources.LimitsMem
		}
	}
	// Trigger
	annotations[CoreComponentTriggerReplicasAnnotation] = fmt.Sprintf("%d", *c.Trigger.Replicas)
	if c.Trigger.Resources != nil {
		if c.Trigger.Resources.LimitsCpu != nil {
			annotations[CoreComponentTriggerResourceLimitsCpuAnnotation] = *c.Trigger.Resources.LimitsCpu
		}
		if c.Trigger.Resources.LimitsMem != nil {
			annotations[CoreComponentTriggerResourceLimitsMemAnnotation] = *c.Trigger.Resources.LimitsMem
		}
	}
	// Timer
	annotations[CoreComponentTimerTimingWheelTickAnnotation] = fmt.Sprintf("%d", *c.Timer.TimingWheel.Tick)
	annotations[CoreComponentTimerTimingWheelSizeAnnotation] = fmt.Sprintf("%d", *c.Timer.TimingWheel.Size)
	annotations[CoreComponentTimerTimingWheelLayersAnnotation] = fmt.Sprintf("%d", *c.Timer.TimingWheel.Layers)
	if c.Timer.Resources != nil {
		if c.Timer.Resources.LimitsCpu != nil {
			annotations[CoreComponentTimerResourceLimitsCpuAnnotation] = *c.Timer.Resources.LimitsCpu
		}
		if c.Timer.Resources.LimitsMem != nil {
			annotations[CoreComponentTimerResourceLimitsMemAnnotation] = *c.Timer.Resources.LimitsMem
		}
	}
	return annotations
}
