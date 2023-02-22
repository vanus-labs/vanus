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
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

var (
	supportedConnectors = []ConnectorSpec{
		{
			Kind:    "source",
			Type:    "http",
			Version: "latest",
		},
		{
			Kind:    "sink",
			Type:    "feishu",
			Version: "latest",
		},
	}
)

type ConnectorSpec struct {
	Kind    string
	Type    string
	Version string
}

type ConnectorCreate struct {
	Config  string `json:"config,omitempty"`
	Kind    string `json:"kind,omitempty"`
	Name    string `json:"name,omitempty"`
	Type    string `json:"type,omitempty"`
	Version string `json:"version,omitempty"`
}

type ConnectorDelete struct {
	Force *bool `json:"force,omitempty"`
}

type ConnectorPatch struct {
	ControllerReplicas int32  `json:"controller_replicas,omitempty"`
	StoreReplicas      int32  `json:"store_replicas,omitempty"`
	TriggerReplicas    int32  `json:"trigger_replicas,omitempty"`
	Version            string `json:"version,omitempty"`
}

type ConnectorInfo struct {
	Kind    string `json:"kind,omitempty"`
	Name    string `json:"name,omitempty"`
	Type    string `json:"type,omitempty"`
	Version string `json:"version,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type GetConnectorOKBody struct {
	Code    *int32         `json:"code"`
	Data    *ConnectorInfo `json:"data"`
	Message *string        `json:"message"`
}

type ListConnectorOKBody struct {
	Code    *int32           `json:"code"`
	Data    []*ConnectorInfo `json:"data"`
	Message *string          `json:"message"`
}

func NewConnectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connector sub-command ",
		Short: "vanus connector operations",
	}
	cmd.AddCommand(installConnectorCommand())
	cmd.AddCommand(uninstallConnectorCommand())
	cmd.AddCommand(listConnectorCommand())
	cmd.AddCommand(getConnectorCommand())
	return cmd
}

func installConnectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install",
		Short: "install a connector",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := getOperatorEndpoint()
			if err != nil {
				operator_endpoint, err = cmd.Flags().GetString("operator-endpoint")
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if showConnectors {
				if IsFormatJSON(cmd) {
					color.Yellow("WARN: this command doesn't support --output-format\n")
				} else {
					t := table.NewWriter()
					t.AppendHeader(table.Row{"Kind", "Type", "Version"})
					t.AppendRows([]table.Row{
						{"Source", "http", "latest"},
						{"Sink", "feishu", "latest"},
					})
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

			if kind == "" {
				cmdFailedf(cmd, "the --kind flag MUST be set")
			}
			if kind != "source" && kind != "sink" {
				cmdFailedf(cmd, "the --kind flag Only support 'source' or 'sink'")
			}
			if name == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}
			if ctype == "" {
				cmdFailedf(cmd, "the --ctype flag MUST be set")
			}
			if connectorConfigFile == "" {
				cmdFailedf(cmd, "the --config-file flag MUST be set")
			}

			if !operatorIsDeployed(cmd, operator_endpoint) {
				cmdFailedWithHelpNotice(cmd, "The vanus operator has not been deployed. Please use the following command to deploy: \n\n    kubectl apply -f https://download.linkall.com/vanus/operator/latest.yml")
			}

			if isUnsupported(kind, ctype, connectorVersion) {
				cmdFailedf(cmd, "Unsupported connector. Please use 'vsctl connector install --list' command to query the list of supported connectors")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors", HttpPrefix, operator_endpoint, BaseUrl)
			data, err := getConfig(connectorConfigFile)
			if err != nil {
				cmdFailedf(cmd, "get config failed, file: %s, err: %s", connectorConfigFile, err)
			}
			connector := ConnectorCreate{
				Config:  data,
				Kind:    kind,
				Name:    name,
				Type:    ctype,
				Version: connectorVersion,
			}
			dataByte, err := json.Marshal(connector)
			if err != nil {
				cmdFailedf(cmd, "json marshal connector failed: %s", err)
			}
			bodyReader := bytes.NewReader(dataByte)
			req, err := http.NewRequest("POST", url, bodyReader)
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "install connector failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Install Connector Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Install Connector Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&connectorVersion, "version", "latest", "connector version")
	cmd.Flags().StringVar(&connectorConfigFile, "config-file", "", "connector config file")
	cmd.Flags().StringVar(&kind, "kind", "", "connector kind, support 'source' or 'sink'")
	cmd.Flags().StringVar(&name, "name", "", "connector name")
	cmd.Flags().StringVar(&ctype, "type", "", "connector type")
	cmd.Flags().BoolVar(&showConnectors, "list", false, "if show all connector of installable")
	return cmd
}

func uninstallConnectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "uninstall a connector",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := getOperatorEndpoint()
			if err != nil {
				operator_endpoint, err = cmd.Flags().GetString("operator-endpoint")
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if name == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors/%s", HttpPrefix, operator_endpoint, BaseUrl, name)
			req, err := http.NewRequest("DELETE", url, &bytes.Reader{})
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "uninstall connector failed: %s", err)
			}
			defer resp.Body.Close()

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Uninstall Connector Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{"Uninstall Connector Success"})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "connector name")
	return cmd
}

func listConnectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list connectors",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := getOperatorEndpoint()
			if err != nil {
				operator_endpoint, err = cmd.Flags().GetString("operator-endpoint")
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if !operatorIsDeployed(cmd, operator_endpoint) {
				cmdFailedWithHelpNotice(cmd, "The vanus operator has not been deployed. Please use the following command to deploy: \n\n    kubectl apply -f https://download.linkall.com/vanus/operator/latest.yml")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors", HttpPrefix, operator_endpoint, BaseUrl)
			req, err := http.NewRequest("GET", url, &bytes.Reader{})
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "get connector failed: %s", err)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ListConnectorOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Kind", "Name", "Type", "Version", "Status", "Reason"})
				for i := range info.Data {
					t.AppendRow(table.Row{
						info.Data[i].Kind,
						info.Data[i].Name,
						info.Data[i].Type,
						info.Data[i].Version,
						info.Data[i].Status,
						info.Data[i].Reason,
					})
				}
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

func getConnectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info",
		Short: "get connector info",
		Run: func(cmd *cobra.Command, args []string) {
			operator_endpoint, err := getOperatorEndpoint()
			if err != nil {
				operator_endpoint, err = cmd.Flags().GetString("operator-endpoint")
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if name == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors/%s", HttpPrefix, operator_endpoint, BaseUrl, name)
			req, err := http.NewRequest("GET", url, &bytes.Reader{})
			if err != nil {
				cmdFailedf(cmd, "new http request failed: %s", err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				cmdFailedf(cmd, "get connector failed: %s", err)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &GetConnectorOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Kind", "Name", "Type", "Version", "Status", "Reason"})
				t.AppendRows([]table.Row{
					{
						info.Data.Kind,
						info.Data.Name,
						info.Data.Type,
						info.Data.Version,
						info.Data.Status,
						info.Data.Reason,
					},
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
	cmd.Flags().StringVar(&name, "name", "", "connector name")
	return cmd
}

func getConfig(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()
	value := bytes.Buffer{}
	br := bufio.NewReader(f)
	for {
		line, _, err := br.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		value.Write(line)
		value.WriteString("\n")
	}
	return value.String(), nil
}

func isUnsupported(kind, ctype, version string) bool {
	for _, c := range supportedConnectors {
		if c.Kind == kind && c.Type == ctype && c.Version == version {
			return false
		}
	}
	return true
}
