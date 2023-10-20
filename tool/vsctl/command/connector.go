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
	"net/http"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var supportedConnectors = []ConnectorSpec{
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

type ConnectorSpec struct {
	Kind    string
	Type    string
	Version string
}

type ConnectorCreate struct {
	Kind        string                 `json:"kind,omitempty"`
	Name        string                 `json:"name,omitempty"`
	Type        string                 `json:"type,omitempty"`
	Version     string                 `json:"version,omitempty"`
	Config      map[string]interface{} `json:"config,omitempty"`
	Annotations map[string]string      `json:"annotations,omitempty"`
}

type ConnectorDelete struct {
	Force *bool `json:"force,omitempty"`
}

type ConnectorInfo struct {
	Kind    string `json:"kind,omitempty"`
	Name    string `json:"name,omitempty"`
	Type    string `json:"type,omitempty"`
	Version string `json:"version,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type ConnectorOKBody struct {
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
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
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
			if !IsDNS1123Subdomain(name) {
				cmdFailedf(cmd, "invalid format of name: %s\n", dns1123SubdomainErrorMsg)
			}

			if !operatorIsDeployed(cmd, operatorEndpoint) {
				cmdFailedWithHelpNotice(cmd, "The vanus operator has not been deployed. Please use the following command to deploy: \n\n    kubectl apply -f https://dl.vanus.ai/vanus/operator/latest/vanus-operator.yml")
			}

			if isUnsupported(kind, ctype, connectorVersion) {
				cmdFailedf(cmd, "Unsupported connector. Please use 'vsctl connector install --list' command to query the list of supported connectors")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors", HttpPrefix, operatorEndpoint, BaseUrl)
			data, err := getConfig(connectorConfigFile)
			if err != nil {
				cmdFailedf(cmd, "get config failed, file: %s, err: %s", connectorConfigFile, err)
			}
			config := make(map[string]interface{})
			err = yaml.Unmarshal(data, config)
			if err != nil {
				cmdFailedf(cmd, "yaml unmarshal failed: %s", err)
			}
			connector := ConnectorCreate{
				Kind:    kind,
				Name:    name,
				Type:    ctype,
				Version: connectorVersion,
				Config:  config,
			}
			if annotations != "" {
				args := strings.Split(annotations, ",")
				connector.Annotations = make(map[string]string, len(args))
				for idx := range args {
					parts := strings.SplitN(args[idx], "=", 2)
					if len(parts) != 2 {
						cmdFailedf(cmd, "invalid format of annotations: %s\n", err)
					}
					connector.Annotations[parts[0]] = parts[1]
				}
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Install Connector Failed: %s", resp.Status)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ConnectorOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Install Connector Failed: %s", *info.Message)
			}

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
	cmd.Flags().StringVar(&annotations, "annotations", "",
		"connector annotations (e.g. --annotations key1=value1,key2=value2)")
	cmd.Flags().BoolVar(&showConnectors, "list", false, "if show all connector of installable")
	return cmd
}

func uninstallConnectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "uninstall a connector",
		Run: func(cmd *cobra.Command, args []string) {
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if name == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors/%s", HttpPrefix, operatorEndpoint, BaseUrl, name)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Uninstall Connector Failed: %s", resp.Status)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ConnectorOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Uninstall Connector Failed: %s", *info.Message)
			}

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{
					"Result": "Uninstall Connector Success",
				})
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
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if !operatorIsDeployed(cmd, operatorEndpoint) {
				cmdFailedWithHelpNotice(cmd, "The vanus operator has not been deployed. Please use the following command to deploy: \n\n    kubectl apply -f https://dl.vanus.ai/vanus/operator/latest/vanus-operator.yml")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors", HttpPrefix, operatorEndpoint, BaseUrl)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "List Connectors Failed: %s", resp.Status)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ListConnectorOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "List Connector Failed: %s", *info.Message)
			}

			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Kind", "NodeName", "Type", "Version", "Status", "Reason"})
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
			operatorEndpoint, err := cmd.Flags().GetString("operator-endpoint")
			if err != nil {
				operatorEndpoint, err = getOperatorEndpoint()
				if err != nil {
					cmdFailedf(cmd, "get operator endpoint failed: %s", err)
				}
			}

			if name == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}

			client := &http.Client{}
			url := fmt.Sprintf("%s%s%s/connectors/%s", HttpPrefix, operatorEndpoint, BaseUrl, name)
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
			if resp.StatusCode != http.StatusOK {
				cmdFailedf(cmd, "Get Connector Failed: %s", resp.Status)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				cmdFailedf(cmd, "read response body: %s", err)
			}
			info := &ConnectorOKBody{}
			err = json.Unmarshal(body, info)
			if err != nil {
				cmdFailedf(cmd, "json unmarshal failed: %s", err)
			}
			if *info.Code != RespCodeOK {
				cmdFailedf(cmd, "Get Connector Failed: %s", *info.Message)
			}

			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Kind", "NodeName", "Type", "Version", "Status", "Reason"})
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

func getConfig(file string) ([]byte, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		value.Write(line)
		value.WriteString("\n")
	}
	return value.Bytes(), nil
}

func isUnsupported(kind, ctype, version string) bool {
	for _, c := range supportedConnectors {
		if c.Kind == kind && c.Type == ctype && c.Version == version {
			return false
		}
	}
	return true
}
