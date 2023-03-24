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
	"os/exec"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/vanus-labs/vanus/pkg/errors"
)

const (
	dns1123LabelFmt          string = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"
	dns1123SubdomainFmt      string = dns1123LabelFmt + "(\\." + dns1123LabelFmt + ")*"
	dns1123SubdomainErrorMsg string = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character"
	// DNS1123SubdomainMaxLength is a subdomain's max length in DNS (RFC 1123)
	DNS1123SubdomainMaxLength int = 253
)

var dns1123SubdomainRegexp = regexp.MustCompile("^" + dns1123SubdomainFmt + "$")

func cmdFailedf(cmd *cobra.Command, format string, a ...interface{}) {
	errStr := format
	if a != nil {
		errStr = fmt.Sprintf(format, a...)
	}
	if IsFormatJSON(cmd) {
		m := map[string]string{"ERROR": errStr}
		data, _ := json.Marshal(m)
		color.Red(string(data))
	} else {
		t := table.NewWriter()
		t.AppendHeader(table.Row{"ERROR"})
		t.AppendRow(table.Row{errStr})
		t.SetColumnConfigs([]table.ColumnConfig{
			{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
			{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		})
		t.SetOutputMirror(os.Stdout)
		t.Render()
	}

	os.Exit(-1)
}

func cmdFailedWithHelpNotice(cmd *cobra.Command, format string) {
	color.White(format)
	color.Cyan("\n============ see below for right usage ============\n\n")
	_ = cmd.Help()
	os.Exit(-1)
}

func operatorIsDeployed(cmd *cobra.Command, endpoint string) bool {
	client := &http.Client{}
	url := fmt.Sprintf("%s%s%s/healthz", HttpPrefix, endpoint, BaseUrl)
	req, err := http.NewRequest("GET", url, &bytes.Reader{})
	if err != nil {
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return true
}

func getOperatorEndpoint() (string, error) {
	nodeip, err := exec.Command("bash", "-c", "kubectl get no --no-headers -o wide | awk '{print $6}' | head -n 1").Output()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", strings.Trim(string(nodeip), "\n"), DefaultOperatorPort), nil
}

func LoadConfig(filename string, config interface{}) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	str := os.ExpandEnv(string(b))
	err = yaml.Unmarshal([]byte(str), config)
	if err != nil {
		return err
	}
	return nil
}

// IsDNS1123Subdomain tests for a string that conforms to the definition of a
// subdomain in DNS (RFC 1123).
func IsDNS1123Subdomain(value string) bool {
	if len(value) > DNS1123SubdomainMaxLength {
		return false
	}
	if !dns1123SubdomainRegexp.MatchString(value) {
		return false
	}
	return true
}

func Error(err error) string {
	e, _ := errors.FromError(err)
	if e.Message == "" {
		return e.Description
	}
	return e.Message
}
