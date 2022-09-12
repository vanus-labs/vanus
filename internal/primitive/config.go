// Copyright 2022 Linkall Inc.
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

package primitive

import (
	"io/ioutil"
	"os"

	"github.com/linkall-labs/vanus/internal/primitive/credential"

	"gopkg.in/yaml.v3"
)

type KvStorageConfig struct {
	KeyPrefix  string   `yaml:"key_prefix" json:"key_prefix"`
	ServerList []string `yaml:"server_list" json:"server_list"`
}

type TLSConfig struct {
	CertFile       string `yaml:"cert_file" json:"cert_file"`
	KeyFile        string `yaml:"key_file" json:"key_file"`
	ClientCertFile string `yaml:"client_cert_file" json:"client_cert_file"`
	ClientKeyFile  string `yaml:"client_key_file" json:"client_key_file"`
	TrustedCAFile  string `yaml:"trusted_ca_file" json:"trusted_ca_file"`
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

func ConvertTLSInfo(cfg TLSConfig) credential.TLSInfo {
	tlsInfo := credential.TLSInfo{
		CertFile:       cfg.CertFile,
		KeyFile:        cfg.KeyFile,
		ClientCertFile: cfg.ClientKeyFile,
		ClientKeyFile:  cfg.ClientKeyFile,
		TrustedCAFile:  cfg.TrustedCAFile,
	}
	return tlsInfo
}
