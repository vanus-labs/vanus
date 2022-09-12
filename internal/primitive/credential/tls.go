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

package credential

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type TLSInfo struct {
	// CertFile is the _server_ cert, it will also be used as a _client_ certificate if ClientCertFile is empty
	CertFile string
	// KeyFile is the key for the CertFile
	KeyFile string
	// ClientCertFile is a _client_ cert for initiating connections when ClientCertAuth is defined. If ClientCertAuth
	// is true but this value is empty, the CertFile will be used instead.
	ClientCertFile string
	// ClientKeyFile is the key for the ClientCertFile
	ClientKeyFile string

	TrustedCAFile string
}

func (info TLSInfo) Empty() bool {
	return info.CertFile == "" && info.KeyFile == ""
}

func (info TLSInfo) Validate() error {
	if info.Empty() {
		return nil
	}
	if info.KeyFile == "" || info.CertFile == "" {
		return fmt.Errorf("KeyFile and CertFile must both be present[key: %v, cert: %v]", info.KeyFile, info.CertFile)
	}
	_, err := tls.LoadX509KeyPair(info.CertFile, info.KeyFile)
	if err != nil {
		return err
	}
	if (info.ClientKeyFile == "") != (info.ClientCertFile == "") {
		return fmt.Errorf("ClientKeyFile and ClientCertFile must both be present or both absent: "+
			"key: %v, cert: %v]", info.ClientKeyFile, info.ClientCertFile)
	}
	if info.ClientCertFile != "" {
		_, err = tls.LoadX509KeyPair(info.ClientCertFile, info.ClientKeyFile)
		if err != nil {
			return err
		}
	}
	if info.TrustedCAFile != "" {
		_, err = newCert(info.TrustedCAFile)
		if err != nil {
			return err
		}
	}
	return nil
}

// ServerConfig generates a tls.Config object for use by server.
func (info TLSInfo) ServerConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if info.TrustedCAFile != "" {
		cp, err := newCert(info.TrustedCAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = cp
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	cert, err := tls.LoadX509KeyPair(info.CertFile, info.KeyFile)
	if err != nil {
		return nil, err
	}
	cfg.Certificates = []tls.Certificate{cert}
	return cfg, nil
}

// ClientConfig generates a tls.Config object for use by client.
func (info TLSInfo) ClientConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	}
	if info.TrustedCAFile != "" {
		cp, err := newCert(info.TrustedCAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = cp
	}
	certFile, keyFile := info.CertFile, info.KeyFile
	if info.ClientCertFile != "" {
		certFile, keyFile = info.ClientCertFile, info.ClientKeyFile
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg.Certificates = []tls.Certificate{cert}
	return cfg, nil
}

func newCert(certFile string) (*x509.CertPool, error) {
	b, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, err
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("credentials: failed to append certificates")
	}
	return cp, nil
}
