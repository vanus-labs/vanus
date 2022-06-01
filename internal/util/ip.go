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

package util

import (
	"net"
	"strconv"
	"strings"
)

func GetLocalIP() string {
	return getInternalIP()
}

func getInternalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func IsValidIPV4Address(str string) bool {
	strs := strings.Split(str, ":")
	if len(strs) > 2 {
		return false
	}
	ip := net.ParseIP(strs[0])
	if ip == nil {
		return false
	}
	if len(strs) == 2 {
		_, err := strconv.Atoi(strs[1])
		if err != nil {
			return false
		}
	}
	return true
}
