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

package command

import (
	"os"

	"github.com/fatih/color"
	"github.com/go-resty/resty/v2"
)

var (
	httpClient = resty.New()
)

func cmdFailedf(format string, a ...interface{}) {
	color.Red(format, a)
	os.Exit(-1)
}

func newHTTPRequest() *resty.Request {
	return httpClient.NewRequest()
}
