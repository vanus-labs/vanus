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

package main

import (
	"context"
	"fmt"
	"github.com/linkall-labs/vanus/client"
)

func main() {
	vrn := fmt.Sprintf("vanus://%s/eventlog/%s?namespace=vanus", "127.0.0.1:2048", "df5aff09-9242-4b47-b9df-cf07c5868e3a")
	r, err := client.OpenLogReader(vrn)
	if err != nil {
		panic(err)
	}
	_, _ = r.Seek(context.Background(), 20, 0)
	es, er := r.Read(context.Background(), 100)
	if err != nil {
		panic(er)
	}
	for i, v := range es {
		println(i, string(v.Data()))
	}
}
