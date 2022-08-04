// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
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
	eb "github.com/linkall-labs/vanus/client"
	"io"
	"log"
)

func main() {
	ls, err := eb.LookupReadableLogs(context.Background(), "vanus:///eventbus/test?controllers=localhost:2048")
	if err != nil {
		log.Fatal(err)
	}

	r, err := eb.OpenLogReader(ls[0].VRN)
	if err != nil {
		log.Fatal(err)
	}

	_, err = r.Seek(context.Background(), 0, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	idx := 0
	for {
		events, err := r.Read(context.Background(), 5)
		if err != nil {
			log.Printf("%s", err)
		}

		if len(events) == 0 {
			log.Println("no more events")
			continue
		}

		for _, e := range events {
			fmt.Sprintf("%s", e)
			log.Printf("event %d: \n", idx)
			idx++
		}
	}

	r.Close()
}
