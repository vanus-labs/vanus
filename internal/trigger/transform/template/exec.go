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

package template

import (
	"bytes"

	ce "github.com/cloudevents/sdk-go/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
)

func (t *Template) Execute(data map[string]interface{}) []byte {
	var sb bytes.Buffer
	stream := jsoniter.ConfigFastest.BorrowStream(&sb)
	defer jsoniter.ConfigFastest.ReturnStream(stream)
	for _, node := range t.parser.getNodes() {
		switch node.Type() {
		case Constant:
			stream.WriteRaw(node.Value())
		case Variable:
			v, exist := data[node.Value()]
			if !exist {
				stream.WriteString("<" + node.Value() + ">")
				continue
			}
			stream.WriteVal(v)
		case StringVariable:
			v, exist := data[node.Value()]
			if !exist {
				stream.WriteRaw("<" + node.Value() + ">")
				continue
			}
			if v == nil {
				continue
			}
			// type string no need quota
			switch val := v.(type) {
			case string:
				stream.WriteRaw(val)
			case []interface{}:
				stream.WriteRaw("[]")
			case map[string]interface{}:
				stream.WriteRaw("{}")
			default:
				stream.WriteVal(v)
			}
		}
	}
	stream.Flush()
	bytes := sb.Bytes()
	if t.contentType == "" {
		if gjson.ValidBytes(bytes) {
			t.contentType = ce.ApplicationJSON
		} else {
			t.contentType = ce.TextPlain
		}
	}
	return bytes
}
