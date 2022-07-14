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

package transformation

import (
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/transformation/define"
	"github.com/linkall-labs/vanus/internal/trigger/transformation/template"
	"github.com/linkall-labs/vanus/internal/trigger/util"
	"github.com/tidwall/gjson"

	ce "github.com/cloudevents/sdk-go/v2"
)

type InputTransformer struct {
	define   *define.Parser
	template *template.Parser
}

func NewInputTransformer(inputTransformer *primitive.InputTransformer) *InputTransformer {
	tf := &InputTransformer{
		define:   define.NewParse(),
		template: template.NewParser(),
	}
	tf.define.Parse(inputTransformer.Define)
	tf.template.Parse(inputTransformer.Template)
	return tf
}

func (tf *InputTransformer) Execute(event *ce.Event) error {
	dataMap := tf.parseData(event)
	newData := tf.template.Execute(dataMap)
	switch tf.template.OutputType {
	case template.JSON:
		event.SetDataContentType(ce.ApplicationJSON)
	default:
		event.SetDataContentType("")
	}
	event.DataEncoded = []byte(newData)
	return nil
}

func (tf *InputTransformer) parseData(event *ce.Event) map[string]template.Data {
	dataMap := make(map[string]template.Data)
	for k, n := range tf.define.GetNodes() {
		switch n.Type {
		case define.Constant:
			dataMap[k] = template.NewTextData([]byte(n.Value))
		case define.ContextVariable:
			v, exist := util.LookupAttribute(*event, n.Value)
			if !exist {
				dataMap[k] = template.NewNullData()
				continue
			}
			dataMap[k] = template.NewTextData([]byte(v))
		case define.DataVariable:
			if n.Value == "" {
				dataMap[k] = template.NewOtherData(event.Data())
			} else {
				dataMap[k] = parseDataVariable(event.Data(), n.Value)
			}
		}
	}
	return dataMap
}

func parseDataVariable(json []byte, path string) template.Data {
	r := gjson.GetBytes(json, path)
	switch r.Type {
	case gjson.Null:
		return template.NewNullData()
	case gjson.String:
		return template.NewTextData([]byte(r.Str))
	default:
		return template.NewOtherData([]byte(r.Raw))
	}
}
