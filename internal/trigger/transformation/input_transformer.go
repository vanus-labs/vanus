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
	"github.com/linkall-labs/vanus/internal/trigger/transformation/input"
	"github.com/linkall-labs/vanus/internal/trigger/transformation/template"
	"github.com/linkall-labs/vanus/internal/trigger/transformation/vjson"
	"github.com/linkall-labs/vanus/internal/trigger/util"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
)

type InputTransformer struct {
	InputParser *input.Parser
	Template    *template.Parser
}

func NewInputTransformer(inputTransformer *primitive.InputTransformer) *InputTransformer {
	tf := &InputTransformer{
		InputParser: input.NewParse(),
		Template:    template.NewParser(),
	}
	tf.InputParser.Parse(inputTransformer.InputPath)
	tf.Template.Parse(inputTransformer.InputTemplate)
	return tf
}

func (tf *InputTransformer) Execute(event *ce.Event) error {
	dataMap, err := tf.ParseData(event)
	if err != nil {
		return err
	}
	newData := tf.Template.Execute(dataMap)
	event.DataEncoded = []byte(newData)
	return nil
}

func (tf *InputTransformer) ParseData(event *ce.Event) (map[string]template.Data, error) {
	var results map[string]vjson.Result
	var err error
	if tf.InputParser.HasDataVariable() {
		results, err = vjson.Decode(event.Data())
		if err != nil {
			return nil, err
		}
	}
	dataMap := make(map[string]template.Data)
	for k, n := range tf.InputParser.GetNodes() {
		switch n.Type {
		case input.Constant:
			dataMap[k] = template.NewTextData([]byte(n.Value[0]))
		case input.ContextVariable:
			v, exist := util.LookupAttribute(*event, n.Value[0])
			if !exist {
				dataMap[k] = template.NewNoExistData()
				continue
			}
			s, _ := types.Format(v)
			dataMap[k] = template.NewTextData([]byte(s))
		case input.DataVariable:
			if len(n.Value) == 0 {
				dataMap[k] = template.NewOtherData(event.Data())
			} else {
				dataMap[k] = ParseDataVariable(results, n.Value)
			}
		}
	}
	return dataMap, nil
}

func ParseDataVariable(rs map[string]vjson.Result, keys []string) template.Data {
	length := len(keys)
	for i := 0; i < length; i++ {
		if len(rs) == 0 {
			break
		}
		r, exist := rs[keys[i]]
		if !exist {
			break
		}
		if i == length-1 {
			switch r.Type {
			case vjson.String:
				return template.NewTextData(r.Raw)
			case vjson.Null:
				return template.NewNullData()
			default:
				return template.NewOtherData(r.Raw)
			}
		}
		rs = r.Result
	}
	return template.NewNoExistData()
}
