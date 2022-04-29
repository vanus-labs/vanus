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
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/transformation/input"
	"github.com/linkall-labs/vanus/internal/trigger/transformation/template"
	"github.com/linkall-labs/vanus/internal/trigger/transformation/vjson"
	"github.com/linkall-labs/vanus/internal/trigger/util"
)

type Transformation struct {
	InputParser *input.Parser
	Template    *template.Parser
}

func NewTransformation(inputTransformer primitive.InputTransformer) *Transformation {
	tf := &Transformation{
		InputParser: input.NewParse(),
		Template:    template.NewParser(),
	}
	tf.InputParser.Parse(inputTransformer.InputPath)
	tf.Template.Parse(inputTransformer.InputTemplate)
	return tf
}

func (tf *Transformation) Execute(event *ce.Event) error {
	dataMap, err := tf.parseData(event)
	if err != nil {
		return err
	}
	newData := tf.Template.Execute(dataMap)
	event.DataEncoded = []byte(newData)
	return nil
}

func (tf *Transformation) parseData(event *ce.Event) (map[string][]byte, error) {
	var results map[string]vjson.Result
	var err error
	if tf.InputParser.HasDataVariable() {
		results, err = vjson.Decode(event.Data())
		if err != nil {
			return nil, err
		}
	}
	dataMap := make(map[string][]byte)
	for k, n := range tf.InputParser.GetNodes() {
		switch n.Type {
		case input.Constant:
			dataMap[k] = []byte(n.Value[0])
		case input.ContextVariable:
			v, exist := util.LookupAttribute(*event, k)
			if !exist {
				dataMap[k] = []byte{}
				continue
			}
			s, _ := types.Format(v)
			dataMap[k] = []byte(s)
		case input.DataVariable:
			if len(n.Value) == 0 {
				dataMap[k] = event.Data()
			} else {
				rs := results
				var v []byte
				length := len(n.Value)
				for i := 0; i < length; i++ {
					r, exist := rs[n.Value[i]]
					if !exist {
						break
					}
					if i == length-1 {
						if r.Type != vjson.Null {
							v = r.Raw
						}
						break
					}
					rs = r.Result
					if len(rs) == 0 {
						break
					}
				}
				dataMap[k] = v
			}
		}
	}
	return dataMap, nil
}
