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

package transform

import (
	"encoding/json"
	"runtime"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/trigger/transform/define"
	"github.com/linkall-labs/vanus/internal/trigger/transform/pipeline"
	"github.com/linkall-labs/vanus/internal/trigger/transform/template"
	"github.com/pkg/errors"
)

type Transformer struct {
	define   *define.Define
	pipeline *pipeline.Pipeline
	template *template.Template
}

func NewTransformer(transformer *primitive.Transformer) *Transformer {
	if !transformer.Exist() {
		return nil
	}
	tf := &Transformer{
		define:   define.NewDefine(),
		pipeline: pipeline.NewPipeline(),
		template: template.NewTemplate(),
	}
	tf.define.Parse(transformer.Define)
	tf.pipeline.Parse(transformer.Pipeline)
	tf.template.Parse(transformer.Template)
	return tf
}

func (tf *Transformer) Execute(event *ce.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			size := 1024
			stacktrace := make([]byte, size)
			stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
			err = errors.New(string(stacktrace))
		}
	}()

	var data interface{}
	err = json.Unmarshal(event.Data(), &data)
	if err != nil {
		return err
	}
	ceCtx := &context.EventContext{
		Event: event,
		Data:  data,
	}
	defineValue, err := tf.define.EvaluateValue(ceCtx)
	if err != nil {
		return err
	}
	ceCtx.Define = defineValue
	err = tf.pipeline.Run(ceCtx)
	if err != nil {
		return err
	}
	if tf.template.Exist() {
		d := tf.template.Execute(ceCtx)
		event.DataEncoded = d
		event.SetDataContentType(tf.template.ContentType())
		return nil
	}
	return event.SetData(ce.ApplicationJSON, ceCtx.Data)
}
