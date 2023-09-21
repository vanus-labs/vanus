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
	// standard libraries.
	"encoding/json"
	"errors"
	"runtime"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"
	"golang.org/x/exp/maps"

	// first-party project.
	"github.com/vanus-labs/vanus/pkg/template"
	jt "github.com/vanus-labs/vanus/pkg/template/json"
	tt "github.com/vanus-labs/vanus/pkg/template/text"

	// this project.
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/transform/context"
	"github.com/vanus-labs/vanus/server/trigger/transform/define"
	"github.com/vanus-labs/vanus/server/trigger/transform/pipeline"
)

type Transformer struct {
	define   *define.Define
	pipeline *pipeline.Pipeline
	template template.Template
}

func NewTransformer(transformer *primitive.Transformer) (*Transformer, error) {
	if !transformer.Exist() {
		return nil, nil //nolint:nilnil // nil is valid
	}

	tf := &Transformer{
		define:   define.NewDefine(),
		pipeline: pipeline.NewPipeline(),
	}

	tf.define.Parse(transformer.Define)
	tf.pipeline.Parse(transformer.Pipeline)

	t, err := CompileTemplate(transformer.Template)
	if err != nil {
		return nil, err
	}
	tf.template = t

	return tf, nil
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

	if tf.template != nil {
		model := buildTemplateModel(event, data)
		d, _ := tf.template.Execute(model, defineValue)

		event.DataEncoded = d
		event.SetDataContentType(tf.template.ContentType())
		return nil
	}

	return event.SetData(ce.ApplicationJSON, ceCtx.Data)
}

func buildTemplateModel(event *ce.Event, data any) map[string]any {
	model := map[string]any{
		"id":          event.ID(),
		"source":      event.Source(),
		"specversion": event.SpecVersion(),
		"type":        event.Type(),
	}
	if dataContentType := event.DataContentType(); dataContentType != "" {
		model["datacontenttype"] = dataContentType
	}
	if dataSchema := event.DataSchema(); dataSchema != "" {
		model["dataschema"] = dataSchema
	}
	if subject := event.Subject(); subject != "" {
		model["subject"] = subject
	}
	if time := event.Time(); !time.IsZero() {
		model["time"] = time
	}
	if data != nil {
		model["data"] = data
	}
	if exts := event.Extensions(); len(exts) > 0 {
		maps.Copy(model, exts)
	}
	return model
}

func CompileTemplate(tc primitive.TemplateConfig) (t template.Template, err error) {
	templateType, ok := tc.RecognizeTemplateType()
	if !ok {
		return nil, errors.New("unknown template type")
	}

	switch templateType {
	case primitive.TemplateTypeNone:
		return nil, nil
	case primitive.TemplateTypeText:
		return tt.Compile(tc.Template)
	case primitive.TemplateTypeJSON:
		return jt.Compile(tc.Template)
	default:
		return nil, errors.New("unsupported template type")
	}
}
