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

package convert

import (
	// standard libraries.
	"time"

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	// first-party libraries.
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
)

type ceWrapper struct {
	e block.Entry
}

func (w *ceWrapper) ID() string {
	return w.e.GetString(ceschema.IDOrdinal)
}

func (w *ceWrapper) Source() string {
	return w.e.GetString(ceschema.SourceOrdinal)
}

func (w *ceWrapper) SpecVersion() string {
	return w.e.GetString(ceschema.SpecVersionOrdinal)
}

func (w *ceWrapper) Type() string {
	return w.e.GetString(ceschema.TypeOrdinal)
}

func (w *ceWrapper) DataContentType() string {
	return w.e.GetString(ceschema.DataContentTypeOrdinal)
}

func (w *ceWrapper) DataSchema() string {
	return w.e.GetString(ceschema.DataSchemaOrdinal)
}

func (w *ceWrapper) Subject() string {
	return w.e.GetString(ceschema.SubjectOrdinal)
}

func (w *ceWrapper) Time() time.Time {
	return w.e.GetTime(ceschema.TimeOrdinal)
	// str := w.e.GetString(ceschema.TimeOrdinal)
	// if str == "" {
	// 	return time.Time{}
	// }
	// t, _ := time.Parse(time.RFC3339, str)
	// return t
}

func (w *ceWrapper) Extension(ext []byte) []byte {
	return w.e.GetExtensionAttribute(ext)
}

func (w *ceWrapper) Data() []byte {
	return w.e.GetBytes(ceschema.DataOrdinal)
}

func ToPb(e block.Entry) *cepb.CloudEvent {
	var event cepb.CloudEvent

	w := ceWrapper{e: e}
	event.Id = w.ID()
	event.Source = w.Source()
	event.SpecVersion = w.SpecVersion()
	event.Type = w.Type()

	event.Attributes = make(map[string]*cepb.CloudEventAttributeValue)
	if s := w.DataContentType(); s != "" {
		event.Attributes[dataContentTypeAttr] = &cepb.CloudEventAttributeValue{
			Attr: &cepb.CloudEventAttributeValue_CeString{
				CeString: s,
			},
		}
	}
	if s := w.DataSchema(); s != "" {
		event.Attributes[dataSchemaAttr] = &cepb.CloudEventAttributeValue{
			Attr: &cepb.CloudEventAttributeValue_CeString{
				CeString: s,
			},
		}
	}
	if s := w.Subject(); s != "" {
		event.Attributes[subjectAttr] = &cepb.CloudEventAttributeValue{
			Attr: &cepb.CloudEventAttributeValue_CeString{
				CeString: s,
			},
		}
	}
	if t := w.Time(); !t.IsZero() {
		event.Attributes[timeAttr] = &cepb.CloudEventAttributeValue{
			Attr: &cepb.CloudEventAttributeValue_CeTimestamp{
				CeTimestamp: timestamppb.New(t),
			},
		}
	}
	w.e.RangeExtensionAttributes(func(attr, val []byte) {
		// TODO(james.yin): support native type.
		event.Attributes[string(attr)] = &cepb.CloudEventAttributeValue{
			Attr: &cepb.CloudEventAttributeValue_CeString{
				CeString: string(val),
			},
		}
	})
	// Overwrite XVanusBlockOffset and XVanusStime if exists.
	event.Attributes[segpb.XVanusBlockOffset] = &cepb.CloudEventAttributeValue{
		Attr: &cepb.CloudEventAttributeValue_CeInteger{
			CeInteger: int32(ceschema.SequenceNumber(e)),
		},
	}
	event.Attributes[segpb.XVanusStime] = &cepb.CloudEventAttributeValue{
		Attr: &cepb.CloudEventAttributeValue_CeTimestamp{
			CeTimestamp: timestamppb.New(time.UnixMilli(ceschema.Stime(e))),
		},
	}

	if data := w.Data(); data != nil {
		event.Data = &cepb.CloudEvent_BinaryData{
			BinaryData: w.Data(),
		}
	}

	return &event
}
