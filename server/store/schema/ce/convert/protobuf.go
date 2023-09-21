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
	"encoding/binary"
	"time"

	// third-party libraries.
	"google.golang.org/protobuf/types/known/timestamppb"

	// first-party libraries.
	cepb "github.com/vanus-labs/vanus/api/cloudevents"
	segpb "github.com/vanus-labs/vanus/api/segment"

	// this project.
	"github.com/vanus-labs/vanus/server/store/block"
	ceschema "github.com/vanus-labs/vanus/server/store/schema/ce"
	cetype "github.com/vanus-labs/vanus/server/store/schema/ce/typesystem"
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

	event.Attributes = make(map[string]*cepb.CloudEvent_CloudEventAttributeValue)
	if s := w.DataContentType(); s != "" {
		event.Attributes[dataContentTypeAttr] = &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeString{
				CeString: s,
			},
		}
	}
	if s := w.DataSchema(); s != "" {
		event.Attributes[dataSchemaAttr] = &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeString{
				CeString: s,
			},
		}
	}
	if s := w.Subject(); s != "" {
		event.Attributes[subjectAttr] = &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeString{
				CeString: s,
			},
		}
	}
	if t := w.Time(); !t.IsZero() {
		event.Attributes[timeAttr] = &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeTimestamp{
				CeTimestamp: timestamppb.New(t),
			},
		}
	}
	w.e.RangeExtensionAttributes(block.OnExtensionAttributeFunc(func(attr []byte, val block.Value) {
		if v := toPbValue(val); v != nil {
			event.Attributes[string(attr)] = toPbValue(val)
		}
	}))
	// Overwrite XVanusBlockOffset and XVanusStime if exists.
	event.Attributes[segpb.XVanusBlockOffset] = &cepb.CloudEvent_CloudEventAttributeValue{
		Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeInteger{
			CeInteger: int32(ceschema.SequenceNumber(e)),
		},
	}
	event.Attributes[segpb.XVanusStime] = &cepb.CloudEvent_CloudEventAttributeValue{
		Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeTimestamp{
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

func toPbValue(val block.Value) *cepb.CloudEvent_CloudEventAttributeValue {
	v := val.Value()
	n := len(v)

	if n == 0 {
		return nil // unreachable
	}

	switch v[n-1] {
	case cetype.AttrTypeFalse:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeBoolean{
				CeBoolean: false,
			},
		}
	case cetype.AttrTypeTrue:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeBoolean{
				CeBoolean: true,
			},
		}
	case cetype.AttrTypeInteger:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeInteger{
				CeInteger: int32(binary.LittleEndian.Uint32(v[:n-1])),
			},
		}
	case cetype.AttrTypeString:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeString{
				CeString: string(v[:n-1]),
			},
		}
	case cetype.AttrTypeBytes:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeBytes{
				CeBytes: v[:n-1],
			},
		}
	case cetype.AttrTypeURI:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeUri{
				CeUri: string(v[:n-1]),
			},
		}
	case cetype.AttrTypeURIRef:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeUriRef{
				CeUriRef: string(v[:n-1]),
			},
		}
	case cetype.AttrTypeTimestamp:
		return &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeTimestamp{
				CeTimestamp: &timestamppb.Timestamp{
					Seconds: int64(binary.LittleEndian.Uint64(v[:n-5])),
					Nanos:   int32(binary.LittleEndian.Uint32(v[n-5 : n-1])),
				},
			},
		}
	}

	return nil // attrTypeNone
}
