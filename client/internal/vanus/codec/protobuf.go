/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package codec

import (
	// standard libraries
	"fmt"
	"net/url"
	stdtime "time"

	// third-party libraries
	cepb "cloudevents.io/genproto/v1"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/types"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// ContentTypeProtobuf indicates that the data attribute is a protobuf message.
	ContentTypeProtobuf = "application/protobuf"
)

const (
	datacontenttype = "datacontenttype"
	dataschema      = "dataschema"
	subject         = "subject"
	time            = "time"
)

var (
	zeroTime = stdtime.Time{}
)

// convert an SDK event to a protobuf variant of the event that can be marshaled.
func ToProto(e *event.Event) (*cepb.CloudEvent, error) {
	container := &cepb.CloudEvent{
		Id:          e.ID(),
		Source:      e.Source(),
		SpecVersion: e.SpecVersion(),
		Type:        e.Type(),
		Attributes:  make(map[string]*cepb.CloudEventAttributeValue),
	}
	if e.DataContentType() != "" {
		container.Attributes[datacontenttype], _ = attributeFor(e.DataContentType())
	}
	if e.DataSchema() != "" {
		container.Attributes[dataschema], _ = attributeFor(e.DataSchema())
	}
	if e.Subject() != "" {
		container.Attributes[subject], _ = attributeFor(e.Subject())
	}
	if e.Time() != zeroTime {
		container.Attributes[time], _ = attributeFor(e.Time())
	}
	for name, value := range e.Extensions() {
		attr, err := attributeFor(value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode attribute %s: %s", name, err)
		}
		container.Attributes[name] = attr
	}
	container.Data = &cepb.CloudEvent_BinaryData{
		BinaryData: e.Data(),
	}
	if e.DataContentType() == ContentTypeProtobuf {
		anymsg := &anypb.Any{
			TypeUrl: e.DataSchema(),
			Value:   e.Data(),
		}
		container.Data = &cepb.CloudEvent_ProtoData{
			ProtoData: anymsg,
		}
	}
	return container, nil
}

func attributeFor(v interface{}) (*cepb.CloudEventAttributeValue, error) {
	vv, err := types.Validate(v)
	if err != nil {
		return nil, err
	}
	attr := &cepb.CloudEventAttributeValue{}
	switch vt := vv.(type) {
	case bool:
		attr.Attr = &cepb.CloudEventAttributeValue_CeBoolean{
			CeBoolean: vt,
		}
	case int32:
		attr.Attr = &cepb.CloudEventAttributeValue_CeInteger{
			CeInteger: vt,
		}
	case string:
		attr.Attr = &cepb.CloudEventAttributeValue_CeString{
			CeString: vt,
		}
	case []byte:
		attr.Attr = &cepb.CloudEventAttributeValue_CeBytes{
			CeBytes: vt,
		}
	case types.URI:
		attr.Attr = &cepb.CloudEventAttributeValue_CeUri{
			CeUri: vt.String(),
		}
	case types.URIRef:
		attr.Attr = &cepb.CloudEventAttributeValue_CeUriRef{
			CeUriRef: vt.String(),
		}
	case types.Timestamp:
		attr.Attr = &cepb.CloudEventAttributeValue_CeTimestamp{
			CeTimestamp: timestamppb.New(vt.Time),
		}
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", v)
	}
	return attr, nil
}

func valueFrom(attr *cepb.CloudEventAttributeValue) (interface{}, error) {
	var v interface{}
	switch vt := attr.Attr.(type) {
	case *cepb.CloudEventAttributeValue_CeBoolean:
		v = vt.CeBoolean
	case *cepb.CloudEventAttributeValue_CeInteger:
		v = vt.CeInteger
	case *cepb.CloudEventAttributeValue_CeString:
		v = vt.CeString
	case *cepb.CloudEventAttributeValue_CeBytes:
		v = vt.CeBytes
	case *cepb.CloudEventAttributeValue_CeUri:
		uri, err := url.Parse(vt.CeUri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URI value %s: %s", vt.CeUri, err.Error())
		}
		v = uri
	case *cepb.CloudEventAttributeValue_CeUriRef:
		uri, err := url.Parse(vt.CeUriRef)
		if err != nil {
			return nil, fmt.Errorf("failed to parse URIRef value %s: %s", vt.CeUriRef, err.Error())
		}
		v = types.URIRef{URL: *uri}
	case *cepb.CloudEventAttributeValue_CeTimestamp:
		v = vt.CeTimestamp.AsTime()
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", vt)
	}
	return types.Validate(v)
}

// Convert from a protobuf variant into the generic, SDK event.
func FromProto(container *cepb.CloudEvent) (*event.Event, error) {
	e := event.New()
	e.SetID(container.Id)
	e.SetSource(container.Source)
	e.SetSpecVersion(container.SpecVersion)
	e.SetType(container.Type)
	// NOTE: There are some issues around missing data content type values that
	// are still unresolved. It is an optional field and if unset then it is
	// implied that the encoding used for the envelope was also used for the
	// data. However, there is no mapping that exists between data content types
	// and the envelope content types. For example, how would this system know
	// that receiving an envelope in application/cloudevents+protobuf know that
	// the implied data content type if missing is application/protobuf.
	//
	// It is also not clear what should happen if the data content type is unset
	// but it is known that the data content type is _not_ the same as the
	// envelope. For example, a JSON encoded data value would be stored within
	// the BinaryData attribute of the protobuf formatted envelope. Protobuf
	// data values, however, are _always_ stored as a protobuf encoded Any type
	// within the ProtoData field. Any use of the BinaryData or TextData fields
	// means the value is _not_ protobuf. If content type is not set then have
	// no way of knowing what the data encoding actually is. Currently, this
	// code does not address this and only loads explicitly set data content
	// type values.
	contentType := ""
	if container.Attributes != nil {
		attr := container.Attributes[datacontenttype]
		if attr != nil {
			if stattr, ok := attr.Attr.(*cepb.CloudEventAttributeValue_CeString); ok {
				contentType = stattr.CeString
			}
		}
	}
	switch dt := container.Data.(type) {
	case *cepb.CloudEvent_BinaryData:
		e.DataEncoded = dt.BinaryData
		// NOTE: If we use SetData then the current implementation always sets
		// the Base64 bit to true. Direct assignment appears to be the only way
		// to set non-base64 encoded binary data.
		// if err := e.SetData(contentType, dt.BinaryData); err != nil {
		// 	return nil, fmt.Errorf("failed to convert binary type (%s) data: %s", contentType, err)
		// }
	case *cepb.CloudEvent_TextData:
		if err := e.SetData(contentType, dt.TextData); err != nil {
			return nil, fmt.Errorf("failed to convert text type (%s) data: %s", contentType, err)
		}
	case *cepb.CloudEvent_ProtoData:
		e.SetDataContentType(ContentTypeProtobuf)
		e.DataEncoded = dt.ProtoData.Value
	}
	for name, value := range container.Attributes {
		v, err := valueFrom(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert attribute %s: %s", name, err)
		}
		switch name {
		case datacontenttype:
			vs, _ := v.(string)
			e.SetDataContentType(vs)
		case dataschema:
			vs, _ := v.(string)
			e.SetDataSchema(vs)
		case subject:
			vs, _ := v.(string)
			e.SetSubject(vs)
		case time:
			vs, _ := v.(types.Timestamp)
			e.SetTime(vs.Time)
		default:
			e.SetExtension(name, v)
		}
	}
	return &e, nil
}
