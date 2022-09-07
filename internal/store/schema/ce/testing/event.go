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

package testing

import (
	// standard libraries.
	"time"

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"
	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/types/known/timestamppb"

	// first-party libraries.
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"
)

const (
	seq0              int64 = 0
	seq1              int64 = 1
	seq2              int64 = 2
	ceID0                   = "ce-id0"
	ceID1                   = "ce-id1"
	ceSource                = "ce-source"
	ceSpecVersion           = "1.0"
	ceType                  = "ce-type"
	ceDataContentType       = ce.TextPlain
	ceSubject               = "ce-subject"
)

var (
	Stime  int64 = 0x182D2E76BF3
	ceData       = []byte("ce-data")
	ceTime       = time.Unix(0x6306E32E, 0x04030201)
)

func MakeEvent0() *cepb.CloudEvent {
	event := &cepb.CloudEvent{
		Id:          ceID0,
		Source:      ceSource,
		SpecVersion: ceSpecVersion,
		Type:        ceType,
	}
	return event
}

func MakeEvent1() *cepb.CloudEvent {
	event := &cepb.CloudEvent{
		Id:          ceID1,
		Source:      ceSource,
		SpecVersion: ceSpecVersion,
		Type:        ceType,
		Attributes: map[string]*cepb.CloudEventAttributeValue{
			"datacontenttype": {
				Attr: &cepb.CloudEventAttributeValue_CeString{
					CeString: ceDataContentType,
				},
			},
			"subject": {
				Attr: &cepb.CloudEventAttributeValue_CeString{
					CeString: ceSubject,
				},
			},
			"time": {
				Attr: &cepb.CloudEventAttributeValue_CeTimestamp{
					CeTimestamp: timestamppb.New(ceTime),
				},
			},
			"attr0": {
				Attr: &cepb.CloudEventAttributeValue_CeString{
					CeString: "value0",
				},
			},
			"attr1": {
				Attr: &cepb.CloudEventAttributeValue_CeString{
					CeString: "value1",
				},
			},
			"attr2": {
				Attr: &cepb.CloudEventAttributeValue_CeString{
					CeString: "value2",
				},
			},
		},
		Data: &cepb.CloudEvent_BinaryData{
			BinaryData: ceData,
		},
	}
	return event
}

func CheckEvent0(event *cepb.CloudEvent) {
	So(event.Id, ShouldEqual, ceID0)
	So(event.Source, ShouldEqual, ceSource)
	So(event.SpecVersion, ShouldEqual, ceSpecVersion)
	So(event.Type, ShouldEqual, ceType)
	So(event.Attributes, ShouldHaveLength, 2)
	So(event.Attributes[segpb.XVanusBlockOffset].GetCeInteger(), ShouldEqual, seq0)
	So(event.Attributes[segpb.XVanusStime].GetCeTimestamp().AsTime(), ShouldEqual, time.UnixMilli(Stime))
	So(event.Data, ShouldBeNil)
}

func CheckEvent1(event *cepb.CloudEvent) {
	So(event.Id, ShouldEqual, ceID1)
	So(event.Source, ShouldEqual, ceSource)
	So(event.SpecVersion, ShouldEqual, ceSpecVersion)
	So(event.Type, ShouldEqual, ceType)
	So(event.Attributes, ShouldHaveLength, 8)
	So(event.Attributes[segpb.XVanusBlockOffset].GetCeInteger(), ShouldEqual, seq1)
	So(event.Attributes[segpb.XVanusStime].GetCeTimestamp().AsTime(), ShouldEqual, time.UnixMilli(Stime))
	So(event.Attributes["datacontenttype"].GetCeString(), ShouldEqual, ceDataContentType)
	So(event.Attributes["subject"].GetCeString(), ShouldEqual, ceSubject)
	So(event.Attributes["time"].GetCeTimestamp().AsTime(), ShouldEqual, ceTime)
	So(event.Attributes["attr0"].GetCeString(), ShouldEqual, "value0")
	So(event.Attributes["attr1"].GetCeString(), ShouldEqual, "value1")
	So(event.Attributes["attr2"].GetCeString(), ShouldEqual, "value2")
	So(event.GetBinaryData(), ShouldResemble, ceData)
}
