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

package block

import (
	"testing"

	v1 "cloudevents.io/genproto/v1"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCloudEventMarshallAndUnmarshall(t *testing.T) {
	Convey("test cloud event marshall and unmarshall", t, func() {
		_ = &v1.CloudEvent{
			Id:          "aaaaa",
			Source:      "bbbbb",
			SpecVersion: "ccccc",
			Type:        "ddddd",
			Attributes: map[string]*v1.CloudEventAttributeValue{
				"aaa": {
					Attr: &v1.CloudEventAttributeValue_CeBoolean{
						CeBoolean: false,
					},
				},
			},
			Data: &v1.CloudEvent_TextData{TextData: "adasdasdasdasdasdas"},
		}
		//data, _ := proto.Marshal(ce)
		//nce := &v1.CloudEvent{}
		//err := proto.Unmarshal(data, nce)
		//So(err, ShouldBeNil)
		//So(ce, ShouldResemble, nce)
	})
}
