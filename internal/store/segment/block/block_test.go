package block

import (
	v1 "cloudevents.io/genproto/v1"
	"github.com/golang/protobuf/proto"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCloudEventMarshallAndUnmarshall(t *testing.T) {
	Convey("test cloud event marshall and unmarshall", t, func() {
		ce := &v1.CloudEvent{
			Id:          "aaaaa",
			Source:      "bbbbb",
			SpecVersion: "ccccc",
			Type:        "ddddd",
			Attributes: map[string]*v1.CloudEvent_CloudEventAttributeValue{
				"aaa": {
					Attr: &v1.CloudEvent_CloudEventAttributeValue_CeBoolean{
						CeBoolean: false,
					},
				},
			},
			Data: &v1.CloudEvent_TextData{TextData: "adasdasdasdasdasdas"},
		}
		data, _ := proto.Marshal(ce)
		nce := &v1.CloudEvent{}
		err := proto.Unmarshal(data, nce)
		So(err, ShouldBeNil)
		So(ce, ShouldResemble, nce)
	})
}
