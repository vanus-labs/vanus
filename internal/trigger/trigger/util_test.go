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

package trigger

import (
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewEventClient(t *testing.T) {
	Convey("test new event client", t, func() {
		Convey("new lambda client", func() {
			cli := newEventClient("test", primitive.AwsLambdaProtocol,
				primitive.NewCloudSinkCredential("ak", "sk"))
			So(cli, ShouldNotBeNil)
		})
		Convey("new http client", func() {
			cli := newEventClient("test", primitive.HTTPProtocol,
				primitive.NewPlainSinkCredential("identifier", "secret"))
			So(cli, ShouldNotBeNil)
		})
	})
}

func TestIsShouldRetry(t *testing.T) {
	Convey("test should retry", t, func() {
		b, _ := isShouldRetry(400)
		So(b, ShouldBeFalse)
		b, _ = isShouldRetry(403)
		So(b, ShouldBeFalse)
		b, _ = isShouldRetry(413)
		So(b, ShouldBeFalse)
		b, _ = isShouldRetry(500)
		So(b, ShouldBeTrue)
	})
}

func TestCalDeliveryTime(t *testing.T) {
	Convey("test cal delivery time", t, func() {
		d := calDeliveryTime(1)
		So(d, ShouldEqual, time.Second)
		d = calDeliveryTime(2)
		So(d, ShouldEqual, time.Second*5)
		d = calDeliveryTime(3)
		So(d, ShouldEqual, time.Second*10)
		d = calDeliveryTime(4)
		So(d, ShouldEqual, time.Second*30)
		d = calDeliveryTime(5)
		So(d, ShouldEqual, time.Second*60)
		d = calDeliveryTime(6)
		So(d, ShouldEqual, time.Second*120)
		d = calDeliveryTime(7)
		So(d, ShouldEqual, time.Second*240)
		d = calDeliveryTime(8)
		So(d, ShouldEqual, time.Second*480)
		d = calDeliveryTime(9)
		So(d, ShouldEqual, time.Second*960)
		d = calDeliveryTime(10)
		So(d, ShouldEqual, time.Second*3600)
	})
}
