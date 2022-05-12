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

package validation

import (
	"testing"

	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateSubscriptionRequestValidator(t *testing.T) {
	Convey("multiple dialect", t, func() {
		request := ConvertCreateSubscriptionRequest(&ctrlpb.CreateSubscriptionRequest{
			Filters: []*metapb.Filter{{
				Exact: map[string]string{
					"key1": "value1",
				},
				Suffix: map[string]string{
					"key2": "values2",
				},
			}},
		})
		So(request.Validate(nil), ShouldNotBeNil)
	})
	Convey("cel", t, func() {
		request := ConvertCreateSubscriptionRequest(&ctrlpb.CreateSubscriptionRequest{
			Filters: []*metapb.Filter{{
				Cel: "$type.(string) =='test'",
			}},
			Sink: "http",
		})
		So(request.Validate(nil), ShouldBeNil)
	})
}
