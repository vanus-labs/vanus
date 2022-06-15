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

package filter_test

import (
	"testing"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/filter"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetFilter(t *testing.T) {
	event := ce.NewEvent()
	event.SetID("testID")
	event.SetSource("testSource")
	_ = event.SetData(ce.ApplicationJSON, map[string]interface{}{
		"key": "value",
		"num": 10,
	})
	filters := make([]*primitive.SubscriptionFilter, 0)
	Convey("trigger filter no filter", t, func() {
		f := filter.GetFilter(filters)
		So(f, ShouldBeNil)
		result := filter.Run(f, event)
		So(result, ShouldEqual, filter.PassFilter)
	})
	filters = append(filters, &primitive.SubscriptionFilter{})
	Convey("trigger filter one filter no filed", t, func() {
		f := filter.GetFilter(filters)
		So(f, ShouldBeNil)
		result := filter.Run(f, event)
		So(result, ShouldEqual, filter.PassFilter)
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		Exact: map[string]string{
			"id": "testID",
		},
	})
	Convey("trigger filter one filter", t, func() {
		f := filter.GetFilter(filters)
		So(f, ShouldNotBeNil)
		result := filter.Run(f, event)
		So(result, ShouldEqual, filter.PassFilter)
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		Suffix: map[string]string{
			"id": "ID",
		},
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		Prefix: map[string]string{
			"id": "test",
		},
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		CEL: "$num.(int64) == 10",
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		CeSQL: "source = 'testSource'",
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		Not: &primitive.SubscriptionFilter{
			Exact: map[string]string{
				"id": "un",
			},
		},
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		Any: []*primitive.SubscriptionFilter{
			{
				Exact: map[string]string{
					"id": "testID",
				},
			},
		},
	})
	filters = append(filters, &primitive.SubscriptionFilter{
		All: []*primitive.SubscriptionFilter{
			{
				Exact: map[string]string{
					"id": "testID",
				},
			},
		},
	})
	Convey("trigger filter multi filter", t, func() {
		f := filter.GetFilter(filters)
		So(f, ShouldNotBeNil)
		result := filter.Run(f, event)
		So(result, ShouldEqual, filter.PassFilter)
	})
}
