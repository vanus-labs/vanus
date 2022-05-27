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
	"context"
	"testing"

	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateSubscriptionRequestValidator(t *testing.T) {
	ctx := context.Background()
	Convey("multiple dialect", t, func() {
		request := &ctrlpb.CreateSubscriptionRequest{
			Filters: []*metapb.Filter{{
				Not: &metapb.Filter{
					Exact: map[string]string{
						"key1": "value1",
					},
				},
				Cel: "$type.(string) =='test'",
			}},
		}
		So(ValidateCreateSubscription(ctx, request), ShouldNotBeNil)
	})
	Convey("sink empty", t, func() {
		request := &ctrlpb.CreateSubscriptionRequest{
			Sink:     "",
			EventBus: "bus",
		}
		So(ValidateCreateSubscription(ctx, request), ShouldNotBeNil)
	})
	Convey("eventBus empty", t, func() {
		request := &ctrlpb.CreateSubscriptionRequest{
			Sink:     "sink",
			EventBus: "",
		}
		So(ValidateCreateSubscription(ctx, request), ShouldNotBeNil)
	})

}

func TestValidateFilter(t *testing.T) {
	ctx := context.Background()
	Convey("exact key empty", t, func() {
		f := &metapb.Filter{
			Exact: map[string]string{
				"": "value",
			},
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("exact value empty", t, func() {
		f := &metapb.Filter{
			Exact: map[string]string{
				"key": "",
			},
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("suffix key empty", t, func() {
		f := &metapb.Filter{
			Suffix: map[string]string{
				"": "value",
			},
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("suffix value empty", t, func() {
		f := &metapb.Filter{
			Suffix: map[string]string{
				"key": "",
			},
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("prefix key empty", t, func() {
		f := &metapb.Filter{
			Prefix: map[string]string{
				"": "value",
			},
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("prefix value empty", t, func() {
		f := &metapb.Filter{
			Prefix: map[string]string{
				"key": "",
			},
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("sql", t, func() {
		f := &metapb.Filter{
			Sql: "source = 'test'",
		}
		So(ValidateFilter(ctx, f), ShouldBeNil)
	})
	Convey("sql invalid", t, func() {
		f := &metapb.Filter{
			Sql: "source == 'test'",
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("cel", t, func() {
		f := &metapb.Filter{
			Cel: "$type.(string) =='test'",
		}
		So(ValidateFilter(ctx, f), ShouldBeNil)
	})
	Convey("cel invalid", t, func() {
		f := &metapb.Filter{
			Cel: "$type.(string) ==test",
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("not", t, func() {
		f := &metapb.Filter{
			Not: &metapb.Filter{
				Exact: map[string]string{
					"key": "value",
				},
			},
		}
		So(ValidateFilter(ctx, f), ShouldBeNil)
	})
	filters := []*metapb.Filter{
		{
			Exact: map[string]string{
				"key": "value",
			},
		}, {
			Suffix: map[string]string{
				"key": "value",
			},
		},
	}
	filtersInvalid := []*metapb.Filter{
		{
			Exact: map[string]string{
				"": "value",
			},
		}, {
			Suffix: map[string]string{
				"key": "value",
			},
		},
	}
	Convey("filter list", t, func() {
		So(ValidateFilterList(ctx, filters), ShouldBeNil)
		So(ValidateFilterList(ctx, filtersInvalid), ShouldNotBeNil)
	})
	Convey("all", t, func() {
		f := &metapb.Filter{
			All: filters,
		}
		So(ValidateFilter(ctx, f), ShouldBeNil)
	})
	Convey("all invalid", t, func() {
		f := &metapb.Filter{
			All: filtersInvalid,
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
	Convey("any", t, func() {
		f := &metapb.Filter{
			Any: filters,
		}
		So(ValidateFilter(ctx, f), ShouldBeNil)
	})
	Convey("any invalid", t, func() {
		f := &metapb.Filter{
			Any: filtersInvalid,
		}
		So(ValidateFilter(ctx, f), ShouldNotBeNil)
	})
}
