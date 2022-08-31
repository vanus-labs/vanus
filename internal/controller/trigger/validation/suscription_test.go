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

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSubscriptionRequestValidator(t *testing.T) {
	ctx := context.Background()
	Convey("multiple dialect", t, func() {
		request := &ctrlpb.SubscriptionRequest{
			Filters: []*metapb.Filter{{
				Not: &metapb.Filter{
					Exact: map[string]string{
						"key1": "value1",
					},
				},
				Cel: "$type.(string) =='test'",
			}},
		}
		So(ValidateSubscriptionRequest(ctx, request), ShouldNotBeNil)
	})
	Convey("sink empty", t, func() {
		request := &ctrlpb.SubscriptionRequest{
			Sink:     "",
			EventBus: "bus",
		}
		So(ValidateSubscriptionRequest(ctx, request), ShouldNotBeNil)
	})
	Convey("eventBus empty", t, func() {
		request := &ctrlpb.SubscriptionRequest{
			Sink:     "sink",
			EventBus: "",
		}
		So(ValidateSubscriptionRequest(ctx, request), ShouldNotBeNil)
	})
}

func TestValidateSubscriptionConfig(t *testing.T) {
	ctx := context.Background()
	Convey("test validate subscription config", t, func() {
		Convey("test rate limit", func() {
			config := &metapb.SubscriptionConfig{
				RateLimit: -2,
			}
			So(validateSubscriptionConfig(ctx, config), ShouldNotBeNil)
		})
		Convey("test offset timestamp", func() {
			config := &metapb.SubscriptionConfig{
				OffsetType: metapb.SubscriptionConfig_TIMESTAMP,
			}
			So(validateSubscriptionConfig(ctx, config), ShouldNotBeNil)
		})
		Convey("test max retry attempts", func() {
			config := &metapb.SubscriptionConfig{
				MaxRetryAttempts: 10000,
			}
			So(validateSubscriptionConfig(ctx, config), ShouldNotBeNil)
		})
	})
}

func TestValidateSinkAndProtocol(t *testing.T) {
	ctx := context.Background()
	Convey("subscription protocol is lambda", t, func() {
		Convey("arn is invalid", func() {
			sink := "arn:aws:lambda"
			credential := &metapb.SinkCredential{}
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_AWS_LAMBDA, credential), ShouldNotBeNil)
		})
		sink := "arn:aws:lambda:us-west-2:843378899134:function:xdltest"
		Convey("sink credential is nil", func() {
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_AWS_LAMBDA, nil), ShouldNotBeNil)
		})
		Convey("sink credential type is invalid", func() {
			credential := &metapb.SinkCredential{CredentialType: metapb.SinkCredential_PLAIN}
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_AWS_LAMBDA, credential), ShouldNotBeNil)
		})
		Convey("all valid", func() {
			credential := &metapb.SinkCredential{CredentialType: metapb.SinkCredential_CLOUD}
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_AWS_LAMBDA, credential), ShouldBeNil)
		})
	})
}

func TestValidateSinkCredential(t *testing.T) {
	ctx := context.Background()
	Convey("subscription sink credential type is lambda", t, func() {
		Convey("ak or sk is empty", func() {
			credential := &metapb.SinkCredential{
				CredentialType: metapb.SinkCredential_CLOUD,
			}
			So(validateSinkCredential(ctx, credential), ShouldNotBeNil)
		})
		Convey("all valid", func() {
			credential := &metapb.SinkCredential{
				CredentialType: metapb.SinkCredential_CLOUD,
				Credential: &metapb.SinkCredential_Cloud{
					Cloud: &metapb.CloudCredential{
						AccessKeyId:     "xxxxxx",
						SecretAccessKey: "xxxxxx",
					},
				},
			}
			So(validateSinkCredential(ctx, credential), ShouldBeNil)
		})
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
