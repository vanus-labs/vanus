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
	"google.golang.org/protobuf/types/known/structpb"
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
		Convey("test offset timestamp", func() {
			config := &metapb.SubscriptionConfig{
				OffsetType: metapb.SubscriptionConfig_TIMESTAMP,
			}
			So(validateSubscriptionConfig(ctx, config), ShouldNotBeNil)
		})
		Convey("test max retry attempts", func() {
			attempt := uint32(10000)
			config := &metapb.SubscriptionConfig{
				MaxRetryAttempts: &attempt,
			}
			So(validateSubscriptionConfig(ctx, config), ShouldNotBeNil)
		})
	})
}

func TestValidateTransformer(t *testing.T) {
	ctx := context.Background()
	Convey("test validate transformer ", t, func() {
		Convey("test define valid", func() {
			trans := &metapb.Transformer{
				Define: map[string]string{
					"var1": "var",
					"var2": "$.id",
					"var3": "$.data.id",
				},
			}
			So(validateTransformer(ctx, trans), ShouldBeNil)
		})
		Convey("test define invalid", func() {
			trans := &metapb.Transformer{
				Define: map[string]string{
					"var2": "$.a-bc",
				},
			}
			So(validateTransformer(ctx, trans), ShouldNotBeNil)
		})
		Convey("test pipeline valid", func() {
			trans := &metapb.Transformer{
				Pipeline: []*metapb.Action{
					{Command: []*structpb.Value{structpb.NewStringValue("delete"), structpb.NewStringValue("$.id")}},
				},
			}
			So(validateTransformer(ctx, trans), ShouldBeNil)
		})
		Convey("test pipeline invalid", func() {
			trans := &metapb.Transformer{
				Pipeline: []*metapb.Action{
					{Command: []*structpb.Value{structpb.NewStringValue("noExistActionName")}},
				},
			}
			So(validateTransformer(ctx, trans), ShouldNotBeNil)
		})
	})
}

func TestValidateSinkAndProtocol(t *testing.T) {
	ctx := context.Background()
	Convey("sink is empty", t, func() {
		So(ValidateSinkAndProtocol(ctx, "", metapb.Protocol_AWS_LAMBDA, nil), ShouldNotBeNil)
	})
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
			credential := &metapb.SinkCredential{CredentialType: metapb.SinkCredential_AWS}
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_AWS_LAMBDA, credential), ShouldBeNil)
		})
	})
	Convey("subscription protocol is gcloud", t, func() {
		sink := "https://function-1-tvm6jmwz6a-uc.a.run.app"
		Convey("sink credential is nil", func() {
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_GCLOUD_FUNCTIONS, nil), ShouldNotBeNil)
		})
		Convey("sink credential type is invalid", func() {
			credential := &metapb.SinkCredential{CredentialType: metapb.SinkCredential_PLAIN}
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_GCLOUD_FUNCTIONS, credential), ShouldNotBeNil)
		})
		Convey("all valid", func() {
			credential := &metapb.SinkCredential{CredentialType: metapb.SinkCredential_GCLOUD}
			So(ValidateSinkAndProtocol(ctx, sink, metapb.Protocol_GCLOUD_FUNCTIONS, credential), ShouldBeNil)
		})
	})
}

func TestValidateSinkCredential(t *testing.T) {
	ctx := context.Background()
	Convey("subscription sink credential type is AK/SK", t, func() {
		sink := "arn:aws:lambda:us-west-2:843378899134:function:xdltest"
		Convey("ak or sk is empty", func() {
			credential := &metapb.SinkCredential{
				CredentialType: metapb.SinkCredential_AWS,
			}
			So(validateSinkCredential(ctx, sink, credential), ShouldNotBeNil)
		})
		Convey("all valid", func() {
			credential := &metapb.SinkCredential{
				CredentialType: metapb.SinkCredential_AWS,
				Credential: &metapb.SinkCredential_Aws{
					Aws: &metapb.AKSKCredential{
						AccessKeyId:     "xxxxxx",
						SecretAccessKey: "xxxxxx",
					},
				},
			}
			So(validateSinkCredential(ctx, sink, credential), ShouldBeNil)
		})
	})
	Convey("subscription sink credential type is gcloud", t, func() {
		sink := "https://example.com"
		Convey("credential json is empty", func() {
			credential := &metapb.SinkCredential{
				CredentialType: metapb.SinkCredential_GCLOUD,
			}
			So(validateSinkCredential(ctx, sink, credential), ShouldNotBeNil)
		})
		Convey("invalid", func() {
			credential := &metapb.SinkCredential{
				CredentialType: metapb.SinkCredential_GCLOUD,
				Credential: &metapb.SinkCredential_Gcloud{
					Gcloud: &metapb.GCloudCredential{
						CredentialsJson: "{\"type\":\"service_account\"}",
					},
				},
			}
			So(validateSinkCredential(ctx, sink, credential), ShouldNotBeNil)
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
