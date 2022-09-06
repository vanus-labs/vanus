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
	"fmt"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/cel"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	cesqlparser "github.com/cloudevents/sdk-go/sql/v2/parser"
)

func ValidateSubscriptionRequest(ctx context.Context, request *ctrlpb.SubscriptionRequest) error {
	if err := ValidateFilterList(ctx, request.Filters); err != nil {
		return errors.ErrInvalidRequest.WithMessage("filters is invalid").Wrap(err)
	}
	if err := validateProtocol(ctx, request.Protocol); err != nil {
		return err
	}
	if request.Sink == "" {
		return errors.ErrInvalidRequest.WithMessage("sink is empty")
	}
	if err := ValidateSinkAndProtocol(ctx, request.Sink, request.Protocol, request.SinkCredential); err != nil {
		return err
	}
	if err := validateSinkCredential(ctx, request.SinkCredential); err != nil {
		return err
	}
	if request.EventBus == "" {
		return errors.ErrInvalidRequest.WithMessage("eventBus is empty")
	}
	if err := validateSubscriptionConfig(ctx, request.Config); err != nil {
		return err
	}
	return nil
}

func validateProtocol(ctx context.Context, protocol metapb.Protocol) error {
	switch protocol {
	case metapb.Protocol_HTTP:
	case metapb.Protocol_AWS_LAMBDA:
	default:
		return errors.ErrInvalidRequest.WithMessage("protocol is invalid")
	}
	return nil
}

func ValidateSinkAndProtocol(ctx context.Context,
	sink string,
	protocol metapb.Protocol,
	credential *metapb.SinkCredential) error {
	switch protocol {
	case metapb.Protocol_AWS_LAMBDA:
		if _, err := arn.Parse(sink); err != nil {
			return errors.ErrInvalidRequest.
				WithMessage("protocol is aws lambda, sink is arn, arn parse error").Wrap(err)
		}
		if credential.GetCredentialType() != metapb.SinkCredential_CLOUD {
			return errors.ErrInvalidRequest.
				WithMessage("protocol is aws lambda, sink credential can not be nil and  credential type is cloud")
		}
	case metapb.Protocol_HTTP:
	}
	return nil
}

func validateSinkCredential(ctx context.Context, credential *metapb.SinkCredential) error {
	if credential == nil {
		return nil
	}
	switch credential.CredentialType {
	case metapb.SinkCredential_None:
	case metapb.SinkCredential_PLAIN:
		if credential.GetPlain().GetIdentifier() == "" || credential.GetPlain().GetSecret() == "" {
			return errors.ErrInvalidRequest.WithMessage("sink credential type is plain,Identifier and Secret can not empty")
		}
	case metapb.SinkCredential_CLOUD:
		if credential.GetCloud().GetAccessKeyId() == "" || credential.GetCloud().GetSecretAccessKey() == "" {
			return errors.ErrInvalidRequest.
				WithMessage("sink credential type is cloud,accessKeyId and SecretAccessKey can not empty")
		}
	default:
		return errors.ErrInvalidRequest.WithMessage("sink credential type is invalid")
	}
	return nil
}

func validateSubscriptionConfig(ctx context.Context, cfg *metapb.SubscriptionConfig) error {
	if cfg == nil {
		return nil
	}
	if cfg.RateLimit < -1 {
		return errors.ErrInvalidRequest.WithMessage("rate limit is -1 or gt than 0")
	}
	switch cfg.OffsetType {
	case metapb.SubscriptionConfig_LATEST, metapb.SubscriptionConfig_EARLIEST:
	case metapb.SubscriptionConfig_TIMESTAMP:
		if cfg.OffsetTimestamp == nil {
			return errors.ErrInvalidRequest.WithMessage("offset type is timestamp, offset timestamp can not be nil")
		}
	default:
		return errors.ErrInvalidRequest.WithMessage("offset type is invalid")
	}
	if cfg.DeadLetterEventbus != "" {
		return errors.ErrInvalidRequest.WithMessage("no support to set dead letter eventbus")
	}
	if cfg.MaxRetryAttempts > primitive.MaxRetryAttempts {
		return errors.ErrInvalidRequest.WithMessage(
			fmt.Sprintf("could not set max retry attempts greater than %d", primitive.MaxRetryAttempts))
	}
	return nil
}
func ValidateFilterList(ctx context.Context, filters []*metapb.Filter) error {
	if len(filters) == 0 {
		return nil
	}
	for _, f := range filters {
		if f == nil {
			continue
		}
		if err := ValidateFilter(ctx, f); err != nil {
			return err
		}
	}
	return nil
}

func ValidateFilter(ctx context.Context, f *metapb.Filter) error {
	if hasMultipleDialects(f) {
		return errors.ErrFilterMultiple.WithMessage("filters can have only one dialect")
	}
	if err := validateAttributeMap("exact", f.Exact); err != nil {
		return err
	}
	if err := validateAttributeMap("prefix", f.Prefix); err != nil {
		return err
	}
	if err := validateAttributeMap("suffix", f.Suffix); err != nil {
		return err
	}
	if f.Sql != "" {
		if err := validateCeSQL(ctx, f.Sql); err != nil {
			return err
		}
	}
	if f.Cel != "" {
		if err := validateCel(ctx, f.Cel); err != nil {
			return err
		}
	}
	if f.Not != nil {
		if err := ValidateFilter(ctx, f.Not); err != nil {
			return errors.ErrInvalidRequest.WithMessage("not filter dialect invalid").Wrap(err)
		}
	}
	if err := ValidateFilterList(ctx, f.All); err != nil {
		return errors.ErrInvalidRequest.WithMessage("all filter dialect invalid").Wrap(err)
	}
	if err := ValidateFilterList(ctx, f.Any); err != nil {
		return errors.ErrInvalidRequest.WithMessage("any filter dialect invalid").Wrap(err)
	}
	return nil
}

func validateCel(ctx context.Context, expression string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.ErrCelExpression.WithMessage(expression)
		}
	}()
	if _, err = cel.Parse(expression); err != nil {
		return errors.ErrCelExpression.WithMessage(expression).Wrap(err)
	}
	return err
}

func validateCeSQL(ctx context.Context, expression string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.ErrCeSQLExpression.WithMessage(expression)
		}
	}()
	if _, err = cesqlparser.Parse(expression); err != nil {
		return errors.ErrCeSQLExpression.WithMessage(expression).Wrap(err)
	}
	return err
}

func validateAttributeMap(attributeName string, attribute map[string]string) error {
	if len(attribute) == 0 {
		return nil
	}
	for k, v := range attribute {
		if k == "" {
			return errors.ErrFilterAttributeIsEmpty.WithMessage(attributeName + " filter dialect attribute name must not empty")
		}
		if v == "" {
			return errors.ErrFilterAttributeIsEmpty.WithMessage(attributeName + " filter dialect attribute value must not empty")
		}
	}
	return nil
}

func hasMultipleDialects(f *metapb.Filter) bool {
	dialectFound := false
	if len(f.Exact) > 0 {
		dialectFound = true
	}
	if len(f.Prefix) > 0 {
		if dialectFound {
			return true
		}
		dialectFound = true
	}
	if len(f.Suffix) > 0 {
		if dialectFound {
			return true
		}
		dialectFound = true
	}
	if len(f.All) > 0 {
		if dialectFound {
			return true
		}
		dialectFound = true
	}
	if len(f.Any) > 0 {
		if dialectFound {
			return true
		}
		dialectFound = true
	}
	if f.Not != nil {
		if dialectFound {
			return true
		}
		dialectFound = true
	}
	if f.Sql != "" {
		if dialectFound {
			return true
		}
		dialectFound = true
	}
	if f.Cel != "" && dialectFound {
		return true
	}
	return false
}
