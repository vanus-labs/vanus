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

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/primitive/cel"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"

	cesqlparser "github.com/cloudevents/sdk-go/sql/v2/parser"
)

type createSubscriptionRequestValidator ctrlpb.CreateSubscriptionRequest

func ConvertCreateSubscriptionRequest(request *ctrlpb.CreateSubscriptionRequest) createSubscriptionRequestValidator {
	if request == nil {
		return createSubscriptionRequestValidator{}
	}
	return createSubscriptionRequestValidator(*request)
}

func (request createSubscriptionRequestValidator) Validate(ctx context.Context) error {
	err := filterListValidator(request.Filters).Validate(ctx)
	if err != nil {
		return errors.ErrInvalidRequest.WithMessage("filters is invalid").Wrap(err)
	}
	if request.Sink == "" {
		return errors.ErrInvalidRequest.WithMessage("sink is empty")
	}
	return nil
}

type filterListValidator []*metapb.Filter

func (filters filterListValidator) Validate(ctx context.Context) error {
	if len(filters) == 0 {
		return nil
	}
	for _, f := range filters {
		if f == nil {
			continue
		}
		err := convertFilterValidation(f).Validate(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

type filterValidator metapb.Filter

func convertFilterValidation(f *metapb.Filter) filterValidator {
	if f == nil {
		return filterValidator{}
	}
	return filterValidator(*f)
}

func (f filterValidator) Validate(ctx context.Context) error {
	if f.hasMultipleDialects() {
		return errors.ErrFilterMultiple.WithMessage("filters can have only one dialect")
	}
	err := validateAttributeMap("exact", f.Exact)
	if err != nil {
		return err
	}
	err = validateAttributeMap("prefix", f.Prefix)
	if err != nil {
		return err
	}
	err = validateAttributeMap("suffix", f.Suffix)
	if err != nil {
		return err
	}
	if f.Sql != "" {
		err = validateCeSql(ctx, f.Sql)
		if err != nil {
			return err
		}
	}
	if f.Cel != "" {
		err = validateCel(ctx, f.Cel)
		if err != nil {
			return err
		}
	}
	if f.Not != nil {
		err = convertFilterValidation(f.Not).Validate(ctx)
		if err != nil {
			return errors.ErrInvalidRequest.WithMessage("not filter dialect invalid").Wrap(err)
		}
	}
	err = filterListValidator(f.All).Validate(ctx)
	if err != nil {
		return errors.ErrInvalidRequest.WithMessage("all filter dialect invalid").Wrap(err)
	}
	err = filterListValidator(f.Any).Validate(ctx)
	if err != nil {
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
	if _, err := cel.Parse(expression); err != nil {
		return errors.ErrCelExpression.WithMessage(expression).Wrap(err)
	}
	return nil
}

func validateCeSql(ctx context.Context, expression string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.ErrCeSqlExpression.WithMessage(expression)
		}
	}()
	if _, err := cesqlparser.Parse(expression); err != nil {
		return errors.ErrCeSqlExpression.WithMessage(expression).Wrap(err)
	}
	return nil
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

func (f filterValidator) hasMultipleDialects() bool {
	dialectFound := false
	if len(f.Exact) > 0 {
		dialectFound = true
	}
	if len(f.Prefix) > 0 {
		if dialectFound {
			return true
		} else {
			dialectFound = true
		}
	}
	if len(f.Suffix) > 0 {
		if dialectFound {
			return true
		} else {
			dialectFound = true
		}
	}
	if len(f.All) > 0 {
		if dialectFound {
			return true
		} else {
			dialectFound = true
		}
	}
	if len(f.Any) > 0 {
		if dialectFound {
			return true
		} else {
			dialectFound = true
		}
	}
	if f.Not != nil {
		if dialectFound {
			return true
		} else {
			dialectFound = true
		}
	}
	if f.Sql != "" && dialectFound {
		return true
	}
	return false
}
