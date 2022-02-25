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

package convert

import (
	"encoding/json"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"github.com/pkg/errors"
)

func MetaSubToInnerSub(sub *meta.Subscription) (*primitive.Subscription, error) {
	b, err := json.Marshal(sub)
	if err != nil {
		return nil, errors.Wrap(err, "marshal error")
	}
	to := &primitive.Subscription{}
	err = json.Unmarshal(b, to)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshal error, json %s", string(b))
	}
	return to, nil
}

func InnerSubToMetaSub(sub *primitive.Subscription) (*meta.Subscription, error) {
	b, err := json.Marshal(sub)
	if err != nil {
		return nil, errors.Wrap(err, "marshal error")
	}
	to := &meta.Subscription{}
	err = json.Unmarshal(b, to)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshal error, json %s", string(b))
	}

	return to, nil
}
