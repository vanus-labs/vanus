// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventlog

import (
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/errors"
)

// Config is the configuration of EventLog.
type Config struct {
	discovery.VRN
}

func ParseVRN(rawVRN string) (*Config, error) {
	vrn, err := discovery.ParseVRN(rawVRN)
	if err != nil {
		return nil, err
	}

	if vrn.Kind != "eventlog" {
		return nil, errors.ErrInvalidArgument
	}

	return &Config{
		*vrn,
	}, nil
}
