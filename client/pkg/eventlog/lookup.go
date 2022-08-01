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
	// standard libraries.
	"context"

	// this project.
	"github.com/linkall-labs/vanus/client/internal/vanus/discovery/eventlog"
)

func LookupEarliestOffset(ctx context.Context, vrn string) (int64, error) {
	cfg, err := ParseVRN(vrn)
	if err != nil {
		return 0, err
	}
	// TODO(james.yin): check scheme.
	return eventlog.LookupEarliestOffset(ctx, &cfg.VRN)
}

func LookupLatestOffset(ctx context.Context, vrn string) (int64, error) {
	cfg, err := ParseVRN(vrn)
	if err != nil {
		return 0, err
	}
	// TODO(james.yin): check scheme.
	return eventlog.LookupLatestOffset(ctx, &cfg.VRN)
}

func LookupOffset(ctx context.Context, vrn string, ts int64) (int64, error) {
	cfg, err := ParseVRN(vrn)
	if err != nil {
		return 0, err
	}
	// TODO(james.yin): check scheme.
	return eventlog.LookupOffset(ctx, &cfg.VRN, ts)
}
