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

package snowflake

import (
	// standard libraries.
	"time"

	// first-party libraries.
	vanus "github.com/vanus-labs/vanus/api/vsr"
)

const waitFinishInitSpinInterval = 50 * time.Millisecond

func NewID() (vanus.ID, error) {
	if fake {
		return NewTestID(), nil
	}

	for !initialized.Load() {
		time.Sleep(waitFinishInitSpinInterval)
	}

	id, err := generator.snow.NextID()
	if err != nil {
		return vanus.EmptyID(), err
	}
	return vanus.ID(id), nil
}
