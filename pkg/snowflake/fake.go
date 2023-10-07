// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package snowflake

import (
	"sync"
	"time"

	vanus "github.com/vanus-labs/vanus/api/vsr"
)

var lock = sync.Mutex{}

// NewTestID only used for Uint Test.
func NewTestID() vanus.ID {
	lock.Lock()
	defer lock.Unlock()

	// avoiding same id
	time.Sleep(time.Microsecond)
	return vanus.ID(time.Now().UnixNano())
}

// InitializeFake just only used for Uint Test.
func InitializeFake() {
	fake = true
}
