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

package client

import (
	"github.com/linkall-labs/vanus/client/internal/vanus"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
)

const (
	XVanusLogOffset = eventlog.XVanusLogOffset
)

// EventBus functions.
var (
	// OpenBusWriter open a Writer of EventBus identified by vrn.
	OpenBusWriter      = eventbus.OpenWriter
	LookupReadableLogs = eventbus.LookupReadableLogs
)

// EventLog functions.
var (
	// OpenLogWriter open a Writer of EventLog identified by vrn.
	OpenLogWriter = eventlog.OpenWriter
	// OpenLogReader open a Reader of EventLog identified by vrn.
	OpenLogReader  = eventlog.OpenReader
	DisablePolling = eventlog.DisablePolling
	PollingTimeout = eventlog.PollingTimeout
	// SearchEventByID searches an event by ID.
	SearchEventByID = eventlog.SearchEventByID

	LookupEarliestLogOffset = eventlog.LookupEarliestOffset
	LookupLatestLogOffset   = eventlog.LookupLatestOffset
	LookupLogOffset         = eventlog.LookupOffset
)

func init() {
	vanus.UseDistributedLog("vanus")
	vanus.UseNameService("vanus")
}
