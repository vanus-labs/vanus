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

package command

var (
	// for vsctl event.
	eventID           string
	dataFormat        string
	eventSource       string
	eventType         string
	eventBody         string
	dataFile          string
	printDataTemplate bool
	offset            int64
	number            int16

	// for vsctl eventbus and subscription.
	eventbus         string
	source           string
	sink             string
	filters          string
	inputTransformer string
	rateLimit        int32
	fromLatest       bool
	fromEarliest     bool
	fromTime         string
	subscriptionID   uint64
	showSegment      bool
	showBlock        bool
)
