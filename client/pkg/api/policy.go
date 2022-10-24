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

package api

import (
	"context"
)

type PolicyType string

const (
	RoundRobin = PolicyType("round_robin")
	Manually   = PolicyType("manually")
	Weight     = PolicyType("weight")
	ReadOnly   = PolicyType("readonly")
	ReadWrite  = PolicyType("readwrite")
)

type ConsumeFromWhere string

var (
	ConsumeFromWhereEarliest = ConsumeFromWhere("earliest")
	ConsumeFromWhereLatest   = ConsumeFromWhere("latest")
)

type WritePolicy interface {
	Type() PolicyType
	NextLog(ctx context.Context) (Eventlog, error)
}

type ReadPolicy interface {
	WritePolicy

	Offset() int64
	Forward(diff int)
}

type LogPolicy interface {
	AccessMode() PolicyType
}
