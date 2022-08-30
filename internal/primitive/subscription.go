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

package primitive

import (
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

type URI string

type Subscription struct {
	ID              vanus.ID              `json:"id"`
	Filters         []*SubscriptionFilter `json:"filters,omitempty"`
	Sink            URI                   `json:"sink,omitempty"`
	EventBus        string                `json:"eventbus"`
	Offsets         info.ListOffsetInfo   `json:"offsets"`
	Transformer     *Transformer          `json:"transformer,omitempty"`
	Config          SubscriptionConfig    `json:"config,omitempty"`
	Protocol        Protocol              `json:"protocol,omitempty"`
	ProtocolSetting *ProtocolSetting      `json:"protocolSetting,omitempty"`
	SinkCredential  SinkCredential        `json:"sink_credential,omitempty"`
}

type Protocol string

const (
	HTTPProtocol      Protocol = "http"
	AwsLambdaProtocol Protocol = "aws-lambda"
)

type ProtocolSetting struct {
	Headers map[string]string `json:"headers,omitempty"`
}

type OffsetType int32

const (
	LatestOffset   OffsetType = 0
	EarliestOffset OffsetType = 1
	Timestamp      OffsetType = 2
)

type SubscriptionConfig struct {
	RateLimit int32 `json:"rate_limit,omitempty"`
	// consumer from
	OffsetType         OffsetType `json:"offset_type,omitempty"`
	OffsetTimestamp    *uint64    `json:"offset_timestamp,omitempty"`
	DeliveryTimeout    int32      `json:"delivery_timeout,omitempty"`
	MaxRetryAttempts   int32      `json:"max_retry_attempts,omitempty"`
	DeadLetterEventbus string     `json:"dead_letter_eventbus,omitempty"`
}

type SubscriptionFilter struct {
	Exact  map[string]string     `json:"exact,omitempty"`
	Prefix map[string]string     `json:"prefix,omitempty"`
	Suffix map[string]string     `json:"suffix,omitempty"`
	CeSQL  string                `json:"ce_sql,omitempty"`
	Not    *SubscriptionFilter   `json:"not,omitempty"`
	All    []*SubscriptionFilter `json:"all,omitempty"`
	Any    []*SubscriptionFilter `json:"any,omitempty"`
	CEL    string                `json:"cel,omitempty"`
}

type Transformer struct {
	Define   map[string]string `json:"define,omitempty"`
	Template string            `json:"template,omitempty"`
}

/* annotation no use code .
type SinkSpec struct {
	Type   string
	Name   string // TODO use id or name? ID used in CloudEvents Specification
	Weight float32
	Config map[string]interface{}
}

type DialectType string

const (
	ExactDialect  = "exact"
	PrefixDialect = "prefix"
	SuffixDialect = "suffix"
	AllDialect    = "all"
	AnyDialect    = "any"
	NotDialect    = "not"
	SQLDialect    = "ce-sql"
)

type FilterSpec struct {
	Name    string
	Exp     interface{}
	ApplyTo []string
	Target  TargetSpec
}

type SubscriptionSpec struct {
	EventBuses []string
	Sinks      []SinkSpec
	Filters    []FilterSpec
}

type TargetSpec struct {
	LBStrategy string
	Sinks      []string
}
*/
