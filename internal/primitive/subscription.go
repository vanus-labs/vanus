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

type URI string

type Subscription struct {
	ID               string               `json:"id"`
	Source           string               `json:"source"`
	Types            []string             `json:"types"`
	Config           map[string]string    `json:"config"`
	Filters          []SubscriptionFilter `json:"filters"`
	Sink             URI                  `json:"sink"`
	Protocol         string               `json:"protocol"`
	ProtocolSettings map[string]string    `json:"protocolsettings"`
}

type SubscriptionFilter struct {
	Exact  map[string]string    `json:"exact,omitempty"`
	Prefix map[string]string    `json:"prefix,omitempty"`
	Suffix map[string]string    `json:"suffix,omitempty"`
	SQL    string               `json:"sql,omitempty"`
	Not    *SubscriptionFilter  `json:"not,omitempty"`
	All    []SubscriptionFilter `json:"all,omitempty"`
	Any    []SubscriptionFilter `json:"any,omitempty"`
}

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
