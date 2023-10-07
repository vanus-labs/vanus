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

package pkg

import (
	// standard libraries.
	"encoding/json"
	"fmt"

	// first-party libraries.
	vanus "github.com/vanus-labs/vanus/api/vsr"

	// this project.
	"github.com/vanus-labs/vanus/pkg/info"
)

type URI string

type Subscription struct {
	ID                   vanus.ID               `json:"id"`
	Filters              SubscriptionFilterList `json:"filters,omitempty"`
	Sink                 URI                    `json:"sink,omitempty"`
	EventbusID           vanus.ID               `json:"eventbus"`
	DeadLetterEventbusID vanus.ID               `json:"dead_letter_eventbus_id"`
	RetryEventbusID      vanus.ID               `json:"retry_eventbus_id"`
	TimerEventbusID      vanus.ID               `json:"timer_eventbus_id"`
	Offsets              info.ListOffsetInfo    `json:"offsets"`
	Transformer          *Transformer           `json:"transformer,omitempty"`
	Config               SubscriptionConfig     `json:"config,omitempty"`
	Protocol             Protocol               `json:"protocol,omitempty"`
	ProtocolSetting      *ProtocolSetting       `json:"protocol_setting,omitempty"`
	SinkCredential       SinkCredential         `json:"sink_credential,omitempty"`
}

func (sub *Subscription) String() string {
	return fmt.Sprintf("VolumeID=%d, sink=%s, eventbus=%s, filters=%s, offsets=%s, transformer=%s, config=%s, protocol=%v",
		sub.ID, sub.Sink, sub.EventbusID, sub.Filters.String(), sub.Offsets.String(),
		sub.Transformer.String(), sub.Config.String(), sub.Protocol)
}

type Protocol string

const (
	HTTPProtocol      Protocol = "http"
	AwsLambdaProtocol Protocol = "aws-lambda"
	GCloudFunctions   Protocol = "gcloud-functions"
	GRPC              Protocol = "grpc"
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
	RateLimit uint32 `json:"rate_limit,omitempty"`
	// consumer from
	OffsetType        OffsetType `json:"offset_type,omitempty"`
	OffsetTimestamp   *uint64    `json:"offset_timestamp,omitempty"`
	DeliveryTimeout   uint32     `json:"delivery_timeout,omitempty"`
	MaxRetryAttempts  *uint32    `json:"max_retry_attempts,omitempty"`
	DisableDeadLetter bool       `json:"disable_dead_letter,omitempty"`
	// send event with ordered
	OrderedEvent bool `json:"ordered_event"`
}

// GetMaxRetryAttempts return MaxRetryAttempts if nil return -1.
func (c *SubscriptionConfig) GetMaxRetryAttempts() int32 {
	if c != nil && c.MaxRetryAttempts != nil {
		return int32(*c.MaxRetryAttempts)
	}
	return -1
}

func (c *SubscriptionConfig) String() string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return string(b)
}

type SubscriptionFilter struct {
	Exact  map[string]string      `json:"exact,omitempty"`
	Prefix map[string]string      `json:"prefix,omitempty"`
	Suffix map[string]string      `json:"suffix,omitempty"`
	CeSQL  string                 `json:"ce_sql,omitempty"`
	Not    *SubscriptionFilter    `json:"not,omitempty"`
	All    SubscriptionFilterList `json:"all,omitempty"`
	Any    SubscriptionFilterList `json:"any,omitempty"`
	CEL    string                 `json:"cel,omitempty"`
}

type SubscriptionFilterList []*SubscriptionFilter

func (l SubscriptionFilterList) String() string {
	if len(l) == 0 {
		return ""
	}
	b, _ := json.Marshal(l)
	return string(b)
}

type TemplateType string

const (
	TemplateTypeUnspecified TemplateType = ""
	TemplateTypeNone        TemplateType = "none"
	TemplateTypeText        TemplateType = "text"
	TemplateTypeJSON        TemplateType = "json"
)

type TemplateConfig struct {
	Type     TemplateType `json:"template_type,omitempty"`
	Template string       `json:"template,omitempty"`
}

func (tc *TemplateConfig) RecognizeTemplateType() (TemplateType, bool) {
	switch tc.Type {
	case TemplateTypeUnspecified:
		// Compatible with v0.8.0 and below.
		if tc.Template == "" {
			return TemplateTypeNone, true
		}
		switch tc.Template[0] {
		case '{', '[', '"':
			return TemplateTypeJSON, true
		default:
			return TemplateTypeText, true
		}
	case TemplateTypeNone, TemplateTypeText, TemplateTypeJSON:
		return tc.Type, true
	default:
		return TemplateTypeText, false // unreachable
	}
}

type Transformer struct {
	Define   map[string]string `json:"define,omitempty"`
	Pipeline []*Action         `json:"pipeline,omitempty"`
	Template TemplateConfig    `json:",inline"`
}

func (t *Transformer) String() string {
	if t == nil {
		return ""
	}
	b, _ := json.Marshal(t)
	return string(b)
}

func (t *Transformer) Exist() bool {
	if t == nil {
		return false
	}
	tt, _ := t.Template.RecognizeTemplateType()
	if tt == TemplateTypeNone && len(t.Pipeline) == 0 {
		return false
	}
	return true
}

type Action struct {
	Command []interface{} `json:"command"`
}

/* annotation no use code .
type SinkSpec struct {
	Type   string
	NodeName   string // TODO use id or name? VolumeID used in CloudEvents Specification
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
	NodeName    string
	Exp     interface{}
	ApplyTo []string
	Target  TargetSpec
}

type SubscriptionSpec struct {
	Eventbuses []string
	Sinks      []SinkSpec
	Filters    []FilterSpec
}

type TargetSpec struct {
	LBStrategy string
	Sinks      []string
}
*/
