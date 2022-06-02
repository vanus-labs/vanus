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

package eventbus

import (
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"go.uber.org/atomic"
)

type WriterPicker interface {
	Pick(event *ce.Event, writers []eventlog.LogWriter) eventlog.LogWriter
}

type RoundRobinPick struct {
	i atomic.Uint64
}

func (p *RoundRobinPick) Pick(event *ce.Event, writers []eventlog.LogWriter) eventlog.LogWriter {
	sz := len(writers)
	if sz == 1 {
		return writers[0]
	} else if sz == 0 {
		return nil
	}
	i := p.i.Inc() % uint64(sz)
	return writers[i]
}
