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

package worker

import (
	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/eventlog"
	"github.com/linkall-labs/vanus/observability/log"
	"io"
)

type Consumer struct {
	ebVrn string
	lrs   map[eventlog.LogReader]int64
}

func NewConsumer(ebVRN string) (*Consumer, error) {
	ls, err := eb.LookupReadableLog(ebVRN)
	if err != nil {
		return nil, err
	}
	lrs := make(map[eventlog.LogReader]int64, len(ls))
	for i := range ls {
		lr, err := eb.OpenLogReader(ls[i].VRN)
		if err != nil {
			return nil, err
		}
		lr.Log().UseCount()
		lr.Seek(0, io.SeekStart)
		lrs[lr] = 0
	}
	return &Consumer{
		ebVrn: ebVRN,
		lrs:   lrs,
	}, nil
}

func (c *Consumer) messages(size int16) []*ce.Event {
	var events []*ce.Event
	for r := range c.lrs {
		e, err := r.Read(size)
		if err != nil {
			log.Error("lr read error", map[string]interface{}{"error": err})
			return events
		}
		events = append(events, e...)
		size = size - int16(len(e))
		if size == 0 {
			return events
		}
	}
	return events
}
