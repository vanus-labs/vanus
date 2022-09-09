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

package inmemory

import (
	// standard libraries.
	"context"
	"io"
	"testing"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
)

var elog *EventLog

func init() {
	cfg, err := eventlog.ParseVRN("vanus+inmemory:eventlog:test")
	if err != nil {
		panic("parse vrn failed")
	}
	elog = NewInMemoryLog(cfg)
}

func TestAppend(t *testing.T) {
	writer, err := elog.Writer()
	if err != nil {
		t.Error(err.Error())
	}

	event := ce.NewEvent()
	event.SetSource("example/uri")
	event.SetType("example.type")
	err = event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})
	if err != nil {
		t.Error(err.Error())
	}

	_, err = writer.Append(context.Background(), &event)
	if err != nil {
		t.Error(err.Error())
	}
}

func TestRead(t *testing.T) {
	reader, err := elog.Reader(eventlog.ReaderConfig{})
	if err != nil {
		t.Error(err.Error())
	}

	events, err := reader.Read(context.Background(), 5)
	if err != nil {
		t.Error(err.Error())
	}

	for _, e := range events {
		t.Logf("event: \n%s", e)
	}

	pos, err := reader.Seek(context.Background(), 0, io.SeekCurrent)
	if err != nil {
		t.Error(err.Error())
	}
	t.Logf("pos: %d", pos)
}
