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

package eventlog

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cloudevents/sdk-go/v2/event"
)

// OpenWriter open a Writer of EventLog identified by vrn.
func OpenWriter(vrn string) (LogWriter, error) {
	el, err := Get(vrn)
	if err != nil {
		return nil, err
	}
	defer Put(el)

	w, err := el.Writer()
	if err != nil {
		return nil, err
	}

	return w, nil
}

// OpenReader open a Reader of EventLog identified by vrn.
func OpenReader(vrn string) (LogReader, error) {
	el, err := Get(vrn)
	if err != nil {
		return nil, err
	}
	defer Put(el)

	r, err := el.Reader()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func SearchEventByID(eventID string, controllers string) (*event.Event, error) {
	logID, off, err := decodeEventID(eventID)
	if err != nil {
		return nil, err
	}
	vrn := fmt.Sprintf("vanus:///eventlog/%d?eventbus=%s&controllers=%s", logID, "", controllers)
	r, err := OpenReader(vrn)
	if err != nil {
		return nil, err
	}
	_, err = r.Seek(context.Background(), off, io.SeekStart)
	if err != nil {
		return nil, err
	}
	events, err := r.Read(context.Background(), 1)
	if err != nil {
		return nil, err
	}
	return events[0], err
}

func decodeEventID(eventID string) (uint64, int64, error) {
	decoded, err := base64.StdEncoding.DecodeString(eventID)
	if err != nil {
		return 0, 0, err
	}
	if len(decoded) != 16 { // fixed length
		return 0, 0, fmt.Errorf("the length of decoded bytes is incorrect")
	}
	logID := binary.BigEndian.Uint64(decoded[0:8])
	off := binary.BigEndian.Uint64(decoded[8:16])
	return logID, int64(off), nil
}
