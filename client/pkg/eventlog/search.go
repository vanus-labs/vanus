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

func SearchEventByID(ctx context.Context, eventID string, controllers string) (*event.Event, error) {
	logID, off, err := decodeEventID(eventID)
	if err != nil {
		return nil, err
	}
	vrn := fmt.Sprintf("vanus:///eventlog/%d?eventbus=%s&controllers=%s", logID, "", controllers)
	r, err := OpenReader(ctx, vrn, DisablePolling())
	if err != nil {
		return nil, err
	}
	_, err = r.Seek(ctx, off, io.SeekStart)
	if err != nil {
		return nil, err
	}
	events, err := r.Read(ctx, 1)
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
		return 0, 0, fmt.Errorf("invalid event id")
	}
	logID := binary.BigEndian.Uint64(decoded[0:8])
	off := binary.BigEndian.Uint64(decoded[8:16])
	return logID, int64(off), nil
}
