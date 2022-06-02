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

package record

type EventBus struct {
	VRN  string
	Logs []*EventLog
}

type EventLog struct {
	VRN  string
	Mode LogMode
}

func (l *EventLog) Readable() bool {
	return l.Mode.Readable()
}

func (l *EventLog) Writable() bool {
	return l.Mode.Writable()
}

// WritableLog return all writable logs.
func (b *EventBus) WritableLog() []*EventLog {
	if allWritable(b.Logs) {
		return b.Logs
	}

	logs := make([]*EventLog, 0, len(b.Logs))
	for _, l := range b.Logs {
		if l.Writable() {
			logs = append(logs, l)
		}
	}
	return logs
}

func allWritable(ls []*EventLog) bool {
	for _, l := range ls {
		if !l.Writable() {
			return false
		}
	}
	return true
}

// ReadableLog return all readable logs.
func (b *EventBus) ReadableLog() []*EventLog {
	if allReadable(b.Logs) {
		return b.Logs
	}

	logs := make([]*EventLog, 0, len(b.Logs))
	for _, l := range b.Logs {
		if l.Readable() {
			logs = append(logs, l)
		}
	}
	return logs
}

func allReadable(ls []*EventLog) bool {
	for _, l := range ls {
		if !l.Readable() {
			return false
		}
	}
	return true
}
