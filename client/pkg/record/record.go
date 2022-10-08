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

type Eventbus struct {
	Name string
	Logs []*Eventlog
}

type Eventlog struct {
	ID   uint64
	Mode LogMode
}

func (l *Eventlog) Readable() bool {
	return l.Mode.Readable()
}

func (l *Eventlog) Writable() bool {
	return l.Mode.Writable()
}

// WritableLog return all writable logs.
func (b *Eventbus) WritableLog() []*Eventlog {
	if allWritable(b.Logs) {
		return b.Logs
	}

	logs := make([]*Eventlog, 0, len(b.Logs))
	for _, l := range b.Logs {
		if l.Writable() {
			logs = append(logs, l)
		}
	}
	return logs
}

func allWritable(ls []*Eventlog) bool {
	for _, l := range ls {
		if !l.Writable() {
			return false
		}
	}
	return true
}

// ReadableLog return all readable logs.
func (b *Eventbus) ReadableLog() []*Eventlog {
	if allReadable(b.Logs) {
		return b.Logs
	}

	logs := make([]*Eventlog, 0, len(b.Logs))
	for _, l := range b.Logs {
		if l.Readable() {
			logs = append(logs, l)
		}
	}
	return logs
}

func allReadable(ls []*Eventlog) bool {
	for _, l := range ls {
		if !l.Readable() {
			return false
		}
	}
	return true
}

type Segment struct {
	ID          uint64
	StartOffset int64
	EndOffset   int64
	Writable    bool

	Blocks        map[uint64]*Block
	LeaderBlockID uint64
}

type Block struct {
	ID       uint64
	Endpoint string
}

func (s *Segment) GetLeaderEndpoint() string {
	return s.Blocks[s.LeaderBlockID].Endpoint
}
