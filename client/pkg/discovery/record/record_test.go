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

import (
	"testing"
)

func TestLogRecordRW(t *testing.T) {
	l := EventLog{
		VRN:  "",
		Mode: PremWrite | PremRead,
	}
	if !l.Readable() {
		t.Errorf("l.Readable() != true")
	}
	if !l.Writable() {
		t.Errorf("l.Writable() != true")
	}
}

func TestLogRecordRO(t *testing.T) {
	l := EventLog{
		VRN:  "",
		Mode: PremRead,
	}
	if !l.Readable() {
		t.Errorf("l.Readable() != true")
	}
	if l.Writable() {
		t.Errorf("l.Writable() != false")
	}
}

func TestLogRecordWO(t *testing.T) {
	l := EventLog{
		VRN:  "",
		Mode: PremWrite,
	}
	if l.Readable() {
		t.Errorf("l.Readable() != false")
	}
	if !l.Writable() {
		t.Errorf("l.Writable() != true")
	}
}
