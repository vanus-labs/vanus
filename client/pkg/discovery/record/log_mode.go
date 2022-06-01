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

type LogMode uint32

const (
	ModePerm LogMode = 07
)

const (
	PremWrite LogMode = 1 << 1
	PremRead  LogMode = 1 << 2
)

func (m LogMode) Perm() LogMode {
	return m & ModePerm
}

func (m LogMode) Writable() bool {
	return m&PremWrite != 0
}

func (m LogMode) Readable() bool {
	return m&PremRead != 0
}
