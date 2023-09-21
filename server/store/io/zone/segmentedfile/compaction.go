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

package segmentedfile

import (
	// standard libraries.
	"os"
)

// Compact compacts all segments whose end offset is not after off.
func (sf *SegmentedFile) Compact(off int64) error {
	var compacted []*Segment
	defer func() {
		if compacted != nil {
			go doCompact(compacted)
		}
	}()

	sf.mu.Lock()
	defer sf.mu.Unlock()

	sz := len(sf.segments)
	if sz <= 1 {
		return nil
	}

	for i, s := range sf.segments[:sz-1] {
		if s.eo > off {
			if i > 0 {
				compacted = sf.segments[:i]
				sf.segments = sf.segments[i:]
			}
			return nil
		}
	}
	compacted = sf.segments[:sz-1]
	sf.segments = sf.segments[sz-1:]
	return nil
}

func doCompact(segments []*Segment) {
	for _, s := range segments {
		_ = s.Close()
		_ = os.Remove(s.path)
	}
}
