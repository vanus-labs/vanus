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

package log

import (
	// first-party libraries
	"github.com/linkall-labs/raft/raftpb"

	// this project
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

func RecoverLogsAndWAL(walDir string) (map[vanus.ID]*Log, *walog.WAL, error) {
	raftLogs := make(map[uint64]*Log)
	wal, err := walog.RecoverWithVisitor(walDir, func(data []byte, offset int64) error {
		var entry raftpb.Entry
		err := entry.Unmarshal(data)
		if err != nil {
			return err
		}

		raftLog := raftLogs[entry.NodeId]
		if raftLog == nil {
			// TODO(james.yin): peers
			raftLog = NewLog(vanus.NewIDFromUint64(entry.NodeId), nil, nil)
			dummy := &raftLog.ents[0]
			dummy.Index = entry.Index - 1
			// TODO(james.yin): set Term
			raftLogs[entry.NodeId] = raftLog
		}

		return raftLog.appendInRecovery(entry, offset)
	})
	if err != nil {
		return nil, nil, err
	}

	// convert raftLogs, and set wal
	raftLogs2 := make(map[vanus.ID]*Log, len(raftLogs))
	for nodeID, raftLog := range raftLogs {
		raftLog.wal = wal
		raftLogs2[vanus.NewIDFromUint64(nodeID)] = raftLog
	}

	return raftLogs2, wal, nil
}
