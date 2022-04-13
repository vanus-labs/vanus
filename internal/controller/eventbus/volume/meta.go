package volume

import (
	"github.com/linkall-labs/vsproto/pkg/meta"
)

type Metadata struct {
	ID           string            `json:"id""`
	Capacity     int64             `json:"capacity"`
	Used         int64             `json:"used"`
	BlockNumbers int               `json:"block_numbers"`
	Blocks       map[string]string `json:"blocks"`
}

type SegmentState string

const (
	StateCreated  = SegmentState("created")
	StateWorking  = SegmentState("working")
	StateFrozen   = SegmentState("frozen")
	StateArchived = SegmentState("archived")
	StateExpired  = SegmentState("expired")
)

type Segment struct {
	ID                uint64       `json:"id"`
	State             SegmentState `json:"state"`
	Capacity          int64        `json:"capacity"`
	Size              int64        `json:"size"`
	VolumeID          uint64       `json:"volume_id"`
	EventLogID        uint64       `json:"event_log_id"`
	Number            int32        `json:"number"`
	PreviousSegmentId uint64       `json:"previous_segment_id"`
	NextSegmentId     uint64       `json:"next_segment_id"`
	StartOffsetInLog  int64        `json:"start_offset_in_log"`
}

type ReplicaGroup struct {
	ReplicaGroupID string   `json:"replica_group_id"`
	PeersAddress   []string `json:"peers_address"`
}

func Convert2ProtoSegment(ins ...*Segment) []*meta.Segment {
	segs := make([]*meta.Segment, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		seg := ins[idx]
		// TODO optimize reported info
		segs[idx] = &meta.Segment{
			//Id:                seg.ID,
			//PreviousSegmentId: seg.PreviousSegmentId,
			//NextSegmentId:     seg.NextSegmentId,
			//EventLogId:        seg.EventLogID,
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number) - 1,
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
			Tier:              meta.StorageTier_SSD,
		}
		if segs[idx].NumberEventStored == 0 {
			segs[idx].EndOffsetInLog = -1
		}
		//if seg.Metadata != nil && seg.Metadata.assignedSegmentServer != nil {
		//	segs[idx].StorageUri = seg.Metadata.assignedSegmentServer.Address
		//}
	}
	return segs
}
