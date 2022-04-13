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

type SegmentBlock struct {
	ID                string   `json:"id"`
	Capacity          int64    `json:"capacity"`
	Size              int64    `json:"size"`
	VolumeID          string   `json:"volume_id"`
	EventLogID        string   `json:"event_log_id"`
	ReplicaGroupID    string   `json:"replica_group_id"`
	PeersAddress      []string `json:"peers_address"`
	Number            int32    `json:"number"`
	PreviousSegmentId string   `json:"previous_segment_id"`
	NextSegmentId     string   `json:"next_segment_id"`
	StartOffsetInLog  int64    `json:"start_offset_in_log"`
	IsFull            bool     `json:"is_full"`
}

func Convert2ProtoSegment(ins ...*SegmentBlock) []*meta.Segment {
	segs := make([]*meta.Segment, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		seg := ins[idx]
		// TODO optimize reported info
		segs[idx] = &meta.Segment{
			Id:                seg.ID,
			PreviousSegmentId: seg.PreviousSegmentId,
			NextSegmentId:     seg.NextSegmentId,
			EventLogId:        seg.EventLogID,
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number) - 1,
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
			Tier:              meta.StorageTier_SSD,
			IsFull:            seg.IsFull,
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
