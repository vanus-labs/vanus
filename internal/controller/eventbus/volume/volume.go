package volume

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
)

type Instance interface {
	GetMeta() *Metadata
	CreateSegment(context.Context, int64) (*SegmentBlock, error)
	DeleteSegment(context.Context, uint64) error
	ActivateSegment(context.Context, *SegmentBlock) error
	UpdateSegmentServer(*info.SegmentServerInfo)
}

func newInstance(md *Metadata) Instance {
	return nil
}

type instance struct {
	md         *Metadata
	serverInfo *info.SegmentServerInfo
}

func (ins *instance) GetMeta() *Metadata {
	return ins.md
}

func (ins *instance) CreateSegment(context.Context, int64) (*SegmentBlock, error) {
	//client := pool.segmentServerMgr.getSegmentServerClient(srvInfo)
	//volume := pool.volumeMgr.lookupVolumeByServerID(srvInfo.ID())
	//segmentInfo := &info.SegmentBlock{
	//	ID:         pool.generateSegmentBlockID(),
	//	Capacity:   size,
	//	VolumeID:   volume.ID(),
	//	VolumeInfo: volume,
	//}
	//_, err := client.CreateSegmentBlock(ctx, &segment.CreateSegmentBlockRequest{
	//	Size: segmentInfo.Capacity,
	//	Id:   segmentInfo.ID,
	//})
	//if err != nil {
	//	return nil, err
	//}
	//volume.AddBlock(segmentInfo)
	//if err = pool.volumeMgr.updateVolumeInKV(ctx, volume); err != nil {
	//	return nil, err
	//}
	//
	//pool.segmentMap.Store(segmentInfo.ID, segmentInfo)
	//if err = pool.updateSegmentBlockInKV(ctx, segmentInfo); err != nil {
	//	return nil, err
	//}
	return nil, nil
}

func (ins *instance) DeleteSegment(context.Context, uint64) error {
	return nil
}

func (ins *instance) UpdateSegmentServer(inf *info.SegmentServerInfo) {
	ins.serverInfo = inf
}

func (ins *instance) ActivateSegment(context.Context, *SegmentBlock) error {
	//srvInfo := pool.segmentServerMgr.GetServerInfoByServerID(seg.VolumeInfo.SegmentServerID)
	//client := pool.segmentServerMgr.getSegmentServerClient(srvInfo)
	//if client == nil {
	//	return nil, errors.New("the segment server client not found")
	//}
	//_, err = client.ActiveSegmentBlock(ctx, &segment.ActiveSegmentBlockRequest{
	//	EventLogId:     seg.EventLogID,
	//	ReplicaGroupId: seg.ReplicaGroupID,
	//	PeersAddress:   seg.PeersAddress,
	//})
	return nil
}
