package volume

import (
	"context"
	"fmt"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"time"
)

type Instance interface {
	GetMeta() *Metadata
	CreateSegment(context.Context, int64) (*SegmentBlock, error)
	DeleteSegment(context.Context, uint64) error
	ActivateSegment(context.Context, *SegmentBlock) error
	//UpdateSegmentServer(*info.SegmentServerInfo)
}

func newInstance(md *Metadata) Instance {
	return nil
}

type instance struct {
	md     *Metadata
	srv    Server
	client segment.SegmentServerClient
}

func (ins *instance) GetMeta() *Metadata {
	return ins.md
}

func (ins *instance) CreateSegment(ctx context.Context, cap int64) (*SegmentBlock, error) {
	block := &SegmentBlock{
		ID:       ins.generateSegmentBlockID(),
		Capacity: cap,
	}
	_, err := ins.client.CreateSegmentBlock(ctx, &segment.CreateSegmentBlockRequest{
		Size: block.Capacity,
		Id:   block.ID,
	})
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (ins *instance) DeleteSegment(context.Context, uint64) error {
	return nil
}

func (ins *instance) UpdateSegmentServer(srv Server) {
	ins.srv = srv
}

func (ins *instance) ActivateSegment(ctx context.Context, block *SegmentBlock) error {
	_, err := ins.client.ActiveSegmentBlock(ctx, &segment.ActiveSegmentBlockRequest{
		EventLogId:     block.EventLogID,
		ReplicaGroupId: block.ReplicaGroupID,
		PeersAddress:   block.PeersAddress,
	})
	return err
}

func (ins *instance) generateSegmentBlockID() string {
	//TODO optimize
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
