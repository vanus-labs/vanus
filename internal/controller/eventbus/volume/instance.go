package volume

import (
	"context"
	"fmt"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/allocator"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"time"
)

type Instance interface {
	GetMeta() *Metadata
	ID() uint64
	CreateBlock(context.Context, int64) (*allocator.Block, error)
	DeleteBlock(context.Context, uint64) error
	ActivateBlock(context.Context, *allocator.Block) error
	Close() error
}

func newInstance(md *Metadata) Instance {
	return nil
}

type instance struct {
	md       *Metadata
	srv      Server
	grpcConn *grpc.ClientConn
	client   segment.SegmentServerClient
}

func (ins *instance) GetMeta() *Metadata {
	return ins.md
}

func (ins *instance) CreateBlock(ctx context.Context, cap int64) (*allocator.Block, error) {
	block := &Segment{
		//ID:       ins.generateSegmentBlockID(),
		Capacity: cap,
	}
	_, err := ins.client.CreateSegmentBlock(ctx, &segment.CreateSegmentBlockRequest{
		Size: block.Capacity,
		//Id:   block.ID,
	})
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (ins *instance) DeleteBlock(context.Context, uint64) error {
	return nil
}

func (ins *instance) UpdateSegmentServer(srv Server) {
	ins.srv = srv
}

func (ins *instance) ActivateBlock(ctx context.Context, block *allocator.Block) error {
	_, err := ins.client.ActiveSegmentBlock(ctx, &segment.ActiveSegmentBlockRequest{
		//EventLogId:     block.EventLogID,
		//ReplicaGroupId: block.ReplicaGroupID,
		//PeersAddress:   block.PeersAddress,
	})
	return err
}

func (ins *instance) Close() error {
	return ins.grpcConn.Close()
}

func (ins *instance) generateSegmentBlockID() string {
	//TODO optimize
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
