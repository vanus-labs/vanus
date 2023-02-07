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

package raw_client

import (
	"context"
	"io"

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"google.golang.org/grpc"
)

var (
	_ io.Closer = (*eventlogClient)(nil)
)

func NewEventlogClient(cc *Conn) ctrlpb.EventLogControllerClient {
	return &eventlogClient{
		cc: cc,
	}
}

type eventlogClient struct {
	cc *Conn
}

func (elc *eventlogClient) Close() error {
	return elc.cc.close()
}

func (elc *eventlogClient) ListSegment(ctx context.Context,
	in *ctrlpb.ListSegmentRequest, opts ...grpc.CallOption) (*ctrlpb.ListSegmentResponse, error) {
	out := new(ctrlpb.ListSegmentResponse)
	err := elc.cc.invoke(ctx, "/linkall.vanus.controller.EventLogController/ListSegment", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (elc *eventlogClient) GetAppendableSegment(ctx context.Context,
	in *ctrlpb.GetAppendableSegmentRequest, opts ...grpc.CallOption) (*ctrlpb.GetAppendableSegmentResponse, error) {
	out := new(ctrlpb.GetAppendableSegmentResponse)
	err := elc.cc.invoke(ctx, "/linkall.vanus.controller.EventLogController/GetAppendableSegment", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (elc *eventlogClient) GetReadableSegment(ctx context.Context,
	in *ctrlpb.GetReadableSegmentRequest, opts ...grpc.CallOption) (*ctrlpb.GetReadableSegmentResponse, error) {
	out := new(ctrlpb.GetReadableSegmentResponse)
	err := elc.cc.invoke(ctx, "/linkall.vanus.controller.EventLogController/GetReadableSegment", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
