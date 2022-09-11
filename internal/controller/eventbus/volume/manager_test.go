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

package volume

import (
	stdCtx "context"
	stdJson "encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/pkg/util"
	"github.com/linkall-labs/vanus/proto/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestVolumeMgr_Init(t *testing.T) {
	Convey("test init volume manager", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		srvMgr := server.NewMockManager(ctrl)

		Convey("test load blocks of volume", func() {
			mgr := &volumeMgr{serverMgr: srvMgr}
			volume1 := &metadata.VolumeMetadata{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024 * 1024,
				Used:     4 * 64 * 1024 * 1024,
			}
			volume2 := &metadata.VolumeMetadata{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024 * 1024,
				Used:     3 * 64 * 1024 * 1024,
			}
			volume3 := &metadata.VolumeMetadata{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024 * 1024,
				Used:     0,
			}
			data1, _ := stdJson.Marshal(volume1)
			data2, _ := stdJson.Marshal(volume2)
			data3, _ := stdJson.Marshal(volume3)
			kvCli.EXPECT().List(gomock.Any(), gomock.Eq(metadata.VolumeKeyPrefixInKVStore)).Times(1).Return([]kv.Pair{
				{
					Key:   filepath.Join(metadata.VolumeKeyPrefixInKVStore, volume1.ID.String()),
					Value: data1,
				},
				{
					Key:   filepath.Join(metadata.VolumeKeyPrefixInKVStore, volume2.ID.String()),
					Value: data2,
				},
				{
					Key:   filepath.Join(metadata.VolumeKeyPrefixInKVStore, volume3.ID.String()),
					Value: data3,
				},
			}, nil)
			block1 := metadata.Block{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024,
				VolumeID: volume1.ID,
			}
			block2 := metadata.Block{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024,
				VolumeID: volume1.ID,
			}
			block3 := metadata.Block{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024,
				VolumeID: volume1.ID,
			}
			block4 := metadata.Block{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024,
				VolumeID: volume1.ID,
			}
			data1, _ = stdJson.Marshal(block1)
			data2, _ = stdJson.Marshal(block2)
			data3, _ = stdJson.Marshal(block3)
			data4, _ := stdJson.Marshal(block4)
			kvCli.EXPECT().List(gomock.Any(),
				filepath.Join(metadata.BlockKeyPrefixInKVStore, volume1.ID.Key())).Times(1).Return([]kv.Pair{
				{
					Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, volume1.ID.String(), block1.ID.String()),
					Value: data1,
				},
				{
					Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, volume1.ID.String(), block2.ID.String()),
					Value: data2,
				},
				{
					Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, volume1.ID.String(), block3.ID.String()),
					Value: data3,
				},
				{
					Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, volume1.ID.String(), block4.ID.String()),
					Value: data4,
				},
			}, nil)
			block1 = metadata.Block{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024,
				VolumeID: volume2.ID,
			}
			block2 = metadata.Block{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024,
				VolumeID: volume2.ID,
			}
			block3 = metadata.Block{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024,
				VolumeID: volume2.ID,
			}
			data1, _ = stdJson.Marshal(block1)
			data2, _ = stdJson.Marshal(block2)
			data3, _ = stdJson.Marshal(block3)

			kvCli.EXPECT().List(gomock.Any(),
				filepath.Join(metadata.BlockKeyPrefixInKVStore, volume2.ID.Key())).Times(1).Return([]kv.Pair{
				{
					Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, volume2.ID.String(), block1.ID.String()),
					Value: data1,
				},
				{
					Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, volume2.ID.String(), block2.ID.String()),
					Value: data2,
				},
				{
					Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, volume2.ID.String(), block3.ID.String()),
					Value: data3,
				},
			}, nil)
			kvCli.EXPECT().List(gomock.Any(), filepath.Join(metadata.BlockKeyPrefixInKVStore, volume3.ID.Key())).
				Times(1).Return(nil, nil)
			kvCli.EXPECT().List(gomock.Any(), metadata.VolumeInstanceKeyPrefixInKVStore).Times(1).Return(nil, nil)
			err := mgr.Init(stdCtx.Background(), kvCli)
			So(err, ShouldBeNil)

			v, exist := mgr.volInstanceMap.Load(volume1.ID.Key())
			So(exist, ShouldBeTrue)
			vol1 := v.(server.Instance)
			So(vol1.ID(), ShouldEqual, volume1.ID)
			So(vol1.Address(), ShouldBeEmpty)
			So(vol1.GetMeta().Blocks, ShouldHaveLength, 4)

			v, exist = mgr.volInstanceMap.Load(volume2.ID.Key())
			So(exist, ShouldBeTrue)
			vol2 := v.(server.Instance)
			So(vol2.ID(), ShouldEqual, volume2.ID)
			So(vol2.Address(), ShouldBeEmpty)
			So(vol2.GetMeta().Blocks, ShouldHaveLength, 3)

			v, exist = mgr.volInstanceMap.Load(volume3.ID.Key())
			So(exist, ShouldBeTrue)
			vol3 := v.(server.Instance)
			So(vol3.ID(), ShouldEqual, volume3.ID)
			So(vol3.Address(), ShouldBeEmpty)
			So(vol3.GetMeta().Blocks, ShouldHaveLength, 0)
			So(util.MapLen(&mgr.volInstanceMap), ShouldEqual, 3)
		})

		Convey("test load the server of volume instance", func() {
			volume1 := &metadata.VolumeMetadata{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024 * 1024,
				Used:     4 * 64 * 1024 * 1024,
				Blocks:   map[uint64]*metadata.Block{},
			}
			volume2 := &metadata.VolumeMetadata{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024 * 1024,
				Used:     3 * 64 * 1024 * 1024,
				Blocks:   map[uint64]*metadata.Block{},
			}
			data1, _ := stdJson.Marshal(volume1)
			data2, _ := stdJson.Marshal(volume2)
			kvCli.EXPECT().List(gomock.Any(), gomock.Eq(metadata.VolumeKeyPrefixInKVStore)).Times(1).Return([]kv.Pair{
				{
					Key:   filepath.Join(metadata.VolumeKeyPrefixInKVStore, volume1.ID.String()),
					Value: data1,
				},
				{
					Key:   filepath.Join(metadata.VolumeKeyPrefixInKVStore, volume2.ID.String()),
					Value: data2,
				},
			}, nil)
			kvCli.EXPECT().List(gomock.Any(), filepath.Join(metadata.BlockKeyPrefixInKVStore, volume1.ID.Key())).
				Times(1).Return([]kv.Pair{}, nil)
			kvCli.EXPECT().List(gomock.Any(), filepath.Join(metadata.BlockKeyPrefixInKVStore, volume2.ID.Key())).
				Times(1).Return([]kv.Pair{}, nil)
			o1 := new(struct {
				Address  string   `json:"address"`
				ServerID vanus.ID `json:"server_id"`
			})
			o1.Address = "127.0.0.1:10001"
			o1.ServerID = vanus.NewID()
			o2 := new(struct {
				Address  string   `json:"address"`
				ServerID vanus.ID `json:"server_id"`
			})
			o2.Address = "127.0.0.1:10002"
			o2.ServerID = vanus.NewID()
			o3 := new(struct {
				Address  string   `json:"address"`
				ServerID vanus.ID `json:"server_id"`
			})
			o3.Address = "127.0.0.1:10003"
			o3.ServerID = vanus.NewID()

			data1, _ = stdJson.Marshal(o1)
			data2, _ = stdJson.Marshal(o2)
			data3, _ := stdJson.Marshal(o3)

			kvCli.EXPECT().List(gomock.Any(), metadata.VolumeInstanceKeyPrefixInKVStore).Times(1).Return([]kv.Pair{
				{
					Key:   "invalid data",
					Value: []byte{0x1, 0x2, 0x3, 0x4},
				},
				{
					Key:   filepath.Join(metadata.VolumeInstanceKeyPrefixInKVStore, volume1.ID.String()),
					Value: data1,
				},
				{
					Key:   filepath.Join(metadata.VolumeInstanceKeyPrefixInKVStore, volume2.ID.String()),
					Value: data2,
				},
				{
					Key:   filepath.Join(metadata.VolumeInstanceKeyPrefixInKVStore, vanus.NewID().String()),
					Value: data3,
				},
				{
					//Key:   filepath.Join(metadata.VolumeInstanceKeyPrefixInKVStore, vanus.NewID().String()),
					//Value: data4,
				},
			}, nil)

			srv1 := server.NewMockServer(ctrl)
			srv2 := server.NewMockServer(ctrl)
			srv3 := server.NewMockServer(ctrl)

			srv1.EXPECT().IsActive(gomock.Any()).Times(3).Return(true)
			srv1.EXPECT().ID().Times(3).Return(o1.ServerID)
			srv1.EXPECT().Address().Times(2).Return(o1.Address)
			srv1.EXPECT().Uptime().Times(1).Return(time.Now())
			srv2.EXPECT().IsActive(gomock.Any()).Times(1).Return(false)

			server.MockServerGetter(func(id vanus.ID, addr string) (server.Server, error) {
				switch id.Uint64() {
				case o1.ServerID.Uint64():
					return srv1, nil
				case o2.ServerID.Uint64():
					return srv2, nil
				case o3.ServerID.Uint64():
					return srv3, nil
				default:
					return nil, errors.New("invalid server")
				}
			})
			defer server.MockReset()

			srvMgr.EXPECT().AddServer(stdCtx.Background(), srv1).Times(1).Return(nil)
			kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)
			mgr := &volumeMgr{serverMgr: srvMgr}

			err := mgr.Init(stdCtx.Background(), kvCli)
			So(err, ShouldBeNil)

			So(util.MapLen(&mgr.volInstanceMap), ShouldEqual, 2)
			So(util.MapLen(&mgr.volInstanceMapByServerID), ShouldEqual, 1)
			v, exist := mgr.volInstanceMap.Load(volume1.ID.Key())
			So(exist, ShouldBeTrue)
			So(v.(server.Instance).GetMeta(), ShouldResemble, volume1)

			v, exist = mgr.volInstanceMap.Load(volume2.ID.Key())
			So(exist, ShouldBeTrue)
			So(v.(server.Instance).GetMeta(), ShouldResemble, volume2)

			v, exist = mgr.volInstanceMapByServerID.Load(o1.ServerID.Key())
			So(exist, ShouldBeTrue)
			So(v.(server.Instance).GetMeta(), ShouldResemble, volume1)

			So(mgr.GetVolumeInstanceByID(volume1.ID).GetMeta(), ShouldResemble, volume1)
		})
	})
}

func TestVolumeMgr_RegisterVolume(t *testing.T) {
	Convey("test register volume", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		srvMgr := server.NewMockManager(ctrl)
		mgr := &volumeMgr{serverMgr: srvMgr, kvCli: kvCli}

		Convey("test RegisterVolume", func() {
			volume := &metadata.VolumeMetadata{
				ID:       vanus.NewID(),
				Capacity: 64 * 1024 * 1024 * 1024,
				Used:     4 * 64 * 1024 * 1024,
			}
			data, _ := stdJson.Marshal(volume)
			key := filepath.Join(metadata.VolumeKeyPrefixInKVStore, volume.ID.String())
			ctx := stdCtx.Background()
			kvCli.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
			ins, err := mgr.RegisterVolume(ctx, volume)
			So(err, ShouldBeNil)
			So(ins.GetMeta(), ShouldResemble, volume)
			_, err = mgr.RegisterVolume(ctx, volume)
			So(err, ShouldBeNil)
		})
	})
}

func TestVolumeMgr_GetAllActiveVolumes(t *testing.T) {
	Convey("test register volume", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		srvMgr := server.NewMockManager(ctrl)
		mgr := &volumeMgr{serverMgr: srvMgr, kvCli: kvCli}

		ins1 := server.NewMockInstance(ctrl)
		mgr.volInstanceMap.Store(vanus.NewID(), ins1)
		srv1 := server.NewMockServer(ctrl)
		ins1.EXPECT().GetServer().Times(2).Return(srv1)
		srv1.EXPECT().IsActive(gomock.Any()).Times(1).Return(true)

		ins2 := server.NewMockInstance(ctrl)
		mgr.volInstanceMap.Store(vanus.NewID(), ins2)
		srv2 := server.NewMockServer(ctrl)
		ins2.EXPECT().GetServer().Times(2).Return(srv2)
		srv2.EXPECT().IsActive(gomock.Any()).Times(1).Return(false)

		ins3 := server.NewMockInstance(ctrl)
		mgr.volInstanceMap.Store(vanus.NewID(), ins3)
		ins3.EXPECT().GetServer().Times(1).Return(nil)

		So(mgr.GetAllActiveVolumes(), ShouldHaveLength, 1)
	})
}

func TestVolumeMgr_UpdateRoutingAndLookup(t *testing.T) {
	Convey("get update routing and lookup by server id", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		srvMgr := server.NewMockManager(ctrl)
		mgr := &volumeMgr{serverMgr: srvMgr, kvCli: kvCli}

		ins1 := server.NewMockInstance(ctrl)
		srv1 := server.NewMockServer(ctrl)
		ins1.EXPECT().ID().Times(1).Return(vanus.NewID())
		srv1.EXPECT().IsActive(gomock.Any()).Times(1).Return(false)

		ins2 := server.NewMockInstance(ctrl)
		srv2 := server.NewMockServer(ctrl)
		ins2.EXPECT().ID().Times(2).Return(vanus.NewID())
		srv2.EXPECT().IsActive(gomock.Any()).Times(1).Return(true)
		id := vanus.NewID()
		srv2.EXPECT().ID().Times(2).Return(id)
		srv2.EXPECT().Address().Times(1).Return("127.0.0.1:10000")

		ins3 := server.NewMockInstance(ctrl)
		ins3.EXPECT().ID().Times(1).Return(vanus.NewID())

		ctx := stdCtx.Background()
		Convey("update routing", func() {
			kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)
			mgr.UpdateRouting(ctx, ins1, srv1)

			ins2.EXPECT().SetServer(srv2).Times(1).Return()
			mgr.UpdateRouting(ctx, ins2, srv2)

			kvCli.EXPECT().Delete(gomock.Any(), gomock.Any()).Times(1).Return(nil)
			mgr.UpdateRouting(ctx, ins3, nil)

			So(util.MapLen(&mgr.volInstanceMapByServerID), ShouldEqual, 1)
			So(util.MapLen(&mgr.volInstanceMap), ShouldEqual, 1)
			ins := mgr.LookupVolumeByServerID(id)
			So(ins, ShouldEqual, ins2)

			ins = mgr.LookupVolumeByServerID(vanus.NewID())
			So(ins, ShouldBeEmpty)
		})
	})
}

func TestVolumeMgr_GetBlocksOfVolume(t *testing.T) {
	Convey("get update routing and lookup by server id", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		srvMgr := server.NewMockManager(ctrl)
		mgr := NewVolumeManager(srvMgr)
		_mgr := mgr.(*volumeMgr)
		_mgr.kvCli = kvCli

		vol := &metadata.VolumeMetadata{
			ID:       vanus.NewID(),
			Capacity: 16 * 1024 * 1024 * 1024,
			Blocks:   map[uint64]*metadata.Block{},
		}
		ins := server.NewInstance(vol)
		ctx := stdCtx.Background()
		vID := vanus.NewID()
		key := strings.Join([]string{metadata.BlockKeyPrefixInKVStore, ins.ID().String()}, "/")
		block1 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: 64 * 1024 * 1024,
			VolumeID: vID,
		}
		block2 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: 64 * 1024 * 1024,
			VolumeID: vID,
		}
		block3 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: 64 * 1024 * 1024,
			VolumeID: vID,
		}
		block4 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: 64 * 1024 * 1024,
			VolumeID: vID,
		}
		data1, _ := stdJson.Marshal(block1)
		data2, _ := stdJson.Marshal(block2)
		data3, _ := stdJson.Marshal(block3)
		data4, _ := stdJson.Marshal(block4)

		kvCli.EXPECT().List(gomock.Any(), key).Times(1).Return([]kv.Pair{
			{
				Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, vID.String(), block1.ID.String()),
				Value: data1,
			},
			{
				Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, vID.String(), block2.ID.String()),
				Value: data2,
			},
			{
				Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, vID.String(), block3.ID.String()),
				Value: data3,
			},
			{
				Key:   filepath.Join(metadata.BlockKeyPrefixInKVStore, vID.String(), block4.ID.String()),
				Value: data4,
			},
		}, nil)
		blocks, err := _mgr.GetBlocksOfVolume(ctx, ins)
		So(err, ShouldBeNil)
		So(blocks, ShouldHaveLength, 4)
	})
}
