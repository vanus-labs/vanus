// Code generated by MockGen. DO NOT EDIT.
// Source: replica.go
//
// Generated by this command:
//
//	mockgen -source=replica.go -destination=mock_replica.go -package=segment
//

// Package segment is a generated GoMock package.
package segment

import (
	context "context"
	reflect "reflect"

	meta "github.com/vanus-labs/vanus/api/meta"
	vsr "github.com/vanus-labs/vanus/api/vsr"
	block "github.com/vanus-labs/vanus/server/store/block"
	block0 "github.com/vanus-labs/vanus/server/store/raft/block"
	gomock "go.uber.org/mock/gomock"
)

// MockReplica is a mock of Replica interface.
type MockReplica struct {
	ctrl     *gomock.Controller
	recorder *MockReplicaMockRecorder
}

// MockReplicaMockRecorder is the mock recorder for MockReplica.
type MockReplicaMockRecorder struct {
	mock *MockReplica
}

// NewMockReplica creates a new mock instance.
func NewMockReplica(ctrl *gomock.Controller) *MockReplica {
	mock := &MockReplica{ctrl: ctrl}
	mock.recorder = &MockReplicaMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReplica) EXPECT() *MockReplicaMockRecorder {
	return m.recorder
}

// Append mocks base method.
func (m *MockReplica) Append(ctx context.Context, entries []block.Entry, cb block.AppendCallback) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Append", ctx, entries, cb)
}

// Append indicates an expected call of Append.
func (mr *MockReplicaMockRecorder) Append(ctx, entries, cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Append", reflect.TypeOf((*MockReplica)(nil).Append), ctx, entries, cb)
}

// Bootstrap mocks base method.
func (m *MockReplica) Bootstrap(ctx context.Context, blocks []block0.Peer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Bootstrap", ctx, blocks)
	ret0, _ := ret[0].(error)
	return ret0
}

// Bootstrap indicates an expected call of Bootstrap.
func (mr *MockReplicaMockRecorder) Bootstrap(ctx, blocks any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Bootstrap", reflect.TypeOf((*MockReplica)(nil).Bootstrap), ctx, blocks)
}

// Close mocks base method.
func (m *MockReplica) Close(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockReplicaMockRecorder) Close(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockReplica)(nil).Close), ctx)
}

// Delete mocks base method.
func (m *MockReplica) Delete(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockReplicaMockRecorder) Delete(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockReplica)(nil).Delete), ctx)
}

// ID mocks base method.
func (m *MockReplica) ID() vsr.ID {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(vsr.ID)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockReplicaMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockReplica)(nil).ID))
}

// IDStr mocks base method.
func (m *MockReplica) IDStr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IDStr")
	ret0, _ := ret[0].(string)
	return ret0
}

// IDStr indicates an expected call of IDStr.
func (mr *MockReplicaMockRecorder) IDStr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IDStr", reflect.TypeOf((*MockReplica)(nil).IDStr))
}

// Read mocks base method.
func (m *MockReplica) Read(ctx context.Context, seq int64, num int) ([]block.Entry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", ctx, seq, num)
	ret0, _ := ret[0].([]block.Entry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockReplicaMockRecorder) Read(ctx, seq, num any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockReplica)(nil).Read), ctx, seq, num)
}

// Seek mocks base method.
func (m *MockReplica) Seek(ctx context.Context, index int64, key block.Entry, flag block.SeekKeyFlag) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Seek", ctx, index, key, flag)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Seek indicates an expected call of Seek.
func (mr *MockReplicaMockRecorder) Seek(ctx, index, key, flag any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Seek", reflect.TypeOf((*MockReplica)(nil).Seek), ctx, index, key, flag)
}

// Status mocks base method.
func (m *MockReplica) Status() *meta.SegmentHealthInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Status")
	ret0, _ := ret[0].(*meta.SegmentHealthInfo)
	return ret0
}

// Status indicates an expected call of Status.
func (mr *MockReplicaMockRecorder) Status() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Status", reflect.TypeOf((*MockReplica)(nil).Status))
}
