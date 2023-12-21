// Code generated by MockGen. DO NOT EDIT.
// Source: eventlog.go
//
// Generated by this command:
//
//	mockgen -source=eventlog.go -destination=mock_eventlog.go -package=eventlog
//

// Package eventlog is a generated GoMock package.
package eventlog

import (
	context "context"
	reflect "reflect"

	vsr "github.com/vanus-labs/vanus/api/vsr"
	kv "github.com/vanus-labs/vanus/pkg/kv"
	metadata "github.com/vanus-labs/vanus/server/controller/eventbus/metadata"
	gomock "go.uber.org/mock/gomock"
)

// MockManager is a mock of Manager interface.
type MockManager struct {
	ctrl     *gomock.Controller
	recorder *MockManagerMockRecorder
}

// MockManagerMockRecorder is the mock recorder for MockManager.
type MockManagerMockRecorder struct {
	mock *MockManager
}

// NewMockManager creates a new mock instance.
func NewMockManager(ctrl *gomock.Controller) *MockManager {
	mock := &MockManager{ctrl: ctrl}
	mock.recorder = &MockManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockManager) EXPECT() *MockManagerMockRecorder {
	return m.recorder
}

// AcquireEventlog mocks base method.
func (m *MockManager) AcquireEventlog(ctx context.Context, eventbusID vsr.ID, eventbusName string) (*metadata.Eventlog, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AcquireEventlog", ctx, eventbusID, eventbusName)
	ret0, _ := ret[0].(*metadata.Eventlog)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AcquireEventlog indicates an expected call of AcquireEventlog.
func (mr *MockManagerMockRecorder) AcquireEventlog(ctx, eventbusID, eventbusName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AcquireEventlog", reflect.TypeOf((*MockManager)(nil).AcquireEventlog), ctx, eventbusID, eventbusName)
}

// DeleteEventlog mocks base method.
func (m *MockManager) DeleteEventlog(ctx context.Context, id vsr.ID) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeleteEventlog", ctx, id)
}

// DeleteEventlog indicates an expected call of DeleteEventlog.
func (mr *MockManagerMockRecorder) DeleteEventlog(ctx, id any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteEventlog", reflect.TypeOf((*MockManager)(nil).DeleteEventlog), ctx, id)
}

// GetAppendableSegment mocks base method.
func (m *MockManager) GetAppendableSegment(ctx context.Context, eli *metadata.Eventlog, num int) ([]Segment, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAppendableSegment", ctx, eli, num)
	ret0, _ := ret[0].([]Segment)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAppendableSegment indicates an expected call of GetAppendableSegment.
func (mr *MockManagerMockRecorder) GetAppendableSegment(ctx, eli, num any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAppendableSegment", reflect.TypeOf((*MockManager)(nil).GetAppendableSegment), ctx, eli, num)
}

// GetBlock mocks base method.
func (m *MockManager) GetBlock(id vsr.ID) *metadata.Block {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBlock", id)
	ret0, _ := ret[0].(*metadata.Block)
	return ret0
}

// GetBlock indicates an expected call of GetBlock.
func (mr *MockManagerMockRecorder) GetBlock(id any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBlock", reflect.TypeOf((*MockManager)(nil).GetBlock), id)
}

// GetEventlog mocks base method.
func (m *MockManager) GetEventlog(ctx context.Context, id vsr.ID) *metadata.Eventlog {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEventlog", ctx, id)
	ret0, _ := ret[0].(*metadata.Eventlog)
	return ret0
}

// GetEventlog indicates an expected call of GetEventlog.
func (mr *MockManagerMockRecorder) GetEventlog(ctx, id any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEventlog", reflect.TypeOf((*MockManager)(nil).GetEventlog), ctx, id)
}

// GetEventlogSegmentList mocks base method.
func (m *MockManager) GetEventlogSegmentList(elID vsr.ID) []Segment {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEventlogSegmentList", elID)
	ret0, _ := ret[0].([]Segment)
	return ret0
}

// GetEventlogSegmentList indicates an expected call of GetEventlogSegmentList.
func (mr *MockManagerMockRecorder) GetEventlogSegmentList(elID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEventlogSegmentList", reflect.TypeOf((*MockManager)(nil).GetEventlogSegmentList), elID)
}

// GetSegmentByBlockID mocks base method.
func (m *MockManager) GetSegmentByBlockID(block *metadata.Block) (Segment, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSegmentByBlockID", block)
	ret0, _ := ret[0].(Segment)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSegmentByBlockID indicates an expected call of GetSegmentByBlockID.
func (mr *MockManagerMockRecorder) GetSegmentByBlockID(block any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSegmentByBlockID", reflect.TypeOf((*MockManager)(nil).GetSegmentByBlockID), block)
}

// Run mocks base method.
func (m *MockManager) Run(ctx context.Context, kvClient kv.Client, startTask bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", ctx, kvClient, startTask)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockManagerMockRecorder) Run(ctx, kvClient, startTask any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockManager)(nil).Run), ctx, kvClient, startTask)
}

// Stop mocks base method.
func (m *MockManager) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockManagerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockManager)(nil).Stop))
}

// UpdateSegment mocks base method.
func (m_2 *MockManager) UpdateSegment(ctx context.Context, m map[string][]Segment) {
	m_2.ctrl.T.Helper()
	m_2.ctrl.Call(m_2, "UpdateSegment", ctx, m)
}

// UpdateSegment indicates an expected call of UpdateSegment.
func (mr *MockManagerMockRecorder) UpdateSegment(ctx, m any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateSegment", reflect.TypeOf((*MockManager)(nil).UpdateSegment), ctx, m)
}

// UpdateSegmentReplicas mocks base method.
func (m *MockManager) UpdateSegmentReplicas(ctx context.Context, segID vsr.ID, term uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateSegmentReplicas", ctx, segID, term)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateSegmentReplicas indicates an expected call of UpdateSegmentReplicas.
func (mr *MockManagerMockRecorder) UpdateSegmentReplicas(ctx, segID, term any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateSegmentReplicas", reflect.TypeOf((*MockManager)(nil).UpdateSegmentReplicas), ctx, segID, term)
}
