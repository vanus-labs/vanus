// Code generated by MockGen. DO NOT EDIT.
// Source: worker.go

// Package worker is a generated GoMock package.
package worker

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	metadata "github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	vanus "github.com/linkall-labs/vanus/internal/primitive/vanus"
)

// MockTriggerWorker is a mock of TriggerWorker interface.
type MockTriggerWorker struct {
	ctrl     *gomock.Controller
	recorder *MockTriggerWorkerMockRecorder
}

// MockTriggerWorkerMockRecorder is the mock recorder for MockTriggerWorker.
type MockTriggerWorkerMockRecorder struct {
	mock *MockTriggerWorker
}

// NewMockTriggerWorker creates a new mock instance.
func NewMockTriggerWorker(ctrl *gomock.Controller) *MockTriggerWorker {
	mock := &MockTriggerWorker{ctrl: ctrl}
	mock.recorder = &MockTriggerWorkerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTriggerWorker) EXPECT() *MockTriggerWorkerMockRecorder {
	return m.recorder
}

// AssignSubscription mocks base method.
func (m *MockTriggerWorker) AssignSubscription(id vanus.ID) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AssignSubscription", id)
}

// AssignSubscription indicates an expected call of AssignSubscription.
func (mr *MockTriggerWorkerMockRecorder) AssignSubscription(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AssignSubscription", reflect.TypeOf((*MockTriggerWorker)(nil).AssignSubscription), id)
}

// Close mocks base method.
func (m *MockTriggerWorker) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockTriggerWorkerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockTriggerWorker)(nil).Close))
}

// GetAddr mocks base method.
func (m *MockTriggerWorker) GetAddr() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAddr")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetAddr indicates an expected call of GetAddr.
func (mr *MockTriggerWorkerMockRecorder) GetAddr() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAddr", reflect.TypeOf((*MockTriggerWorker)(nil).GetAddr))
}

// GetAssignedSubscriptions mocks base method.
func (m *MockTriggerWorker) GetAssignedSubscriptions() []vanus.ID {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAssignedSubscriptions")
	ret0, _ := ret[0].([]vanus.ID)
	return ret0
}

// GetAssignedSubscriptions indicates an expected call of GetAssignedSubscriptions.
func (mr *MockTriggerWorkerMockRecorder) GetAssignedSubscriptions() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAssignedSubscriptions", reflect.TypeOf((*MockTriggerWorker)(nil).GetAssignedSubscriptions))
}

// GetHeartbeatTime mocks base method.
func (m *MockTriggerWorker) GetHeartbeatTime() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHeartbeatTime")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// GetHeartbeatTime indicates an expected call of GetHeartbeatTime.
func (mr *MockTriggerWorkerMockRecorder) GetHeartbeatTime() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHeartbeatTime", reflect.TypeOf((*MockTriggerWorker)(nil).GetHeartbeatTime))
}

// GetInfo mocks base method.
func (m *MockTriggerWorker) GetInfo() metadata.TriggerWorkerInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetInfo")
	ret0, _ := ret[0].(metadata.TriggerWorkerInfo)
	return ret0
}

// GetInfo indicates an expected call of GetInfo.
func (mr *MockTriggerWorkerMockRecorder) GetInfo() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetInfo", reflect.TypeOf((*MockTriggerWorker)(nil).GetInfo))
}

// GetPendingTime mocks base method.
func (m *MockTriggerWorker) GetPendingTime() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPendingTime")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// GetPendingTime indicates an expected call of GetPendingTime.
func (mr *MockTriggerWorkerMockRecorder) GetPendingTime() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPendingTime", reflect.TypeOf((*MockTriggerWorker)(nil).GetPendingTime))
}

// GetPhase mocks base method.
func (m *MockTriggerWorker) GetPhase() metadata.TriggerWorkerPhase {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPhase")
	ret0, _ := ret[0].(metadata.TriggerWorkerPhase)
	return ret0
}

// GetPhase indicates an expected call of GetPhase.
func (mr *MockTriggerWorkerMockRecorder) GetPhase() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPhase", reflect.TypeOf((*MockTriggerWorker)(nil).GetPhase))
}

// IsActive mocks base method.
func (m *MockTriggerWorker) IsActive() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsActive")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsActive indicates an expected call of IsActive.
func (mr *MockTriggerWorkerMockRecorder) IsActive() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsActive", reflect.TypeOf((*MockTriggerWorker)(nil).IsActive))
}

// Polish mocks base method.
func (m *MockTriggerWorker) Polish() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Polish")
}

// Polish indicates an expected call of Polish.
func (mr *MockTriggerWorkerMockRecorder) Polish() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Polish", reflect.TypeOf((*MockTriggerWorker)(nil).Polish))
}

// RemoteStart mocks base method.
func (m *MockTriggerWorker) RemoteStart(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoteStart", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoteStart indicates an expected call of RemoteStart.
func (mr *MockTriggerWorkerMockRecorder) RemoteStart(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoteStart", reflect.TypeOf((*MockTriggerWorker)(nil).RemoteStart), ctx)
}

// RemoteStop mocks base method.
func (m *MockTriggerWorker) RemoteStop(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoteStop", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoteStop indicates an expected call of RemoteStop.
func (mr *MockTriggerWorkerMockRecorder) RemoteStop(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoteStop", reflect.TypeOf((*MockTriggerWorker)(nil).RemoteStop), ctx)
}

// Reset mocks base method.
func (m *MockTriggerWorker) Reset() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Reset")
}

// Reset indicates an expected call of Reset.
func (mr *MockTriggerWorkerMockRecorder) Reset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reset", reflect.TypeOf((*MockTriggerWorker)(nil).Reset))
}

// SetPhase mocks base method.
func (m *MockTriggerWorker) SetPhase(arg0 metadata.TriggerWorkerPhase) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetPhase", arg0)
}

// SetPhase indicates an expected call of SetPhase.
func (mr *MockTriggerWorkerMockRecorder) SetPhase(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetPhase", reflect.TypeOf((*MockTriggerWorker)(nil).SetPhase), arg0)
}

// Start mocks base method.
func (m *MockTriggerWorker) Start(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockTriggerWorkerMockRecorder) Start(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockTriggerWorker)(nil).Start), ctx)
}

// UnAssignSubscription mocks base method.
func (m *MockTriggerWorker) UnAssignSubscription(id vanus.ID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnAssignSubscription", id)
	ret0, _ := ret[0].(error)
	return ret0
}

// UnAssignSubscription indicates an expected call of UnAssignSubscription.
func (mr *MockTriggerWorkerMockRecorder) UnAssignSubscription(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnAssignSubscription", reflect.TypeOf((*MockTriggerWorker)(nil).UnAssignSubscription), id)
}
