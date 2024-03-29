// Code generated by MockGen. DO NOT EDIT.
// Source: manager.go
//
// Generated by this command:
//
//	mockgen -source=manager.go -destination=mock_manager.go -package=worker
//

// Package worker is a generated GoMock package.
package worker

import (
	context "context"
	reflect "reflect"

	metadata "github.com/vanus-labs/vanus/server/controller/trigger/metadata"
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

// AddTriggerWorker mocks base method.
func (m *MockManager) AddTriggerWorker(ctx context.Context, addr string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddTriggerWorker", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddTriggerWorker indicates an expected call of AddTriggerWorker.
func (mr *MockManagerMockRecorder) AddTriggerWorker(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddTriggerWorker", reflect.TypeOf((*MockManager)(nil).AddTriggerWorker), ctx, addr)
}

// GetActiveRunningTriggerWorker mocks base method.
func (m *MockManager) GetActiveRunningTriggerWorker() []metadata.TriggerWorkerInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetActiveRunningTriggerWorker")
	ret0, _ := ret[0].([]metadata.TriggerWorkerInfo)
	return ret0
}

// GetActiveRunningTriggerWorker indicates an expected call of GetActiveRunningTriggerWorker.
func (mr *MockManagerMockRecorder) GetActiveRunningTriggerWorker() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetActiveRunningTriggerWorker", reflect.TypeOf((*MockManager)(nil).GetActiveRunningTriggerWorker))
}

// GetTriggerWorker mocks base method.
func (m *MockManager) GetTriggerWorker(addr string) TriggerWorker {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTriggerWorker", addr)
	ret0, _ := ret[0].(TriggerWorker)
	return ret0
}

// GetTriggerWorker indicates an expected call of GetTriggerWorker.
func (mr *MockManagerMockRecorder) GetTriggerWorker(addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTriggerWorker", reflect.TypeOf((*MockManager)(nil).GetTriggerWorker), addr)
}

// Init mocks base method.
func (m *MockManager) Init(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockManagerMockRecorder) Init(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockManager)(nil).Init), ctx)
}

// RemoveTriggerWorker mocks base method.
func (m *MockManager) RemoveTriggerWorker(ctx context.Context, addr string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RemoveTriggerWorker", ctx, addr)
}

// RemoveTriggerWorker indicates an expected call of RemoveTriggerWorker.
func (mr *MockManagerMockRecorder) RemoveTriggerWorker(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveTriggerWorker", reflect.TypeOf((*MockManager)(nil).RemoveTriggerWorker), ctx, addr)
}

// Start mocks base method.
func (m *MockManager) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockManagerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockManager)(nil).Start))
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

// UpdateTriggerWorkerInfo mocks base method.
func (m *MockManager) UpdateTriggerWorkerInfo(ctx context.Context, addr string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateTriggerWorkerInfo", ctx, addr)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateTriggerWorkerInfo indicates an expected call of UpdateTriggerWorkerInfo.
func (mr *MockManagerMockRecorder) UpdateTriggerWorkerInfo(ctx, addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateTriggerWorkerInfo", reflect.TypeOf((*MockManager)(nil).UpdateTriggerWorkerInfo), ctx, addr)
}
