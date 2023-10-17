// Code generated by MockGen. DO NOT EDIT.
// Source: trigger.go

// Package trigger is a generated GoMock package.
package trigger

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	primitive "github.com/vanus-labs/vanus/pkg"
	info "github.com/vanus-labs/vanus/pkg/info"
)

// MockTrigger is a mock of Trigger interface.
type MockTrigger struct {
	ctrl     *gomock.Controller
	recorder *MockTriggerMockRecorder
}

// MockTriggerMockRecorder is the mock recorder for MockTrigger.
type MockTriggerMockRecorder struct {
	mock *MockTrigger
}

// NewMockTrigger creates a new mock instance.
func NewMockTrigger(ctrl *gomock.Controller) *MockTrigger {
	mock := &MockTrigger{ctrl: ctrl}
	mock.recorder = &MockTriggerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTrigger) EXPECT() *MockTriggerMockRecorder {
	return m.recorder
}

// Change mocks base method.
func (m *MockTrigger) Change(ctx context.Context, subscription *primitive.Subscription) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Change", ctx, subscription)
	ret0, _ := ret[0].(error)
	return ret0
}

// Change indicates an expected call of Change.
func (mr *MockTriggerMockRecorder) Change(ctx, subscription interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Change", reflect.TypeOf((*MockTrigger)(nil).Change), ctx, subscription)
}

// GetOffsets mocks base method.
func (m *MockTrigger) GetOffsets(ctx context.Context) info.ListOffsetInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOffsets", ctx)
	ret0, _ := ret[0].(info.ListOffsetInfo)
	return ret0
}

// GetOffsets indicates an expected call of GetOffsets.
func (mr *MockTriggerMockRecorder) GetOffsets(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOffsets", reflect.TypeOf((*MockTrigger)(nil).GetOffsets), ctx)
}

// Init mocks base method.
func (m *MockTrigger) Init(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockTriggerMockRecorder) Init(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockTrigger)(nil).Init), ctx)
}

// Start mocks base method.
func (m *MockTrigger) Start(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockTriggerMockRecorder) Start(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockTrigger)(nil).Start), ctx)
}

// Stop mocks base method.
func (m *MockTrigger) Stop(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockTriggerMockRecorder) Stop(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockTrigger)(nil).Stop), ctx)
}