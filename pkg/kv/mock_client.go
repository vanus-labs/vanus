// Code generated by MockGen. DO NOT EDIT.
// Source: client.go
//
// Generated by this command:
//
//	mockgen -source=client.go -destination=mock_client.go -package=kv
//

// Package kv is a generated GoMock package.
package kv

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "go.uber.org/mock/gomock"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockClient) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockClient)(nil).Close))
}

// CompareAndDelete mocks base method.
func (m *MockClient) CompareAndDelete(ctx context.Context, key string, preValue []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompareAndDelete", ctx, key, preValue)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompareAndDelete indicates an expected call of CompareAndDelete.
func (mr *MockClientMockRecorder) CompareAndDelete(ctx, key, preValue any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompareAndDelete", reflect.TypeOf((*MockClient)(nil).CompareAndDelete), ctx, key, preValue)
}

// CompareAndSwap mocks base method.
func (m *MockClient) CompareAndSwap(ctx context.Context, key string, preValue, value []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompareAndSwap", ctx, key, preValue, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompareAndSwap indicates an expected call of CompareAndSwap.
func (mr *MockClientMockRecorder) CompareAndSwap(ctx, key, preValue, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompareAndSwap", reflect.TypeOf((*MockClient)(nil).CompareAndSwap), ctx, key, preValue, value)
}

// Create mocks base method.
func (m *MockClient) Create(ctx context.Context, key string, value []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", ctx, key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *MockClientMockRecorder) Create(ctx, key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockClient)(nil).Create), ctx, key, value)
}

// Delete mocks base method.
func (m *MockClient) Delete(ctx context.Context, key string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, key)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockClientMockRecorder) Delete(ctx, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockClient)(nil).Delete), ctx, key)
}

// DeleteDir mocks base method.
func (m *MockClient) DeleteDir(ctx context.Context, path string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteDir", ctx, path)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteDir indicates an expected call of DeleteDir.
func (mr *MockClientMockRecorder) DeleteDir(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteDir", reflect.TypeOf((*MockClient)(nil).DeleteDir), ctx, path)
}

// Exists mocks base method.
func (m *MockClient) Exists(ctx context.Context, key string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exists", ctx, key)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exists indicates an expected call of Exists.
func (mr *MockClientMockRecorder) Exists(ctx, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exists", reflect.TypeOf((*MockClient)(nil).Exists), ctx, key)
}

// Get mocks base method.
func (m *MockClient) Get(ctx context.Context, key string) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", ctx, key)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockClientMockRecorder) Get(ctx, key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockClient)(nil).Get), ctx, key)
}

// List mocks base method.
func (m *MockClient) List(ctx context.Context, path string) ([]Pair, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "List", ctx, path)
	ret0, _ := ret[0].([]Pair)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// List indicates an expected call of List.
func (mr *MockClientMockRecorder) List(ctx, path any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "List", reflect.TypeOf((*MockClient)(nil).List), ctx, path)
}

// Set mocks base method.
func (m *MockClient) Set(ctx context.Context, key string, value []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Set", ctx, key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// Set indicates an expected call of Set.
func (mr *MockClientMockRecorder) Set(ctx, key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Set", reflect.TypeOf((*MockClient)(nil).Set), ctx, key, value)
}

// SetWithTTL mocks base method.
func (m *MockClient) SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetWithTTL", ctx, key, value, ttl)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetWithTTL indicates an expected call of SetWithTTL.
func (mr *MockClientMockRecorder) SetWithTTL(ctx, key, value, ttl any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetWithTTL", reflect.TypeOf((*MockClient)(nil).SetWithTTL), ctx, key, value, ttl)
}

// Update mocks base method.
func (m *MockClient) Update(ctx context.Context, key string, value []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Update", ctx, key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// Update indicates an expected call of Update.
func (mr *MockClientMockRecorder) Update(ctx, key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*MockClient)(nil).Update), ctx, key, value)
}

// Watch mocks base method.
func (m *MockClient) Watch(ctx context.Context, key string, stopCh <-chan struct{}) (chan Pair, chan error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Watch", ctx, key, stopCh)
	ret0, _ := ret[0].(chan Pair)
	ret1, _ := ret[1].(chan error)
	return ret0, ret1
}

// Watch indicates an expected call of Watch.
func (mr *MockClientMockRecorder) Watch(ctx, key, stopCh any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Watch", reflect.TypeOf((*MockClient)(nil).Watch), ctx, key, stopCh)
}

// WatchTree mocks base method.
func (m *MockClient) WatchTree(ctx context.Context, path string, stopCh <-chan struct{}) (chan Pair, chan error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WatchTree", ctx, path, stopCh)
	ret0, _ := ret[0].(chan Pair)
	ret1, _ := ret[1].(chan error)
	return ret0, ret1
}

// WatchTree indicates an expected call of WatchTree.
func (mr *MockClientMockRecorder) WatchTree(ctx, path, stopCh any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WatchTree", reflect.TypeOf((*MockClient)(nil).WatchTree), ctx, path, stopCh)
}
