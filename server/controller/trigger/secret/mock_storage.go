// Code generated by MockGen. DO NOT EDIT.
// Source: storage.go
//
// Generated by this command:
//
//	mockgen -source=storage.go -destination=mock_storage.go -package=secret
//

// Package secret is a generated GoMock package.
package secret

import (
	context "context"
	reflect "reflect"

	vsr "github.com/vanus-labs/vanus/api/vsr"
	pkg "github.com/vanus-labs/vanus/pkg"
	gomock "go.uber.org/mock/gomock"
)

// MockStorage is a mock of Storage interface.
type MockStorage struct {
	ctrl     *gomock.Controller
	recorder *MockStorageMockRecorder
}

// MockStorageMockRecorder is the mock recorder for MockStorage.
type MockStorageMockRecorder struct {
	mock *MockStorage
}

// NewMockStorage creates a new mock instance.
func NewMockStorage(ctrl *gomock.Controller) *MockStorage {
	mock := &MockStorage{ctrl: ctrl}
	mock.recorder = &MockStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStorage) EXPECT() *MockStorageMockRecorder {
	return m.recorder
}

// Delete mocks base method.
func (m *MockStorage) Delete(ctx context.Context, subID vsr.ID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, subID)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockStorageMockRecorder) Delete(ctx, subID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockStorage)(nil).Delete), ctx, subID)
}

// Read mocks base method.
func (m *MockStorage) Read(ctx context.Context, subID vsr.ID, credentialType pkg.CredentialType) (pkg.SinkCredential, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", ctx, subID, credentialType)
	ret0, _ := ret[0].(pkg.SinkCredential)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockStorageMockRecorder) Read(ctx, subID, credentialType any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockStorage)(nil).Read), ctx, subID, credentialType)
}

// Write mocks base method.
func (m *MockStorage) Write(ctx context.Context, subID vsr.ID, credential pkg.SinkCredential) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", ctx, subID, credential)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockStorageMockRecorder) Write(ctx, subID, credential any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockStorage)(nil).Write), ctx, subID, credential)
}
