// Code generated by MockGen. DO NOT EDIT.
// Source: token.go

// Package manager is a generated GoMock package.
package manager

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	metadata "github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	vanus "github.com/vanus-labs/vanus/internal/primitive/vanus"
)

// MockTokenManager is a mock of TokenManager interface.
type MockTokenManager struct {
	ctrl     *gomock.Controller
	recorder *MockTokenManagerMockRecorder
}

// MockTokenManagerMockRecorder is the mock recorder for MockTokenManager.
type MockTokenManagerMockRecorder struct {
	mock *MockTokenManager
}

// NewMockTokenManager creates a new mock instance.
func NewMockTokenManager(ctrl *gomock.Controller) *MockTokenManager {
	mock := &MockTokenManager{ctrl: ctrl}
	mock.recorder = &MockTokenManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTokenManager) EXPECT() *MockTokenManagerMockRecorder {
	return m.recorder
}

// AddToken mocks base method.
func (m *MockTokenManager) AddToken(ctx context.Context, user *metadata.Token) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddToken", ctx, user)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddToken indicates an expected call of AddToken.
func (mr *MockTokenManagerMockRecorder) AddToken(ctx, user interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddToken", reflect.TypeOf((*MockTokenManager)(nil).AddToken), ctx, user)
}

// DeleteToken mocks base method.
func (m *MockTokenManager) DeleteToken(ctx context.Context, id vanus.ID) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteToken", ctx, id)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteToken indicates an expected call of DeleteToken.
func (mr *MockTokenManagerMockRecorder) DeleteToken(ctx, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteToken", reflect.TypeOf((*MockTokenManager)(nil).DeleteToken), ctx, id)
}

// GetToken mocks base method.
func (m *MockTokenManager) GetToken(ctx context.Context, id vanus.ID) (*metadata.Token, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetToken", ctx, id)
	ret0, _ := ret[0].(*metadata.Token)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetToken indicates an expected call of GetToken.
func (mr *MockTokenManagerMockRecorder) GetToken(ctx, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetToken", reflect.TypeOf((*MockTokenManager)(nil).GetToken), ctx, id)
}

// GetUser mocks base method.
func (m *MockTokenManager) GetUser(ctx context.Context, token string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUser", ctx, token)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetUser indicates an expected call of GetUser.
func (mr *MockTokenManagerMockRecorder) GetUser(ctx, token interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUser", reflect.TypeOf((*MockTokenManager)(nil).GetUser), ctx, token)
}

// GetUserToken mocks base method.
func (m *MockTokenManager) GetUserToken(ctx context.Context, identifier string) []*metadata.Token {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUserToken", ctx, identifier)
	ret0, _ := ret[0].([]*metadata.Token)
	return ret0
}

// GetUserToken indicates an expected call of GetUserToken.
func (mr *MockTokenManagerMockRecorder) GetUserToken(ctx, identifier interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUserToken", reflect.TypeOf((*MockTokenManager)(nil).GetUserToken), ctx, identifier)
}

// Init mocks base method.
func (m *MockTokenManager) Init(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockTokenManagerMockRecorder) Init(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockTokenManager)(nil).Init), ctx)
}

// ListToken mocks base method.
func (m *MockTokenManager) ListToken(ctx context.Context) []*metadata.Token {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListToken", ctx)
	ret0, _ := ret[0].([]*metadata.Token)
	return ret0
}

// ListToken indicates an expected call of ListToken.
func (mr *MockTokenManagerMockRecorder) ListToken(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListToken", reflect.TypeOf((*MockTokenManager)(nil).ListToken), ctx)
}