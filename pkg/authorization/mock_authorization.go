// Code generated by MockGen. DO NOT EDIT.
// Source: authorization.go
//
// Generated by this command:
//
//	mockgen -source=authorization.go -destination=mock_authorization.go -package=authorization
//

// Package authorization is a generated GoMock package.
package authorization

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockAuthorization is a mock of Authorization interface.
type MockAuthorization struct {
	ctrl     *gomock.Controller
	recorder *MockAuthorizationMockRecorder
}

// MockAuthorizationMockRecorder is the mock recorder for MockAuthorization.
type MockAuthorizationMockRecorder struct {
	mock *MockAuthorization
}

// NewMockAuthorization creates a new mock instance.
func NewMockAuthorization(ctrl *gomock.Controller) *MockAuthorization {
	mock := &MockAuthorization{ctrl: ctrl}
	mock.recorder = &MockAuthorizationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAuthorization) EXPECT() *MockAuthorizationMockRecorder {
	return m.recorder
}

// Authorize mocks base method.
func (m *MockAuthorization) Authorize(ctx context.Context, user string, attributes Attributes) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Authorize", ctx, user, attributes)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Authorize indicates an expected call of Authorize.
func (mr *MockAuthorizationMockRecorder) Authorize(ctx, user, attributes any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Authorize", reflect.TypeOf((*MockAuthorization)(nil).Authorize), ctx, user, attributes)
}
