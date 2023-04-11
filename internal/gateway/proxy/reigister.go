// Copyright 2023 Linkall Inc.
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

package proxy

import proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"

func (cp *ControllerProxy) registerAuthentication() {
	//cp.authService.RegisterAuthorizeFunc(proxypb.StoreProxy_Publish_FullMethodName, authPublish)

	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_CreateUser_FullMethodName, authCreateUser)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_DeleteUser_FullMethodName, authDeleteUser)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_ListToken_FullMethodName, authListToken)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_GrantRole_FullMethodName, authGrantRole)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_RevokeRole_FullMethodName, authRevokeRole)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_GetResourceRole_FullMethodName, authGetResourceRole)

	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_CreateNamespace_FullMethodName, authCreateNamespace)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_GetNamespace_FullMethodName, authGetNamespace)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_DeleteNamespace_FullMethodName, authDeleteNamespace)

	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_CreateEventbus_FullMethodName, authCreateEventbus)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_DeleteEventbus_FullMethodName, authDeleteEventbus)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_GetEventbus_FullMethodName, authGetEventbus)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_ListSegment_FullMethodName, authListSegment)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_LookupOffset_FullMethodName, authLookupOffset)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_GetEvent_FullMethodName, authGetEvent)

	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_CreateSubscription_FullMethodName, authCreateSubscription) //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_DeleteSubscription_FullMethodName, authDeleteSubscription) //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_UpdateSubscription_FullMethodName, authUpdateSubscription) //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_GetSubscription_FullMethodName, authGetSubscription)
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_DisableSubscription_FullMethodName, authDisableSubscription)           //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_ResumeSubscription_FullMethodName, authResumeSubscription)             //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_ResetOffsetToTimestamp_FullMethodName, authResetOffsetSubscription)    //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_GetDeadLetterEvent_FullMethodName, authGetDeadLetterEvent)             //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_SetDeadLetterEventOffset_FullMethodName, authSetDeadLetterEventOffset) //nolint:lll // ok
	cp.authService.RegisterAuthorizeFunc(proxypb.ControllerProxy_ResendDeadLetterEvent_FullMethodName, authResendDeadLetterEvent)       //nolint:lll // ok
}
