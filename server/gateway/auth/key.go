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

package auth

import "context"

type userKeyType struct{}

var userKey userKeyType

func GetUser(ctx context.Context) string {
	user, _ := ctx.Value(userKey).(string)
	return user
}

func SetUser(ctx context.Context, user string) context.Context {
	return context.WithValue(ctx, userKey, user)
}
