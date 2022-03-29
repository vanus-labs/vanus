// Copyright 2022 Linkall Inc.
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

package ut

import "fmt"

//go:generate mockgen --source=test/ut/ut.go --destination=test/ut/mock_ut.go --package=ut
type Foo interface {
	Bar(x int) int
}

func SUT(f Foo) {
	fmt.Println(f.Bar(99))
}

var doubleInt = func(int int) int {
	return int * 2
}
