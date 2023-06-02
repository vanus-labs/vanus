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

package generate

import (
	// standard libraries.
	"strconv"

	// third-party libraries.
	"golang.org/x/exp/constraints"
)

func AppendInt[T constraints.Signed](dst []byte, i T) []byte {
	return strconv.AppendInt(dst, int64(i), 10)
}

func AppendUint[T constraints.Unsigned](dst []byte, i T) []byte {
	return strconv.AppendUint(dst, uint64(i), 10)
}

func AppendFloat32(dst []byte, f float32) []byte {
	return strconv.AppendFloat(dst, float64(f), 'g', -1, 32)
}

func AppendFloat64(dst []byte, f float64) []byte {
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}
