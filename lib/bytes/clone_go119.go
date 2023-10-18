// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

//go:build !go1.20

package bytes

// Clone returns a copy of b[:len(b)].
// The result may have additional unused capacity.
// Clone(nil) returns nil.
func Clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}
