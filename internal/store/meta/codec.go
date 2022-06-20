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

package meta

import (
	// standard libraries.
	"errors"
	"math"
	"reflect"

	// third-party libraries.
	"google.golang.org/protobuf/encoding/protowire"
)

var (
	deletedMark  deletedMarkType
	defaultCodec codec
)

type Marshaler interface {
	Marshal(data Ranger) ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal(data []byte, cb RangeCallback) error
}

type deletedMarkType struct{}

type Kind uint8

const (
	Invalid Kind = iota
	Deleted
	True
	False
	Bytes
	String
	Int
	Int8
	Int16
	Int32
	Int64
	Uint
	Uint8
	Uint16
	Uint32
	Uint64
	Float32
	Float64
)

type codec struct{}

// Make sure codec implements Marshaler and Unmarshaler.
var _ Marshaler = (*codec)(nil)
var _ Unmarshaler = (*codec)(nil)

func (codec) Marshal(data Ranger) ([]byte, error) {
	buf := make([]byte, 0)

	var last []byte
	err := data.Range(func(key []byte, value interface{}) error {
		// Encode key.
		buf = appendKey(buf, key, last)

		// Encode value.
		var err error
		buf, err = appendValue(buf, value)
		if err != nil {
			return err
		}

		last = key

		return nil
	})
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func appendKey(buf []byte, key, last []byte) []byte {
	shared, private := encodeKey(key, last)
	buf = protowire.AppendVarint(buf, uint64(shared))
	buf = protowire.AppendBytes(buf, private)
	return buf
}

func encodeKey(key, last []byte) (int, []byte) {
	min := len(last)
	if min == 0 {
		return 0, key
	}
	if len(key) < min {
		min = len(key)
	}
	for i := 0; i < min; i++ {
		if key[i] != last[i] {
			return i, key[i:]
		}
	}
	return min, key[min:]
}

func appendValue(buf []byte, value interface{}) ([]byte, error) {
	if value == deletedMark {
		buf = protowire.AppendVarint(buf, uint64(Deleted))
		return buf, nil
	}
	switch v := value.(type) {
	case bool:
		if v {
			buf = protowire.AppendVarint(buf, uint64(True))
		} else {
			buf = protowire.AppendVarint(buf, uint64(False))
		}
		return buf, nil
	case []byte:
		buf = protowire.AppendVarint(buf, uint64(Bytes))
		buf = protowire.AppendBytes(buf, v)
		return buf, nil
	case string:
		buf = protowire.AppendVarint(buf, uint64(String))
		buf = protowire.AppendString(buf, v)
		return buf, nil
	case int, int8, int16, int32, int64:
		rv := reflect.ValueOf(value)
		buf = protowire.AppendVarint(buf, uint64(toKind(rv.Kind())))
		buf = protowire.AppendVarint(buf, protowire.EncodeZigZag(rv.Int()))
		return buf, nil
	case uint, uint8, uint16, uint32, uint64:
		rv := reflect.ValueOf(value)
		buf = protowire.AppendVarint(buf, uint64(toKind(rv.Kind())))
		buf = protowire.AppendVarint(buf, rv.Uint())
		return buf, nil
	case float32:
		buf = protowire.AppendVarint(buf, uint64(Float32))
		buf = protowire.AppendFixed32(buf, math.Float32bits(v))
		return buf, nil
	case float64:
		buf = protowire.AppendVarint(buf, uint64(Float64))
		buf = protowire.AppendFixed64(buf, math.Float64bits(v))
		return buf, nil
	}
	// TODO(james.yin): validate type
	return nil, errors.New("unsupported type")
}

func toKind(k reflect.Kind) Kind {
	if k >= reflect.Int && k <= reflect.Uint64 {
		return Kind(uint8(Int) + uint8(k-reflect.Int))
	}
	switch k {
	case reflect.Float32:
		return Float32
	case reflect.Float64:
		return Float64
	case reflect.String:
		return String
	}
	return Invalid
}

func (codec) Unmarshal(data []byte, cb RangeCallback) error {
	var last []byte
	for so := 0; so < len(data); {
		// Decode key.
		key, n := consumeKey(data[so:], last)
		if n < 0 {
			return protowire.ParseError(n)
		}
		so += n

		// Decode value.
		value, n := consumeValue(data[so:])
		if n < 0 {
			return protowire.ParseError(n)
		}
		so += n

		if err := cb(key, value); err != nil {
			return err
		}

		last = key
	}
	return nil
}

func consumeKey(buf []byte, last []byte) ([]byte, int) {
	shared, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return nil, n
	}
	private, n2 := protowire.ConsumeBytes(buf[n:])
	if n2 < 0 {
		return nil, n2
	}
	key := decodeKey(private, last, int(shared))
	return key, n + n2
}

func decodeKey(private, last []byte, shared int) []byte {
	key := make([]byte, shared+len(private))
	if shared > 0 {
		copy(key, last[:shared])
	}
	copy(key[shared:], private)
	return key
}

func consumeValue(buf []byte) (interface{}, int) {
	k, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return nil, n
	}
	kind := Kind(k)
	switch kind {
	case Deleted:
		return deletedMark, n
	case True:
		return true, n
	case False:
		return False, n
	case Bytes:
		b, n2 := protowire.ConsumeBytes(buf[n:])
		if n2 < 0 {
			return nil, n2
		}
		return append([]byte(nil), b...), n + n2
	case String:
		s, n2 := protowire.ConsumeString(buf[n:])
		if n2 < 0 {
			return nil, n2
		}
		return s, n + n2
	case Int, Int8, Int16, Int32, Int64:
		t, n2 := protowire.ConsumeVarint(buf[n:])
		if n2 < 0 {
			return nil, n2
		}
		v := protowire.DecodeZigZag(t)
		switch kind {
		case Int:
			return int(v), n + n2
		case Int8:
			return int8(v), n + n2
		case Int16:
			return int16(v), n + n2
		case Int32:
			return int32(v), n + n2
		case Int64:
			return v, n + n2
		}
	case Uint, Uint8, Uint16, Uint32, Uint64:
		v, n2 := protowire.ConsumeVarint(buf[n:])
		if n2 < 0 {
			return nil, n2
		}
		switch kind {
		case Uint:
			return uint(v), n + n2
		case Uint8:
			return uint8(v), n + n2
		case Uint16:
			return uint16(v), n + n2
		case Uint32:
			return uint32(v), n + n2
		case Uint64:
			return v, n + n2
		}
	case Float32:
		f, n2 := protowire.ConsumeFixed32(buf[n:])
		if n2 < 0 {
			return nil, n2
		}
		return math.Float32frombits(f), n + n2
	case Float64:
		f, n2 := protowire.ConsumeFixed64(buf[n:])
		if n2 < 0 {
			return nil, n2
		}
		return math.Float64frombits(f), n + n2
	}
	return nil, -6
}
