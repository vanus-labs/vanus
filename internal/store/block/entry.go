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

//go:generate mockgen -source=entry.go  -destination=testing/mock_entry.go -package=testing
package block

import "time"

type Entry interface {
	Get(ordinal int) interface{}
	GetBytes(ordinal int) []byte
	GetString(ordinal int) string
	GetUint16(ordinal int) uint16
	GetUint64(ordinal int) uint64
	GetInt64(ordinal int) int64
	GetTime(ordinal int) time.Time

	GetExtensionAttribute([]byte) []byte
	RangeExtensionAttributes(f func(attr, val []byte))
}

type EntryExt interface {
	Entry

	OptionalAttributeCount() int
	ExtensionAttributeCount() int

	RangeOptionalAttributes(f func(ordinal int, val interface{}))
}

type EmptyEntry struct{}

// Mark sure EmptyEntry implements Entry.
var _ Entry = (*EmptyEntry)(nil)

func (e *EmptyEntry) Get(ordinal int) interface{} {
	return nil
}

func (e *EmptyEntry) GetBytes(ordinal int) []byte {
	return nil
}

func (e *EmptyEntry) GetString(ordinal int) string {
	return ""
}

func (e *EmptyEntry) GetUint16(ordinal int) uint16 {
	return 0
}

func (e *EmptyEntry) GetUint64(ordinal int) uint64 {
	return 0
}

func (e *EmptyEntry) GetInt64(ordinal int) int64 {
	return 0
}

func (e *EmptyEntry) GetTime(ordinal int) time.Time {
	return time.Time{}
}

func (e *EmptyEntry) GetExtensionAttribute([]byte) []byte {
	return nil
}

func (e *EmptyEntry) RangeExtensionAttributes(f func(attr, val []byte)) {
}

type EmptyEntryExt struct {
	EmptyEntry
}

// Mark sure EmptyEntryExt implements EntryExt.
var _ EntryExt = (*EmptyEntryExt)(nil)

func (e *EmptyEntryExt) OptionalAttributeCount() int {
	return 0
}

func (e *EmptyEntryExt) RangeOptionalAttributes(f func(ordinal int, val interface{})) {
}

func (e *EmptyEntryExt) ExtensionAttributeCount() int {
	return 0
}

type EntryExtWrapper struct {
	E EntryExt
}

// Make sure entryWrapper implements block.Entry.
var _ EntryExt = (*EntryExtWrapper)(nil)

func (w *EntryExtWrapper) Get(ordinal int) interface{} {
	return w.E.Get(ordinal)
}

func (w *EntryExtWrapper) GetBytes(ordinal int) []byte {
	return w.E.GetBytes(ordinal)
}

func (w *EntryExtWrapper) GetString(ordinal int) string {
	return w.E.GetString(ordinal)
}

func (w *EntryExtWrapper) GetUint16(ordinal int) uint16 {
	return w.E.GetUint16(ordinal)
}

func (w *EntryExtWrapper) GetUint64(ordinal int) uint64 {
	return w.E.GetUint64(ordinal)
}

func (w *EntryExtWrapper) GetInt64(ordinal int) int64 {
	return w.E.GetInt64(ordinal)
}

func (w *EntryExtWrapper) GetTime(ordinal int) time.Time {
	return w.E.GetTime(ordinal)
}

func (w *EntryExtWrapper) RangeOptionalAttributes(f func(ordinal int, val interface{})) {
	w.E.RangeOptionalAttributes(f)
}

func (w *EntryExtWrapper) OptionalAttributeCount() int {
	return w.E.OptionalAttributeCount()
}

func (w *EntryExtWrapper) GetExtensionAttribute(attr []byte) []byte {
	return w.E.GetExtensionAttribute(attr)
}

func (w *EntryExtWrapper) RangeExtensionAttributes(f func(attr, val []byte)) {
	w.E.RangeExtensionAttributes(f)
}

func (w *EntryExtWrapper) ExtensionAttributeCount() int {
	return w.E.ExtensionAttributeCount()
}
