// Copyright 2015 The etcd Authors
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

package wal

import (
	// standard libraries.
	"context"
	"fmt"
	"log"
	"sync"
	"testing"

	// this project.
	"github.com/vanus-labs/vanus/server/store/wal/record"
)

func BenchmarkWAL_AppendOne(b *testing.B) {
	walDir := b.TempDir()

	wal, err := Open(context.Background(), walDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		wal.Close()
		wal.Wait()
	}()

	b.ResetTimer()

	// b.Run("WAL: batching append", func(b *testing.B) {
	// 	for i := 0; i < b.N; i++ {
	// 		AppendOne(context.Background(), wal, []byte("foo"))
	// 	}
	// })

	b.Run("WAL: concurrent batching append", func(b *testing.B) {
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				AppendOne(context.Background(), wal, []byte("foo"))
			}
		})
	})
}

func BenchmarkWAL_DirectAppendOne(b *testing.B) {
	walDir := b.TempDir()

	wal, err := Open(context.Background(), walDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		wal.Close()
		wal.Wait()
	}()

	b.ResetTimer()

	b.Run("WAL: direct append", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			DirectAppendOne(context.Background(), wal, []byte("foo"))
		}
	})

	// b.Run("WAL: concurrent direct append", func(b *testing.B) {
	// 	b.RunParallel(func(p *testing.PB) {
	// 		for p.Next() {
	// 			DirectAppendOne(context.Background(), wal, []byte("foo"))
	// 		}
	// 	})
	// })
}

func BenchmarkWAL_AppendOneWithCallback(b *testing.B) {
	// walDir := filepath.Join(walTempDir, "wal-test")
	walDir := b.TempDir()

	wal, err := Open(context.Background(), walDir)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		wal.Close()
		wal.Wait()
	}()

	testCases := []struct {
		size    int
		payload []byte
	}{
		// {size: 1},
		// {size: 2},
		// {size: 4},
		// {size: 8},
		// {size: 16},
		// {size: 32},
		// {size: 64},
		// {size: 128},
		// {size: 256},
		// {size: 512},
		{size: 1024},
		// {size: 2048},
		// {size: 4096},
		// {size: 8192},
	}
	for i := range testCases {
		tc := &testCases[i]
		if tc.size > record.HeaderSize {
			tc.size -= record.HeaderSize
		}
		tc.payload = generatePayload(tc.size)
	}

	b.ResetTimer()

	for _, tc := range testCases {
		b.Run(fmt.Sprintf("WAL: async append %d bytes payload", tc.size), func(b *testing.B) {
			wg := sync.WaitGroup{}
			wg.Add(b.N)
			for i := 0; i < b.N; i++ {
				wal.AppendOne(context.Background(), tc.payload, func(_ Range, err error) {
					if err != nil {
						log.Printf("err: %v", err)
					}
					wg.Done()
				})
			}
			wg.Wait()
		})
	}
}

func generatePayload(size int) []byte {
	data := func() string {
		str := ""
		for idx := 0; idx < size-1; idx++ {
			str += "a"
		}
		str += "\n"
		return str
	}()
	return []byte(data)
}
