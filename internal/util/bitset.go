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

package util

import (
	"math/bits"
	"strconv"
	"strings"
)

const (
	AddressBitsPerWord = 6
	bitsPerWord        = 1 << AddressBitsPerWord
	WordMask           = uint64(1)
)

type BitSet struct {
	words      []uint64
	wordsInUse int
}

func NewBitSet() *BitSet {
	s := &BitSet{}
	s.words = make([]uint64, 1)
	return s
}

func (s *BitSet) Clear(begin int) *BitSet {
	if begin > len(s.words) {
		return s
	}
	tmpWords := s.words[begin:]
	n := len(tmpWords)
	for n > 0 && tmpWords[n-1] == 0 {
		n--
	}
	tmpWords = tmpWords[:n]
	newWords := make([]uint64, len(tmpWords))
	copy(newWords, tmpWords)
	return &BitSet{
		words:      newWords,
		wordsInUse: len(newWords),
	}
}

func (s *BitSet) wordIndex(bitIndex int) int {
	return bitIndex >> AddressBitsPerWord
}

func (s *BitSet) expandTo(wordIndex int) {
	wordRequired := wordIndex + 1
	if s.wordsInUse < wordRequired {
		s.ensureCapacity(wordRequired)
		s.wordsInUse = wordRequired
	}
}
func (s *BitSet) ensureCapacity(wordRequired int) {
	if len(s.words) >= wordRequired {
		return
	}
	request := 2 * len(s.words)
	if request < wordRequired {
		request = wordRequired
	}
	newWords := make([]uint64, request)
	copy(newWords, s.words)
	s.words = newWords
}

func (s *BitSet) maskForBit(bit int) uint64 {
	return WordMask << (bit % bitsPerWord)
}

func (s *BitSet) Set(bitIndex int) {
	wordIndex := s.wordIndex(bitIndex)
	s.expandTo(wordIndex)
	s.words[wordIndex] |= s.maskForBit(bitIndex)
}

func (s *BitSet) NextClearBit(fromIndex int) int {
	wordIndex := s.wordIndex(fromIndex)
	if wordIndex >= s.wordsInUse {
		return fromIndex
	}
	word := ^s.words[wordIndex]
	for {
		if word != 0 {
			return wordIndex*bitsPerWord + bits.TrailingZeros64(word)
		}
		wordIndex++
		if wordIndex == s.wordsInUse {
			return s.wordsInUse * bitsPerWord
		}
		word = ^s.words[wordIndex]
	}
}

func (s *BitSet) length() int {
	cnt := 0
	for _, val := range s.words {
		cnt += bits.OnesCount64(val)
	}
	return cnt
}

func (s *BitSet) String() string {
	vals := make([]string, 0, s.length())

	for i, v := range s.words {
		for v != 0 {
			n := bits.TrailingZeros64(v)
			vals = append(vals, strconv.Itoa(i*bitsPerWord+n))
			v &= ^(uint64(1) << n)
		}
	}
	return "{" + strings.Join(vals, ", ") + "}"
}
