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

package vjson

import (
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/internal/util"
)

// Type is json raw type.
type Type int

const (
	Null Type = iota
	String
	Bool
	Number
	Array
	Object
)

type Result struct {
	Key string
	// Type is the json type.
	Type Type
	// Raw is the raw json.
	Raw []byte

	Result map[string]Result
}

func (r *Result) Bytes() []byte {
	return r.Raw
}

func (r *Result) String() string {
	return string(r.Raw)
}

func Decode(json []byte) (map[string]Result, error) {
	sList := &scannerList{json: json, result: map[string]Result{}}
	err := sList.parseObject()
	if err != nil {
		return nil, err
	}
	return sList.result, nil
}

type scanner struct {
	json   []byte
	pos    int
	result Result
}

type scannerList struct {
	json   []byte
	pos    int
	result map[string]Result
}

func (sList *scannerList) parseObject() error {
	for ; sList.pos < len(sList.json); sList.pos++ {
		c := sList.json[sList.pos]
		if util.IsSpace(c) {
			continue
		}
		switch c {
		case '"':
			// key begin.
			s := &scanner{json: sList.json, pos: sList.pos + 1}
			err := s.parseBeginKey()
			if err != nil {
				return err
			}
			sList.result[s.result.Key] = s.result
			sList.pos = s.pos
		case '}':
			return nil
		}
	}
	return errors.ErrVanusJSONParse.WithMessage("object not end")
}

func (s *scanner) parseBeginKey() error {
	start := s.pos
	err := s.parseJSONString()
	if err != nil {
		return err
	}
	s.result.Key = string(s.json[start:s.pos])
	s.pos++
	for ; s.pos < len(s.json); s.pos++ {
		c := s.json[s.pos]
		if util.IsSpace(c) {
			continue
		}
		switch c {
		case ':':
			continue
		case '"':
			s.pos++
			start = s.pos
			err = s.parseJSONString()
			if err != nil {
				return err
			}
			// raw abc.
			s.result.Raw = s.json[start:s.pos]
			s.result.Type = String
			return nil
		case '[':
			start = s.pos
			s.pos++
			err = s.parseJSONArr()
			if err != nil {
				return err
			}
			// raw [1,2,3],contains [].
			s.result.Raw = s.json[start : s.pos+1]
			s.result.Type = Array
			return nil
		case '{':
			start = s.pos
			s.pos++
			sList := &scannerList{json: s.json, pos: s.pos, result: map[string]Result{}}
			err = sList.parseObject()
			if err != nil {
				return err
			}
			// raw {"key":"value"}，contains {} .
			s.result.Raw = s.json[start : sList.pos+1]
			s.result.Type = Object
			s.result.Result = sList.result
			s.pos = sList.pos
			return nil
		case 't':
			start = s.pos
			err = s.parseJSONTrue()
			if err != nil {
				return err
			}
			// raw ture.
			s.result.Raw = s.json[start : s.pos+1]
			s.result.Type = Bool
			return nil
		case 'f':
			start = s.pos
			err = s.parseJSONFalse()
			if err != nil {
				return err
			}
			// raw false.
			s.result.Raw = s.json[start : s.pos+1]
			s.result.Type = Bool
			return nil
		case 'n':
			start = s.pos
			err = s.parseJSONNil()
			if err != nil {
				return err
			}
			//raw null
			s.result.Raw = s.json[start : s.pos+1]
			s.result.Type = Null
			return nil
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
			start = s.pos
			s.pos++
			err = s.parseNum()
			if err != nil {
				return err
			}
			s.result.Raw = s.json[start:s.pos]
			s.result.Type = Number
			s.pos--
			return nil
		}
		return errors.ErrVanusJSONParse.WithMessage("unknown value begin")
	}
	return nil
}

func (s *scanner) parseNum() error {
	for ; s.pos < len(s.json); s.pos++ {
		c := s.json[s.pos]
		switch c {
		case ',', '}':
			return nil
		}
	}
	return errors.ErrVanusJSONParse.WithMessage("number invalid end")
}

func (s *scanner) parseJSONNil() error {
	s.pos += 3
	if s.pos < len(s.json) &&
		s.json[s.pos-2] == 'u' &&
		s.json[s.pos-1] == 'l' &&
		s.json[s.pos] == 'l' {
		return nil
	}
	return errors.ErrVanusJSONParse.WithMessage("null invalid")
}

func (s *scanner) parseJSONTrue() error {
	s.pos += 3
	if s.pos < len(s.json) &&
		s.json[s.pos-2] == 'r' &&
		s.json[s.pos-1] == 'u' &&
		s.json[s.pos] == 'e' {
		return nil
	}
	return errors.ErrVanusJSONParse.WithMessage("bool true invalid")
}

func (s *scanner) parseJSONFalse() error {
	s.pos += 4
	if s.pos < len(s.json) &&
		s.json[s.pos-3] == 'a' &&
		s.json[s.pos-2] == 'l' &&
		s.json[s.pos-1] == 's' &&
		s.json[s.pos] == 'e' {
		return nil
	}
	return errors.ErrVanusJSONParse.WithMessage("bool false invalid")
}

func (s *scanner) parseJSONArr() error {
	startCount := 1
	for ; s.pos < len(s.json); s.pos++ {
		c := s.json[s.pos]
		switch c {
		case '[':
			startCount++
		case ']':
			startCount--
			if startCount == 0 {
				return nil
			}
		}
	}
	return errors.ErrVanusJSONParse.WithMessage("string not end")
}

func (s *scanner) parseJSONString() error {
	for ; s.pos < len(s.json); s.pos++ {
		c := s.json[s.pos]
		switch c {
		case '"':
			if s.json[s.pos-1] != '\\' {
				return nil
			}
		}
	}
	return errors.ErrVanusJSONParse.WithMessage("string not end")
}
