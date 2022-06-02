// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/linkall-labs/vanus/client/pkg/errors"
)

const (
	multipleControllersKey = "controllers"
)

// VRN represent a name of Vanus' resource.
// TODO optimize this with option-style, option-style is the most popular
// style for creating configuration in golang world
// see https://pkg.go.dev/google.golang.org/grpc#CallOption for more examples
type VRN struct {
	Scheme    string
	Endpoints []string
	Kind      string
	ID        uint64
	Attrs     url.Values
	Name      string

	str atomic.Value
}

func (v *VRN) String() string {
	str := v.str.Load()
	if str == nil {
		s := v.doString()
		v.str.Store(&s)
		return s
	}
	return *str.(*string)
}

func (v *VRN) doString() string {
	str := "invalid vrn"
	if v.Scheme != "vanus" && !strings.HasPrefix(v.Scheme, "vanus+") {
		return str
	}
	if len(v.Endpoints) == 0 {
		return str
	}

	if v.Kind == "eventbus" {
		str = fmt.Sprintf("%s://%s/%s/%s", v.Scheme, v.Endpoints[0], v.Kind, v.Name)
	} else {
		str = fmt.Sprintf("%s://%s/%s/%d", v.Scheme, v.Endpoints[0], v.Kind, v.ID)
	}

	if len(v.Endpoints) > 1 {
		str = fmt.Sprintf("%s?%s=%s", str, multipleControllersKey, strings.Join(v.Endpoints, ","))
	}

	return str
}

func ParseVRN(vrn string) (*VRN, error) {
	url, err := url.Parse(vrn)
	if err != nil {
		return nil, err
	}

	if url.Scheme == "vanus" || strings.HasPrefix(url.Scheme, "vanus+") {
		return doParse(url)
	}

	return nil, errors.ErrInvalidArgument
}

func doParse(url *url.URL) (*VRN, error) {
	if len(url.Opaque) > 0 {
		return doParseOpaque(url)
	} else {
		return doParseURL(url)
	}
}

func doParseOpaque(url *url.URL) (*VRN, error) {
	s := strings.Split(url.Opaque, ":")
	if len(s) != 2 {
		return nil, errors.ErrInvalidArgument
	}
	id, err := strconv.ParseUint(s[1], 10, 64)
	if err != nil {
		return nil, errors.ErrInvalidArgument
	}

	// TODO: endpoint
	return &VRN{
		Scheme:    url.Scheme,
		Endpoints: []string{"vanus"},
		Kind:      s[0],
		ID:        id,
		Attrs:     url.Query(),
	}, nil
}

func doParseURL(url *url.URL) (*VRN, error) {
	path := url.Path
	if strings.HasPrefix(path, "/") {
		path = url.Path[1:]
	}

	s := strings.Split(path, "/")
	if len(s) != 2 {
		return nil, errors.ErrInvalidArgument
	}

	vrn := &VRN{
		Scheme:    url.Scheme,
		Endpoints: []string{url.Host},
		Kind:      s[0],
		Name:      s[1],
		Attrs:     url.Query(),
	}

	if id, err := strconv.ParseUint(s[1], 10, 64); err == nil {
		vrn.ID = id
	}

	return parseControllers(vrn)
}

func parseControllers(vrn *VRN) (*VRN, error) {
	if vrn == nil {
		return nil, nil
	}
	str := vrn.Attrs.Get(multipleControllersKey)
	if str == "" {
		return vrn, nil
	}
	endpoints := strings.Split(str, ",")
	if len(endpoints) > 0 {
		vrn.Endpoints = endpoints
	}
	return vrn, nil
}
