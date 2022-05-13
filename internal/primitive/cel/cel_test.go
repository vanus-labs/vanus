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

package cel_test

import (
	"testing"

	"github.com/linkall-labs/vanus/internal/primitive/cel"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestParse(t *testing.T) {
	event := ce.NewEvent()
	event.SetData(ce.ApplicationJSON, map[string]interface{}{
		"key": "test",
	})
	Convey("cel parse", t, func() {
		p, err := cel.Parse("$key.(string) == 'test'")
		So(err, ShouldBeNil)
		b, err := p.Eval(event)
		So(err, ShouldBeNil)
		So(b, ShouldBeTrue)
	})
}
