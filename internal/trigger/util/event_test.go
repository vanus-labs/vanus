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
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLookupData(t *testing.T) {
	Convey("test lookup data", t, func() {
		data := map[string]interface{}{
			"map": map[string]interface{}{
				"number": 123.4,
				"str":    "str",
			},
			"str":   "stringV",
			"array": []interface{}{1.1, "str"},
		}
		Convey("get string", func() {
			v, err := LookupData(data, "$.str")
			So(err, ShouldBeNil)
			So(v, ShouldEqual, "stringV")
		})
		Convey("get map", func() {
			v, err := LookupData(data, "$.map")
			So(err, ShouldBeNil)
			So(len(v.(map[string]interface{})), ShouldEqual, 2)
		})
		Convey("get array", func() {
			v, err := LookupData(data, "$.array")
			So(err, ShouldBeNil)
			So(len(v.([]interface{})), ShouldEqual, 2)
		})
	})
}

func TestSetAttribute(t *testing.T) {
	Convey("test set attribute", t, func() {
		event := ce.NewEvent()
		Convey("set event spec attribute", func() {
			err := SetAttribute(&event, "id", "idV")
			So(err, ShouldBeNil)
			So(event.ID(), ShouldEqual, "idV")
			err = SetAttribute(&event, "source", "sourceV")
			So(err, ShouldBeNil)
			So(event.Source(), ShouldEqual, "sourceV")
			err = SetAttribute(&event, "type", "typeV")
			So(err, ShouldBeNil)
			So(event.Type(), ShouldEqual, "typeV")
			err = SetAttribute(&event, "source", "sourceV")
			So(err, ShouldBeNil)
			So(event.Source(), ShouldEqual, "sourceV")
			err = SetAttribute(&event, "subject", "subjectV")
			So(err, ShouldBeNil)
			So(event.Subject(), ShouldEqual, "subjectV")
			err = SetAttribute(&event, "subject", "subjectV")
			So(err, ShouldBeNil)
			So(event.Subject(), ShouldEqual, "subjectV")
			err = SetAttribute(&event, "dataschema", "http://schema.com/1")
			So(err, ShouldBeNil)
			So(event.DataSchema(), ShouldEqual, "http://schema.com/1")
			Convey("test time", func() {
				now := time.Now()
				err = SetAttribute(&event, "time", now)
				So(err, ShouldBeNil)
				So(event.Time(), ShouldEqual, now)
				err = SetAttribute(&event, "time", &now)
				So(err, ShouldBeNil)
				So(event.Time(), ShouldEqual, now)
				err = SetAttribute(&event, "time", ce.Timestamp{Time: now})
				So(err, ShouldBeNil)
				So(event.Time(), ShouldEqual, now)
				err = SetAttribute(&event, "time", now.Format(time.RFC3339Nano))
				So(err, ShouldBeNil)
				So(event.Time(), ShouldEqual, now)
			})
			err = SetAttribute(&event, "datacontenttype", "json")
			So(err, ShouldNotBeNil)
			err = SetAttribute(&event, "specversion", "1.0")
			So(err, ShouldNotBeNil)
		})
		Convey("set event extension", func() {
			err := SetAttribute(&event, "vanus", "vanusV")
			So(err, ShouldBeNil)
			So(event.Extensions()["vanus"], ShouldEqual, "vanusV")
			err = SetAttribute(&event, "Vanus", "vanusV")
			So(err, ShouldBeNil)
			So(event.Extensions()["vanus"], ShouldEqual, "vanusV")
			err = SetAttribute(&event, "vanus.vanus", "vanusV")
			So(err, ShouldNotBeNil)
		})
		Convey("set value nil", func() {
			err := SetAttribute(&event, "id", nil)
			So(err, ShouldNotBeNil)
			err = SetAttribute(&event, "vanus", nil)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestDeleteAttribute(t *testing.T) {
	Convey("test delete attribute", t, func() {
		event := ce.NewEvent()
		event.SetID("idV")
		event.SetSource("sourceV")
		event.SetType("typeV")
		event.SetExtension("vanus", "vanusV")
		Convey("delete spec attribute", func() {
			err := DeleteAttribute(&event, "id")
			So(err, ShouldNotBeNil)
		})
		Convey("delete extension attribute", func() {
			Convey("delete exist", func() {
				err := DeleteAttribute(&event, "vanus")
				So(err, ShouldBeNil)
				_, exist := event.Extensions()["vanus"]
				So(exist, ShouldBeFalse)
			})
			Convey("delete not exist", func() {
				err := DeleteAttribute(&event, "van")
				So(err, ShouldBeNil)
				_, exist := event.Extensions()["van"]
				So(exist, ShouldBeFalse)
			})
		})
	})
}

func TestSetData(t *testing.T) {
	Convey("test set event data", t, func() {
		data := map[string]interface{}{
			"map": map[string]interface{}{
				"number": 123.4,
				"str":    "str",
			},
			"str":   "stringV",
			"array": []interface{}{1.1, "str"},
		}
		Convey("test add root key", func() {
			Convey("add common", func() {
				SetData(data, "addKey1", 1.1)
				SetData(data, "addKey2", "str")
				SetData(data, "addKey3", true)
				v, exist := data["addKey1"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, 1.1)
				v, exist = data["addKey2"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, "str")
				v, exist = data["addKey3"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, true)
			})
			Convey("add map", func() {
				SetData(data, "addKey", map[string]interface{}{"k": "v"})
				v, exist := data["addKey"].(map[string]interface{})["k"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, "v")
			})
			Convey("add array", func() {
				SetData(data, "addKey", []interface{}{123, "str"})
				v, exist := data["addKey"].([]interface{})
				So(exist, ShouldBeTrue)
				So(v[0], ShouldEqual, 123)
				So(v[1], ShouldEqual, "str")
			})
		})
		Convey("test replace root key", func() {
			SetData(data, "str", 1.1)
			v, exist := data["str"]
			So(exist, ShouldBeTrue)
			So(v, ShouldEqual, 1.1)
		})
		Convey("test add second key", func() {
			Convey("add common", func() {
				SetData(data, "map.addKey1", 123)
				SetData(data, "map.addKey2", "str")
				SetData(data, "map.addKey3", true)
				v, exist := data["map"].(map[string]interface{})["addKey1"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, 123)
				v, exist = data["map"].(map[string]interface{})["addKey2"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, "str")
				v, exist = data["map"].(map[string]interface{})["addKey3"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, true)
			})
			Convey("add map", func() {
				SetData(data, "map.addKey", map[string]interface{}{"k": "v"})
				v, exist := data["map"].(map[string]interface{})["addKey"].(map[string]interface{})["k"]
				So(exist, ShouldBeTrue)
				So(v, ShouldEqual, "v")
			})
			Convey("add array", func() {
				SetData(data, "map.addKey", []interface{}{123, "string"})
				v, exist := data["map"].(map[string]interface{})["addKey"].([]interface{})
				So(exist, ShouldBeTrue)
				So(v[0], ShouldEqual, 123)
				So(v[1], ShouldEqual, "string")
			})
		})
		Convey("test replace second key", func() {
			SetData(data, "map.str", map[string]interface{}{"k": "v"})
			_, exist := data["map"].(map[string]interface{})["str"]
			So(exist, ShouldBeTrue)
		})
		Convey("test add third key", func() {
			Convey("add map", func() {
				SetData(data, "map.addKey1.addKey2", map[string]interface{}{"k": "v"})
				v, exist := data["map"].(map[string]interface{})["addKey1"].(map[string]interface{})["addKey2"]
				So(exist, ShouldBeTrue)
				So(v.(map[string]interface{})["k"], ShouldEqual, "v")
			})
		})
	})
}

func TestDeleteData(t *testing.T) {
	Convey("test delete event data", t, func() {
		data := map[string]interface{}{
			"map": map[string]interface{}{
				"number": 123.4,
				"str":    "str",
				"array":  []interface{}{1.1, "str"},
				"map": map[string]interface{}{
					"number": 123.4,
					"str":    "str",
					"array":  []interface{}{1.1, "str"},
				},
			},
			"map2": map[string]interface{}{
				"number": 123.4,
				"str":    "str",
				"array":  []interface{}{1.1, "str"},
				"map": map[string]interface{}{
					"number": 123.4,
				},
			},
			"number": 123.4,
			"str":    "stringV",
			"array":  []interface{}{1.1, "str"},
		}
		Convey("delete root key", func() {
			Convey("delete map", func() {
				DeleteData(data, "map")
				_, exist := data["map"]
				So(exist, ShouldBeFalse)
			})
			Convey("delete array", func() {
				DeleteData(data, "array")
				_, exist := data["array"]
				So(exist, ShouldBeFalse)
			})
			Convey("delete common", func() {
				DeleteData(data, "str")
				_, exist := data["str"]
				So(exist, ShouldBeFalse)
				DeleteData(data, "str1")
				_, exist = data["str1"]
				So(exist, ShouldBeFalse)
			})
		})
		Convey("delete second key", func() {
			Convey("delete map", func() {
				DeleteData(data, "map.map")
				_, exist := data["map"].(map[string]interface{})["map"]
				So(exist, ShouldBeFalse)
			})
			Convey("delete array", func() {
				DeleteData(data, "map.array")
				_, exist := data["map"].(map[string]interface{})["array"]
				So(exist, ShouldBeFalse)
			})
			Convey("delete common", func() {
				DeleteData(data, "map.str")
				_, exist := data["map"].(map[string]interface{})["str"]
				So(exist, ShouldBeFalse)
			})
		})
		Convey("delete third key", func() {
			Convey("delete map number", func() {
				DeleteData(data, "map2.map.number")
				_, exist := data["map"].(map[string]interface{})["map"]
				So(exist, ShouldBeTrue)
			})
		})
	})
}
