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

package transform

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	primitive "github.com/vanus-labs/vanus/pkg"
)

func TestExecute(t *testing.T) {
	Convey("execute transformer", t, func() {
		e := ce.NewEvent()
		e.SetType("testType")
		e.SetSource("testSource")
		e.SetID("testId")
		e.SetExtension("vanuskey", "vanusValue")

		Convey("with text template", func() {
			cfg := &primitive.Transformer{
				Define: map[string]string{
					"keyTest": "keyValue",
					"ctxId":   "$.id",
					"ctxKey":  "$.vanuskey",
					"data":    "$.data",
					"dataKey": "$.data.key",
				},
				Template: primitive.TemplateConfig{
					Type: primitive.TemplateTypeText,
				},
			}

			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})

			cfg.Template.Template = `<dataKey> "<$.data.key1>" <$.data.noExist>`

			t, err := NewTransformer(cfg)
			So(err, ShouldBeNil)

			err = t.Execute(&e)
			So(err, ShouldBeNil)
			So(e.DataContentType(), ShouldEqual, ce.TextPlain)
			So(string(e.Data()), ShouldEqual, `value "value1" `)
		})

		Convey("with json template", func() {
			cfg := &primitive.Transformer{
				Define: map[string]string{
					"keyTest": "keyValue",
					"ctxId":   "$.id",
					"ctxKey":  "$.vanuskey",
					"data":    "$.data",
					"dataKey": "$.data.key",
				},
				Template: primitive.TemplateConfig{
					Type: primitive.TemplateTypeJSON,
				},
			}

			Convey("simple object in data", func() {
				_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
					"key":  "value",
					"key1": "value1",
				})

				Convey("single value", func() {
					cfg.Template.Template = `{ "define": <dataKey>, "data": <$.data.key>, "attribute": <$.id>, "noExist": <$.data.noExist>, "noExistStr": "<$.data.noExist>" }`

					t, err := NewTransformer(cfg)
					So(err, ShouldBeNil)

					err = t.Execute(&e)
					So(err, ShouldBeNil)
					So(string(e.Data()), ShouldEqual, `{"define":"value","data":"value","attribute":"testId","noExistStr":""}`)
				})

				Convey("dynamic string", func() {
					cfg.Template.Template = `{ "data": "source is <dataKey>", "data2": "source is <$.data.noExist>" }`

					t, err := NewTransformer(cfg)
					So(err, ShouldBeNil)

					err = t.Execute(&e)
					So(err, ShouldBeNil)
					So(string(e.Data()), ShouldEqual, `{"data":"source is value","data2":"source is "}`)
				})

				Convey("dynamic string with colon", func() {
					cfg.Template.Template = `{ "data": ":<dataKey>", "data2": "\":<dataKey>\"", "data3": "::<dataKey> other:<ctxId>" }`

					t, err := NewTransformer(cfg)
					So(err, ShouldBeNil)

					err = t.Execute(&e)
					So(err, ShouldBeNil)
					So(string(e.Data()), ShouldEqual, `{"data":":value","data2":"\":value\"","data3":"::value other:testId"}`)
				})

				Convey("dynamic string with quota", func() {
					cfg.Template.Template = `{ "data": "source is \"<dataKey>\"", "data2": "source is \"<$.data.noExist>\"" }`

					t, err := NewTransformer(cfg)
					So(err, ShouldBeNil)

					err = t.Execute(&e)
					So(err, ShouldBeNil)
					So(e.DataContentType(), ShouldEqual, ce.ApplicationJSON)
					So(string(e.Data()), ShouldEqual, `{"data":"source is \"value\"","data2":"source is \"\""}`)
				})
			})

			Convey("execute with all", func() {
				_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
					"issue": map[string]interface{}{
						"html_url": "issue_html_url",
						"title":    "issue_title",
						"number":   123,
					},
				})

				cfg.Define = map[string]string{
					"login":   "abc",
					"comment": "comments",
				}
				cfg.Template.Template = `{ "type": "mrkdwn", "text": "Hi <login>, GitHub user just left a comment in the *\<<$.data.issue.html_url>|<$.data.issue.title> #<$.data.issue.number>\>*.\n *Comment: *<comment>" }`

				t, err := NewTransformer(cfg)
				So(err, ShouldBeNil)

				err = t.Execute(&e)
				So(err, ShouldBeNil)
				So(e.DataContentType(), ShouldEqual, ce.ApplicationJSON)
				So(string(e.Data()), ShouldEqual, `{"type":"mrkdwn","text":"Hi abc, GitHub user just left a comment in the *<issue_html_url|issue_title #123>*.\n *Comment: *comments"}`)
			})

			Convey("execute with line", func() {
				_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
					"issue": map[string]interface{}{
						"html_url": "issue_html_url",
						"title":    "issue_title",
						"number":   123,
						"body":     "first line\r\nsecond line",
					},
					"sender": map[string]interface{}{
						"html_url": "send_html_url",
						"login":    "sender_login",
					},
				})

				cfg.Define = map[string]string{
					"login":  "login_user",
					"repo":   "repo_url",
					"issue":  "$.data.issue.html_url",
					"title":  "$.data.issue.title",
					"body":   "$.data.issue.body",
					"number": "$.data.issue.number",
					"url":    "$.data.sender.html_url",
					"sender": "$.data.sender.login",
				}
				cfg.Template.Template = `{"content": "Hi <login>, GitHub user **[<sender>](<url>)** just opened an issue. Check **[<title> #<number>](<issue>)** out now.\n **Issue body: **<body> "}`

				t, err := NewTransformer(cfg)
				So(err, ShouldBeNil)

				err = t.Execute(&e)
				So(err, ShouldBeNil)
				So(e.DataContentType(), ShouldEqual, ce.ApplicationJSON)
				So(string(e.Data()), ShouldEqual, `{"content":"Hi login_user, GitHub user **[sender_login](send_html_url)** just opened an issue. Check **[issue_title #123](issue_html_url)** out now.\n **Issue body: **first line\r\nsecond line "}`)
			})
		})
	})
}
