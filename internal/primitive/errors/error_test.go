package errors

import (
	"errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/tidwall/gjson"
	"testing"
)

func TestErrorCodeAllMethods(t *testing.T) {
	Convey("test errorCode.Equals", t, func() {
		err1 := ErrCreateEventbus
		err2 := ErrAllocateSegment
		Convey("equals testing", func() {
			So(err1.Equals(errors.New("test")), ShouldBeFalse)
			So(err1.Equals(err2), ShouldBeFalse)

			err3 := ErrCreateEventbus
			So(err1.Equals(err3), ShouldBeTrue)

			err1.Wrap(ErrNoServerAvailable)
			err1.Explain("try to find a segment server to create segment, but no server available")
			So(err1.Equals(err3), ShouldBeTrue)
		})

		Convey("explain & wrap testing", func() {
			msg := "try to find a segment server to create segment, but no server available"
			err1.Explain(msg)
			So(err1.explanation, ShouldEqual, msg)

			err1.Wrap(ErrNoServerAvailable)
			So(err1.underlayErrors[0], ShouldResemble, ErrNoServerAvailable)
		})

		Convey("string testing", func() {
			msg := "try to find a segment server to create segment, but no server available"
			err1.Explain(msg)
			expect := `{"code":2001, ` +
				`"message": "failed to create eventbus", ` +
				`"explanation": "try to find a segment server to create segment, but no server available"}`

			errMsg := err1.Error()
			So(gjson.Valid(errMsg), ShouldBeTrue)
			So(errMsg, ShouldEqual, expect)

			obj := gjson.Parse(errMsg)
			So(int(obj.Get("code").Int()), ShouldEqual, 2001)
			So(obj.Get("message").String(), ShouldEqual, "failed to create eventbus")
			So(obj.Get("explanation").String(), ShouldEqual,
				"try to find a segment server to create segment, but no server available")

			err1.Wrap(ErrNoServerAvailable)
			expect = `{"code":2001, "message": "failed to create eventbus", ` +
				`"explanation": "try to find a segment server to create segment, but no server available", ` +
				`"err0": {"code":1003, "message": "no server available"}}`
			errMsg = err1.Error()
			So(gjson.Valid(errMsg), ShouldBeTrue)
			So(errMsg, ShouldEqual, expect)
			err1.Wrap(nil)
			err1.Wrap(errors.New(""))
			errMsg = err1.Error()
			So(gjson.Valid(errMsg), ShouldBeTrue)
			So(errMsg, ShouldEqual, expect)

			err1.Wrap(ErrAllocateSegment)
			err1.Wrap(errors.New("testing error"))
			expect = `{"code":2001, "message": "failed to create eventbus", ` +
				`"explanation": "try to find a segment server to create segment, but no server available", ` +
				`"err0": {"code":1003, "message": "no server available"}, ` +
				`"err1": {"code":2003, "message": "failed to allocate segment"}, ` +
				`"err2": "testing error"}`
			errMsg = err1.Error()
			So(gjson.Valid(errMsg), ShouldBeTrue)
			So(errMsg, ShouldEqual, expect)
			obj = gjson.Parse(errMsg)
			obj.Get("err1").Get("code")
			So(int(obj.Get("err1").Get("code").Int()), ShouldEqual, 2003)
			So(obj.Get("err1").Get("message").String(), ShouldEqual,
				"failed to allocate segment")
			So(obj.Get("err1").Get("explanation").String(), ShouldBeEmpty)
			So(obj.Get("err2").String(), ShouldEqual, "testing error")
		})
	})
}

func TestChain(t *testing.T) {

}
