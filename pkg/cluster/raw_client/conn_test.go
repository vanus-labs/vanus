package raw_client

import (
	"errors"
	"testing"
)

func Test_try(t *testing.T) {
	err := errors.New("rpc error: code = Unknown desc = {\\\"code\\\":\\\"NOT_LEADER\\\",\\\"message\\\":\\\"\\\"}")
	isNeedRetry(err)
}
