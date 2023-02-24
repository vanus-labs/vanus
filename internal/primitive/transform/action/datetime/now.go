package datetime

import (
	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function"
)

func now() action.Action {
	a := &action.SourceTargetSameAction{}
	a.CommonAction = action.CommonAction{
		ActionName:  "NOW",
		FixedArgs:   []arg.TypeList{arg.EventList, arg.All},
		VariadicArg: arg.All,
		Fn:          function.NowFunction,
	}
	return a
}
