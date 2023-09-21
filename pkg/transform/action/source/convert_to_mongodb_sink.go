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

package source

import (
	"fmt"

	"github.com/vanus-labs/vanus/pkg/transform/action"
	"github.com/vanus-labs/vanus/pkg/transform/arg"
	"github.com/vanus-labs/vanus/pkg/transform/common"
	"github.com/vanus-labs/vanus/pkg/transform/context"
)

const debeziumOp = "iodebeziumop"

// ["debezium_convert_to_mongodb_sink","unique_key","unique_value"].
type debeziumConvertToMongoDBSink struct {
	action.CommonAction
}

func NewDebeziumConvertToMongoDBSink() action.Action {
	a := &debeziumConvertToMongoDBSink{}
	a.CommonAction = action.CommonAction{
		ActionName:  "debezium_convert_to_mongodb_sink",
		FixedArgs:   []arg.TypeList{arg.All, arg.All},
		VariadicArg: arg.All,
	}
	return a
}

func (a *debeziumConvertToMongoDBSink) Init(args []arg.Arg) error {
	if len(args)%2 != 0 {
		return fmt.Errorf("arg number invalid, key and keyValue must pair")
	}
	// op arg.
	_arg, _ := arg.NewArg("$." + debeziumOp)
	a.Args = append(a.Args, _arg)
	// data arg.
	_arg, _ = arg.NewArg("$.data")
	a.Args = append(a.Args, _arg)
	a.ArgTypes = []common.Type{common.String, common.Any}
	a.TargetArg = _arg
	for i := 0; i < len(args); {
		// unique key name.
		a.Args = append(a.Args, args[i])
		a.ArgTypes = append(a.ArgTypes, common.String)
		// unique key value path.
		a.Args = append(a.Args, args[i+1])
		a.ArgTypes = append(a.ArgTypes, common.Any)
		i += 2
	}
	return nil
}

func (a *debeziumConvertToMongoDBSink) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	op, _ := args[0].(string)
	data := args[1]
	uniqueMap := make(map[string]interface{})
	for i := 2; i < len(args); {
		uniqueMap[args[i].(string)] = args[i+1]
		i += 2
	}
	result := map[string]interface{}{}
	switch op {
	case "r", "c":
		// insert.
		result["inserts"] = []interface{}{data}
	case "u":
		// update.
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			return fmt.Errorf("data only support map")
		}
		for k := range uniqueMap {
			// remove unique key form update.
			delete(dataMap, k)
		}
		result["updates"] = []interface{}{
			map[string]interface{}{
				"filter": uniqueMap,
				"update": map[string]interface{}{
					"$set": dataMap,
				},
			},
		}
	case "d":
		// delete.
		result["deletes"] = []interface{}{
			map[string]interface{}{
				"filter": uniqueMap,
			},
		}
	default:
		return fmt.Errorf("unknown op %s", op)
	}
	return a.TargetArg.SetValue(ceCtx, result)
}
