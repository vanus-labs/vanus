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

package cel

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/vanus-labs/vanus/pkg/util"
)

var ErrInvalidExpression = fmt.Errorf("expression is invalid,format is: $json_path.(type)")

type Expression struct {
	program   cel.Program
	variables map[string]Variable
}

type Variable struct {
	Name string
	Path string
	Type string
}

func Parse(expression string) (*Expression, error) {
	expr, vars, err := parseExpression(expression)
	if err != nil {
		return nil, err
	}
	program, err := newCelProgram(expr, vars)
	if err != nil {
		return nil, err
	}
	return &Expression{
		program:   program,
		variables: vars,
	}, nil
}

// parseExpressionString breaks inline expression string into Google Expression expression
// and a set of variable definitions, e.g.:
// '$foo.(string) == "bar"' becomes
// expr: foo == "bar", vars: ["foo": string]
func parseExpression(expression string) (string, map[string]Variable, error) {
	varMap := make(map[string]Variable)
	var expr string
	for i := 0; i < len(expression); i++ {
		expression = expression[i:]

		pos := strings.Index(expression, "$")
		if pos == -1 {
			expr += expression
			break
		}

		typeStartPos := strings.Index(expression[pos:], ".(")
		if typeStartPos == -1 {
			return "", nil, ErrInvalidExpression
		}
		typeStartPos += pos

		typeEndPos := strings.Index(expression[pos:], ")")
		if typeEndPos == -1 {
			return "", nil, ErrInvalidExpression
		}
		typeEndPos += pos

		i = typeEndPos
		expr += expression[:pos]

		safeCELName := strings.ReplaceAll(expression[pos+1:typeStartPos], ".", "_")
		// integer as the pos name first symbol causes issue with matching
		// var types. String prefix ensures that we don't have first integer symbol.
		safeCELName = "vanus_" + safeCELName

		if pos+1 > typeStartPos || typeStartPos+2 > typeEndPos {
			return "", nil, ErrInvalidExpression
		}

		varMap[safeCELName] = Variable{
			Name: safeCELName,
			Path: "$." + expression[pos+1:typeStartPos],
			Type: expression[typeStartPos+2 : typeEndPos],
		}
		expr += safeCELName
	}
	return expr, varMap, nil
}

func newCelProgram(expr string, vars map[string]Variable) (cel.Program, error) {
	declVars := make([]*exprpb.Decl, 0)
	var pType *exprpb.Type
	for _, variable := range vars {
		switch variable.Type {
		case "string":
			pType = decls.String
		case "int64":
			pType = decls.Int
		case "uint64":
			pType = decls.Uint
		case "bool":
			pType = decls.Bool
		case "double":
			pType = decls.Double
		default:
			return nil, fmt.Errorf("expression type only support [string,int64,uint64,bool,double],but got %s", variable.Type)
		}
		declVars = append(declVars, decls.NewVar(variable.Name, pType))
	}

	env, err := cel.NewEnv(
		cel.Declarations(declVars...),
	)
	if err != nil {
		return nil, err
	}

	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, iss.Err()
	}

	if ast.OutputType() != cel.BoolType {
		return nil, fmt.Errorf("expression must return bool type, but got %s", ast.OutputType().String())
	}

	return env.Program(ast)
}

func (e *Expression) Eval(event ce.Event) (bool, error) {
	vars := make(map[string]interface{})
	obj, err := util.ParseJSON(event.Data())
	if err != nil {
		return false, err
	}
	for _, v := range e.variables {
		value, err := util.GetJSONValue(obj, v.Path)
		if err != nil {
			return false, err
		}
		var val interface{}

		switch v.Type {
		case "string":
			val, err = stringValue(value)
			if err != nil {
				return false, err
			}
		case "int64":
			val, err = intValue(value)
			if err != nil {
				return false, err
			}
		case "uint64":
			val, err = uintValue(value)
			if err != nil {
				return false, err
			}
		case "bool":
			val, err = boolValue(value)
			if err != nil {
				return false, err
			}
		case "double":
			val, err = floatValue(value)
			if err != nil {
				return false, err
			}
		}
		vars[v.Name] = val
	}
	out, _, err := e.program.Eval(vars)
	if err != nil {
		return false, err
	}
	return out.Value().(bool), nil
}

func stringValue(value interface{}) (string, error) {
	v, ok := value.(string)
	if ok {
		return v, nil
	}
	reflectValue := reflect.ValueOf(value)
	switch reflectValue.Kind() {
	case reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Bool:
		return fmt.Sprintf("%v", value), nil
	default:
		return "", fmt.Errorf("%s can't convert to string", reflectValue.Kind().String())
	}
}

func intValue(value interface{}) (int64, error) {
	reflectValue := reflect.ValueOf(value)
	switch {
	case reflectValue.CanInt():
		return reflectValue.Int(), nil
	case reflectValue.CanFloat():
		return int64(reflectValue.Float()), nil
	case reflectValue.CanUint():
		return int64(reflectValue.Uint()), nil
	case reflectValue.Kind() == reflect.String:
		v, err := strconv.ParseInt(value.(string), 10, 64)
		return v, err
	default:
		return 0, fmt.Errorf("%s can't convert to int64", reflectValue.Kind().String())
	}
}

func uintValue(value interface{}) (uint64, error) {
	reflectValue := reflect.ValueOf(value)
	switch {
	case reflectValue.CanUint():
		return reflectValue.Uint(), nil
	case reflectValue.CanFloat():
		return uint64(reflectValue.Float()), nil
	case reflectValue.CanInt():
		return uint64(reflectValue.Int()), nil
	case reflectValue.Kind() == reflect.String:
		v, err := strconv.ParseUint(value.(string), 10, 64)
		return v, err
	default:
		return 0, fmt.Errorf("%s can't convert to uint64", reflectValue.Kind().String())
	}
}

func boolValue(value interface{}) (bool, error) {
	reflectValue := reflect.ValueOf(value)
	switch reflectValue.Kind() {
	case reflect.Bool:
		return reflectValue.Bool(), nil
	case reflect.String:
		b, err := strconv.ParseBool(reflectValue.String())
		return b, err
	default:
		return false, fmt.Errorf("%s can't convert to bool", reflectValue.Kind().String())
	}
}

func floatValue(value interface{}) (float64, error) {
	reflectValue := reflect.ValueOf(value)
	switch {
	case reflectValue.CanFloat():
		return reflectValue.Float(), nil
	case reflectValue.CanInt():
		return float64(reflectValue.Int()), nil
	case reflectValue.CanUint():
		return float64(reflectValue.Uint()), nil
	case reflectValue.Kind() == reflect.String:
		v, err := strconv.ParseFloat(value.(string), 64)
		return v, err
	default:
		return 0, fmt.Errorf("%s can't convert to float64", reflectValue.Kind().String())
	}
}
