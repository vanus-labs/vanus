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
	"strings"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/tidwall/gjson"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidExpression = fmt.Errorf("expression is invalid,format is: $json_path.(type)")
)

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
			Path: expression[pos+1 : typeStartPos],
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

	if !proto.Equal(ast.ResultType(), decls.Bool) {
		return nil, fmt.Errorf("expression must return bool type,but got %s", ast.ResultType().String())
	}

	return env.Program(ast)
}

func (e *Expression) Eval(event ce.Event) (bool, error) {
	vars := make(map[string]interface{})

	for _, v := range e.variables {
		switch v.Type {
		case "string":
			vars[v.Name] = gjson.GetBytes(event.Data(), v.Path).String()
		case "int64":
			vars[v.Name] = gjson.GetBytes(event.Data(), v.Path).Int()
		case "uint64":
			vars[v.Name] = gjson.GetBytes(event.Data(), v.Path).Uint()
		case "bool":
			vars[v.Name] = gjson.GetBytes(event.Data(), v.Path).Bool()
		case "double":
			vars[v.Name] = gjson.GetBytes(event.Data(), v.Path).Float()
		}
	}
	out, _, err := e.program.Eval(vars)
	if err != nil {
		return false, err
	}
	return out.Value().(bool), nil
}
