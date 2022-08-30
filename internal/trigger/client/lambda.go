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

package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	ce "github.com/cloudevents/sdk-go/v2"
)

type awsLambda struct {
	client *lambda.Client
	arn    *string
}

func NewAwsLambdaClient(accessKeyID, secretKeyID, arnStr string) EventClient {
	a, _ := arn.Parse(arnStr)
	credential := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accessKeyID, secretKeyID, ""))
	c := lambda.New(lambda.Options{
		Credentials: credential,
		Region:      a.Region,
	})
	return &awsLambda{
		client: c,
		arn:    &arnStr,
	}
}

func (l *awsLambda) Send(ctx context.Context, event ce.Event) Result {
	payload, err := json.Marshal(event)
	if err != nil {
		return Result{
			StatusCode: ErrInternalCode,
			Err:        err,
		}
	}
	req := &lambda.InvokeInput{
		FunctionName: l.arn,
		Payload:      payload,
	}
	resp, err := l.client.Invoke(ctx, req)
	if err != nil {
		return Result{
			StatusCode: ErrLambdaInvoke,
			Err:        fmt.Errorf("lambda invke error:%w", err),
		}
	}
	if resp.FunctionError != nil {
		return Result{
			StatusCode: int(resp.StatusCode),
			Err:        fmt.Errorf("lambda invoke response error:%s", *resp.FunctionError),
		}
	}
	return Success
}
