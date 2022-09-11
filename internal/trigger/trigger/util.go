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

package trigger

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/linkall-labs/vanus/client/pkg/eventbus"

	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/client"
	"github.com/linkall-labs/vanus/observability/log"
)

func newEventClient(sink primitive.URI,
	protocol primitive.Protocol,
	credential primitive.SinkCredential) client.EventClient {
	switch protocol {
	case primitive.AwsLambdaProtocol:
		_credential, _ := credential.(*primitive.CloudSinkCredential)
		return client.NewAwsLambdaClient(_credential.AccessKeyID, _credential.SecretAccessKey, string(sink))
	default:
		return client.NewHTTPClient(string(sink))
	}
}

func newEventbusWriter(ctx context.Context, eventbus string, endpoints []string) (eventbus.BusWriter, error) {
	vrn := fmt.Sprintf("vanus:///eventbus/%s?controllers=%s", eventbus, strings.Join(endpoints, ","))
	busWriter, err := eb.OpenBusWriter(ctx, vrn)
	if err != nil {
		log.Error(ctx, "open bus writer failed", map[string]interface{}{
			log.KeyError:        err,
			log.KeyEventbusName: eventbus,
		})
		return nil, err
	}
	return busWriter, nil
}

func isShouldRetry(statusCode int) (bool, string) {
	switch statusCode {
	case 400:
		return false, "BadRequest"
	case 403:
		return false, "Forbidden"
	case 413:
		return false, "RequestEntityTooLarge"
	default:
		return true, ""
	}
}

func calDeliveryTime(attempts int32) time.Duration {
	var v int
	switch {
	case attempts >= 10:
		v = 3600
	case attempts >= 4:
		v = int(30 * math.Pow(2, float64(attempts-4)))
	case attempts >= 2:
		v = int(5 * (attempts - 1))
	default:
		v = 1
	}
	return time.Duration(v) * time.Second
}

func getRetryAttempts(attempts interface{}) (int32, error) {
	switch v := attempts.(type) {
	case int32:
		return v, nil
	case string:
		intV, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return int32(intV), nil
		}
		return 0, fmt.Errorf("parse int error: %w", err)
	default:
		return 0, fmt.Errorf("attempts type %v not support", v)
	}
}
