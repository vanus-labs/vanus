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

package command

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Record struct {
	ID         string
	BornAt     time.Time
	SentAt     time.Time
	ReceivedAt time.Time
}

type BlockRecord struct {
	LeaderID   uint64
	LeaderAddr string
	Replicas   map[uint64]string
}

func SetCaseName(_name string) {
	name = _name
}

type ResultType string

const (
	ResultLatency    = ResultType("latency")
	ResultThroughput = ResultType("throughput")
)

type Task struct {
	ID          primitive.ObjectID `bson:"_id"`
	Running     bool               `bson:"running"`
	CreateAt    time.Time          `bson:"create_at"`
	CompletedAt time.Time          `bson:"completed_at"`
}

type BenchmarkResult struct {
	ID       primitive.ObjectID                `bson:"_id"`
	TaskID   primitive.ObjectID                `bson:"task_id"`
	CaseName string                            `bson:"case_name"`
	RType    ResultType                        `bson:"result_type"`
	Args     []string                          `bson:"args"`
	Values   map[string]map[string]interface{} `bson:"values"`
	Mean     float64                           `bson:"mean"`
	Stdev    float64                           `bson:"stdev"`
	CreateAt time.Time                         `bson:"createAt"`
}
