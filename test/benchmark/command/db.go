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
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/linkall-labs/vanus/observability/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	rdb        *redis.Client
	mgo        *mongo.Client
	taskColl   *mongo.Collection
	resultColl *mongo.Collection
	taskID     primitive.ObjectID
)

const (
	database = "vanus-benchmark"
)

func InitDatabase(redisAddr string, mongodb string, begin bool) {
	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	cmd := rdb.Ping(context.Background())
	if cmd.Err() != nil {
		panic("failed to connect redis: " + cmd.Err().Error())
	}
	log.Info(nil, "connect to redis success", map[string]interface{}{
		"addr": redisAddr,
	})

	cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongodb))
	if err != nil {
		panic("failed to connect mongodb: " + err.Error())
	}
	log.Info(nil, "connect to mongodb success", nil)
	mgo = cli
	taskColl = cli.Database(database).Collection("tasks")
	resultColl = cli.Database(database).Collection("results")
	c, err := taskColl.Find(context.Background(), bson.M{
		"running": true,
	})
	if err != nil {
		panic("failed to count tasks: " + err.Error())
	}
	tasks := make([]*Task, 0)
	for c.Next(context.Background()) {
		t := &Task{}
		_ = c.Decode(t)
		tasks = append(tasks, t)
	}

	if begin {
		if len(tasks) != 0 {
			panic(fmt.Sprintf("invalid taks numbers: %d", len(tasks)))
		}
		t := &Task{
			ID:       primitive.NewObjectID(),
			CreateAt: time.Now(),
			Running:  true,
		}
		_, err := taskColl.InsertOne(context.Background(), t)
		if err != nil {
			panic("failed to create task into mongodb: " + err.Error())
		}
		taskID = t.ID
		log.Info(nil, "create a new task", map[string]interface{}{
			"task_id": taskID.Hex(),
		})
	} else {
		if len(tasks) != 1 {
			panic(fmt.Sprintf("invalid taks numbers: %d", len(tasks)))
		}
		taskID = tasks[0].ID
		log.Info(nil, "find a existed task", map[string]interface{}{
			"task_id": taskID.Hex(),
		})
	}
}

func CloseDatabases(end bool) {
	if end {
		res, err := taskColl.UpdateMany(context.Background(), bson.M{
			"running": true,
		}, bson.M{
			"$set": bson.M{
				"running":   false,
				"update_at": time.Now(),
			},
		})
		if err != nil {
			log.Error(nil, "failed to update task status", map[string]interface{}{
				log.KeyError: err,
			})
		}
		log.Info(nil, "task is completed", map[string]interface{}{
			"task_id": res.UpsertedID,
		})
	}
	_ = rdb.Close()
	_ = mgo.Disconnect(context.Background())
}
