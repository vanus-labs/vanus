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

package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/linkall-labs/vanus/test/benchmark/command"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:        "vanus-bench",
	Short:      "the benchmark tool of vanus",
	SuggestFor: []string{"vsctl-bench"},
}

var (
	defaultMongoDBURI = "mongodb+srv://vanus:%s@cluster0.ywakulp.mongodb.net/?retryWrites=true&w=majority"
	caseNames         = map[string]bool{
		"e2e-1eventbus-1eventlog-1client-1parallelism":   true,
		"e2e-1eventbus-1eventlog-1client-16parallelism":  true,
		"e2e-1eventbus-16eventlog-1client-16parallelism": true,
	}

	name        string
	endpoint    string
	redisAddr   string
	mongodbPass string
	begin       bool
	end         bool
)

func main() {

	rootCmd.AddCommand(command.E2ECommand())
	rootCmd.AddCommand(command.ComponentCommand())
	rootCmd.PersistentPreRun = func(_ *cobra.Command, _ []string) {
		if !caseNames[name] {
			panic("invalid case name: " + name)
		}
		command.SetCaseName(name)
		command.InitDatabase(redisAddr, fmt.Sprintf(defaultMongoDBURI, mongodbPass), begin)
	}
	rootCmd.PersistentPostRun = func(_ *cobra.Command, _ []string) {
		command.CloseDatabases(end)
	}

	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint",
		"127.0.0.1:8080", "the endpoints of vanus controller")
	rootCmd.PersistentFlags().StringVar(&redisAddr, "redis-addr",
		"127.0.0.1:6379", "address of redis")
	rootCmd.PersistentFlags().StringVar(&defaultMongoDBURI, "mongodb-uri",
		defaultMongoDBURI, "uri of mongo")
	rootCmd.PersistentFlags().StringVar(&mongodbPass, "mongodb-password",
		mongodbPass, "password for mongo")
	rootCmd.PersistentFlags().StringVar(&name, "name", "", "task name")

	rootCmd.PersistentFlags().BoolVar(&begin, "begin", false, "if the begin of a playbook")
	rootCmd.PersistentFlags().BoolVar(&end, "end", false, "if the end of a playbook")

	if err := rootCmd.Execute(); err != nil {
		color.Red("vsctl-bench run error: %s", err)
		os.Exit(-1)
	}
}
