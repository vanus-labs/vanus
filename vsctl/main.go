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

// vsctl is a command line application that controls vanus.
package main

import (
	"errors"
	"fmt"
	"github.com/linkall-labs/vanus/vsctl/command"
	"github.com/spf13/cobra"
	"os"
)

const (
	cliName        = "vsctl"
	cliDescription = "the command-line tool for vanus"
)

var (
	rootCmd = &cobra.Command{
		Use:        cliName,
		Short:      cliDescription,
		SuggestFor: []string{"vsctl"},
	}
)

func init() {
	cobra.EnablePrefixMatching = true

	rootCmd.AddCommand(
		command.NewSubscriptionCommand(),
		command.NewEventbusCommand(),
		command.NewEventCommand(),
	)
}

func main() {
	MustStart()
}

func usageFunc(c *cobra.Command) error {
	return errors.New("vsctl")
}

func Start() error {
	rootCmd.SetUsageFunc(usageFunc)
	rootCmd.SetHelpTemplate("vsctl help\n")
	return rootCmd.Execute()
}

func MustStart() {
	if err := Start(); err != nil {
		fmt.Printf("vsctl error: %s", err)
		os.Exit(-1)
	}
}
