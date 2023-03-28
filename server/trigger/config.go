// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package trigger

import (
	// standard libraries.
	"fmt"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/pkg/util"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/trigger"
)

type Config struct {
	trigger.Config `yaml:",inline"`

	Observability observability.Config `yaml:"observability"`
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	if c.IP == "" {
		c.IP = util.GetLocalIP()
	}
	c.TriggerAddr = fmt.Sprintf("%s:%d", c.IP, c.Port)
	return c, nil
}
