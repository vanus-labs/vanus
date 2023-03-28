// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package timer

import (
	// first-party libraries.
	"github.com/vanus-labs/vanus/observability"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/timer"
)

type Config struct {
	timer.Config `yaml:",inline"`

	Observability observability.Config `yaml:"observability"`
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	timer.Default(&c.Config)
	return c, nil
}
