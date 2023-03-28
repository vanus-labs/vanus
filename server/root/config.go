// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package root

import (
	// first-party libraries.
	"github.com/vanus-labs/vanus/observability"

	// this project.
	"github.com/vanus-labs/vanus/internal/controller"
	"github.com/vanus-labs/vanus/internal/primitive"
)

type Config struct {
	controller.Config `yaml:",inline"`

	Observability observability.Config `yaml:"observability"`
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
