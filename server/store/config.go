// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package store

import (
	// first-party libraries.
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/pkg/util"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/store"
)

type Config struct {
	store.Config `yaml:",inline"`

	Observability observability.Config `yaml:"observability"`
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	if err := primitive.LoadConfig(filename, c); err != nil {
		return nil, err
	}
	if c.IP == "" {
		c.IP = util.GetLocalIP()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}
