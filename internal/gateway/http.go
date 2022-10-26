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

package gateway

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/option"
	"github.com/linkall-labs/vanus/client/pkg/policy"
)

type Server struct {
	e      *echo.Echo
	cfg    Config
	client eb.Client
}

func NewHTTPServer(cfg Config) *Server {
	// Echo instance
	e := echo.New()
	return &Server{
		e:      e,
		cfg:    cfg,
		client: eb.Connect(cfg.ControllerAddr),
	}
}

func (s *Server) MustStartHTTP() {
	// Middleware
	s.e.Use(middleware.Logger())
	s.e.Use(middleware.Recover())

	// Routes
	s.e.GET("/getControllerEndpoints", s.getControllerEndpoints)
	s.e.GET("/getEvents", s.getEvents)

	// Start server
	s.e.Logger.Fatal(s.e.Start(fmt.Sprintf(":%d", s.cfg.Port+1)))
}

// getControllerAddrs return the endpoints of controller.
func (s *Server) getControllerEndpoints(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"endpoints": s.cfg.ControllerAddr,
	})
}

func (s *Server) getEvents(c echo.Context) error {
	ctx := context.Background()
	eventid := c.QueryParam("eventid")
	eventbus := c.QueryParam("eventbus")
	if eventbus == "" && eventid == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "the eventbus name and event id can't be both empty",
		})
	}
	if eventid != "" {
		logID, off, err := decodeEventID(eventid)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "decode event id failed",
			})
		}

		l, err := s.client.Eventbus(ctx, eventbus).GetLog(ctx, logID)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": fmt.Sprintf("get eventlog failed: %s", err),
			})
		}

		readPolicy := option.WithReadPolicy(policy.NewManuallyReadPolicy(l, off))
		events, _, _, err := s.client.Eventbus(ctx, eventbus).Reader().Read(ctx, readPolicy, option.WithDisablePolling())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": err.Error(),
			})
		}
		return c.JSON(http.StatusOK, map[string]interface{}{
			"events": events,
		})
	}

	var (
		offset int   // TODO use latest
		num    int16 = 1
		err    error
	)
	offsetStr := c.QueryParam("offset")
	if offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": fmt.Sprintf("invalid offset: %s", err),
			})
		}
		if offset < 0 {
			offset = 0
		}
	}
	numStr := c.QueryParam("number")
	if numStr != "" {
		numPara, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": fmt.Sprintf("invalid number: %s", err),
			})
		}
		if numPara > math.MaxInt16 {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "number exceeded, maximum is 32767",
			})
		}
		num = int16(numPara)
	}

	ls, err := s.client.Eventbus(ctx, eventbus).ListLog(ctx)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("list eventlog failed: %s", err),
		})
	}

	events, _, _, err := s.client.Eventbus(ctx, eventbus).Reader(option.WithDisablePolling()).Read(ctx,
		option.WithReadPolicy(policy.NewManuallyReadPolicy(ls[0], int64(offset))),
		option.WithBatchSize(int(num)))
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
	}
	return c.JSON(http.StatusOK, map[string]interface{}{
		"events": events,
	})
}

func decodeEventID(eventID string) (uint64, int64, error) {
	decoded, err := base64.StdEncoding.DecodeString(eventID)
	if err != nil {
		return 0, 0, err
	}
	if len(decoded) != 16 { // fixed length
		return 0, 0, fmt.Errorf("invalid event id")
	}
	logID := binary.BigEndian.Uint64(decoded[0:8])
	off := binary.BigEndian.Uint64(decoded[8:16])
	return logID, int64(off), nil
}
