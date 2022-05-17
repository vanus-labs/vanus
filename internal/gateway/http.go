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
	"fmt"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	eb "github.com/linkall-labs/eventbus-go"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
)

type httpServer struct {
	e   *echo.Echo
	cfg Config
}

func MustStartHTTP(cfg Config) {
	// Echo instance
	e := echo.New()
	srv := &httpServer{
		e:   e,
		cfg: cfg,
	}
	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Routes
	e.GET("/getControllerEndpoints", srv.getControllerEndpoints)
	e.GET("/getEvents", srv.getEvents)

	// Start server
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", cfg.Port+1)))
}

// getControllerAddrs return the endpoints of controller
func (srv *httpServer) getControllerEndpoints(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"endpoints": srv.cfg.ControllerAddr,
	})
}

func (srv *httpServer) getEvents(c echo.Context) error {
	ctx := context.Background()
	eventbus := c.QueryParam("eventbus")
	if eventbus == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "the eventbus name can't be empty",
		})
	}
	var offset int
	var num int
	var err error
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
	} else {
		offset = 0 // TODO use latest
	}
	numStr := c.QueryParam("number")
	if numStr != "" {
		num, err = strconv.Atoi(numStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": fmt.Sprintf("invalid number: %s", err),
			})
		}
	} else {
		num = 1
	}
	if num >= math.MaxInt16 {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("number exceeded, must < 32767: %s", err),
		})
	}

	vrn := fmt.Sprintf("vanus:///eventbus/%s?controllers=%s",
		eventbus, strings.Join(srv.cfg.ControllerAddr, ","))
	ls, err := eb.LookupReadableLogs(ctx, vrn)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("lookup eventlog failed: %s", err),
		})
	}

	r, err := eb.OpenLogReader(ls[0].VRN)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("open eventlog failed: %s", err),
		})
	}
	defer r.Close()

	_, err = r.Seek(context.Background(), int64(offset), io.SeekStart)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("seek offset failed: %s", err),
		})
	}

	events, err := r.Read(context.Background(), int16(num))
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("read event failed: %s", err),
		})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"events": events,
	})
}
