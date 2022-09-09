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
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	eb "github.com/linkall-labs/vanus/client"
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

// getControllerAddrs return the endpoints of controller.
func (srv *httpServer) getControllerEndpoints(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"endpoints": srv.cfg.ControllerAddr,
	})
}

func (srv *httpServer) getEvents(c echo.Context) error {
	ctx := context.Background()
	eventid := c.QueryParam("eventid")
	eventbus := c.QueryParam("eventbus")
	if eventbus == "" && eventid == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "the eventbus name and event id can't be both empty",
		})
	}
	if eventid != "" {
		event, err := eb.SearchEventByID(ctx, eventid, srv.cfg.ControllerAddr[0])
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "can not query the event by event id",
			})
		}
		return c.JSON(http.StatusOK, map[string]interface{}{
			"events": []*v2.Event{event},
		})
	}

	var offset int
	var num int16
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
	} else {
		num = 1
	}
	vrn := fmt.Sprintf("vanus:///eventbus/%s?controllers=%s",
		eventbus, strings.Join(srv.cfg.ControllerAddr, ","))
	ls, err := eb.LookupReadableLogs(ctx, vrn)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("lookup eventlog failed: %s", err),
		})
	}

	r, err := eb.OpenLogReader(ctx, ls[0].VRN, eb.DisablePolling())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("open eventlog failed: %s", err),
		})
	}
	defer r.Close(ctx)

	_, err = r.Seek(context.Background(), int64(offset), io.SeekStart)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("seek offset failed: %s", err),
		})
	}

	events, err := r.Read(context.Background(), num)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("read event failed: %s", err),
		})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"events": events,
	})
}
