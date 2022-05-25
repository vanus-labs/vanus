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

package log

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type Logger interface {
	Debug(ctx context.Context, msg string, fields map[string]interface{})
	Info(ctx context.Context, msg string, fields map[string]interface{})
	Warning(ctx context.Context, msg string, fields map[string]interface{})
	Error(ctx context.Context, msg string, fields map[string]interface{})
	Fatal(ctx context.Context, msg string, fields map[string]interface{})
	SetLevel(level string)
	SetLogWriter(writer io.Writer)
}

func init() {
	logger := logrus.New()
	logger.Formatter = &logrus.TextFormatter{TimestampFormat: time.RFC3339Nano, FullTimestamp: true}
	r := &defaultLogger{
		logger: logger,
	}
	level := os.Getenv("VANUS_LOG_LEVEL")
	switch strings.ToLower(level) {
	case "debug":
		r.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		r.logger.SetLevel(logrus.WarnLevel)
	case "error":
		r.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		r.logger.SetLevel(logrus.FatalLevel)
	default:
		r.logger.SetLevel(logrus.InfoLevel)
	}
	vLog = r
	vLog.Debug(context.Background(), "logger level has been set", map[string]interface{}{
		"log_level": level,
	})
}

var vLog Logger

type defaultLogger struct {
	logger *logrus.Logger
}

func (l *defaultLogger) Debug(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Debug(msg)
}

func (l *defaultLogger) Info(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Info(msg)
}

func (l *defaultLogger) Warning(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Warning(msg)
}

func (l *defaultLogger) Error(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).WithFields(fields).Error(msg)
}

func (l *defaultLogger) Fatal(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Fatal(msg)
}

func (l *defaultLogger) SetLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		l.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		l.logger.SetLevel(logrus.WarnLevel)
	case "error":
		l.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		l.logger.SetLevel(logrus.FatalLevel)
	default:
		l.logger.SetLevel(logrus.InfoLevel)
	}
}

func (l *defaultLogger) SetLogWriter(writer io.Writer) {
	l.logger.Out = writer
	return
}

// SetLogger use specified logger user customized, in general, we suggest user to replace the default logger with specified
func SetLogger(logger Logger) {
	vLog = logger
}
func SetLogLevel(level string) {
	if level == "" {
		return
	}
	vLog.SetLevel(level)
}

func SetLogWriter(writer io.Writer) {
	if writer == nil {
		return
	}
	vLog.SetLogWriter(writer)
}

func Debug(ctx context.Context, msg string, fields map[string]interface{}) {
	vLog.Debug(ctx, msg, fields)
}

func Info(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	vLog.Info(ctx, msg, fields)
}

func Warning(ctx context.Context, msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	vLog.Warning(ctx, msg, fields)
}

func Error(ctx context.Context, msg string, fields map[string]interface{}) {
	vLog.Error(ctx, msg, fields)
}

//
//func Fatal(ctx context.Context, msg string, fields map[string]interface{}) {
//	vLog.Fatal(ctx, msg, fields)
//}
