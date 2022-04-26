# Vanus

[![License](https://img.shields.io/badge/License-Apache_2.0-green.svg)](https://github.com/linkall-labs/vanus/blob/main/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![codecov](https://codecov.io/gh/linkall-labs/vanus/branch/main/graph/badge.svg?token=RSXSIMEY4V)](https://codecov.io/gh/linkall-labs/vanus)

Vanus is a Serverless Event Platform for Event-driven Architecture

## Project Layout

This Project follows [golang-standards/project-layout](https://github.com/golang-standards/project-layout), see the
project for understanding well vanus' codebase.

## Development

### gRPC API Testing

1. install [grpcui](https://github.com/fullstorydev/grpcui)

```bash
go install github.com/fullstorydev/grpcui/cmd/grpcui@latest
```

2. run `grpcui`

```bash
# see Makefile for more commands
# before start grpcui, the target server must has been started
make controller-start module=eventbus
make controller-api-test
```