# Vanus

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