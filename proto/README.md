# vsproto

Protobuf files for Vanus

## How to use

### Setup IDE

1. install plugin 'Proto Buffer' in marketplace
2. config path: Performance -> Language&Framework -> Proto Buffers -> Import paths;
   and add `include` and `proto` directory to

### install `protogen`

```bash
# download https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protoc-3.19.4-osx-x86_64.zip
unzip protoc-3.19.4-osx-x86_64.zip

mv bin/protoc $GOPATH/bin/ 

go install github.com/golang/protobuf/protoc-gen-go@v1.5.2
```

### import

place `vsproto` on in the same directory in `vanus` project because of below

```go.mod
module github.com/linkall-labs/vanus

go 1.17

require (
	// ...
	github.com/linkall-labs/vanus/proto v0.0.0
)

replace (
	github.com/linkall-labs/vanus/proto v0.0.0 => ../vsproto
)
```

### debug

- use [grpcurl](https://github.com/fullstorydev/grpcurl)
- use [Postman](https://www.postman.com/)

## Makefile

The makefile has pre-define some convenient methods to generate go file from proto,
see file content for details.

## Use Buf

### Install by Homebrew on macOS

```bash
brew install bufbuild/buf/buf
```

### Generate code

```bash
# vanus
buf generate proto

# cloudevents
buf generate --template include/cloudevents/buf.gen.yaml --path include/cloudevents
```

### Editor integration

#### Visual Studio Code

The Visual Studio Code extension can be downloaded from the in-editor extension browser under the name "Buf" or manually via [the extension page](https://marketplace.visualstudio.com/items?itemName=bufbuild.vscode-buf). You need to have buf [installed](https://docs.buf.build/installation) to use it.

#### JetBrain IDEs

IntelliJ IDEA, GoLand and other JetBrains IDEs can be configured with a File Watcher that runs `buf lint --path` on save and optionally surface issues as warnings or errors in your editor.

See this [doc](https://docs.buf.build/editor-integration#jetbrains-ides) for more details.
