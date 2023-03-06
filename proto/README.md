# Vanus Proto

Protobuf files for Vanus

## How to use

### Setup IDE

1. install plugin 'Proto Buffer' in marketplace
2. config path: Performance -> Language&Framework -> Proto Buffers -> Import paths;
   and add `include` and `proto` directory to

### install `protogen`

```bash
# 1. Install with Homebrew
brew install protobuf

# or download https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protoc-3.19.4-osx-x86_64.zip
unzip protoc-3.19.4-osx-x86_64.zip
mv bin/protoc $GOPATH/bin/ 

# 2. Install protoc-gen-go
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

### debug

- use [grpcurl](https://github.com/fullstorydev/grpcurl)
- use [Postman](https://www.postman.com/)

## Makefile

The makefile has pre-define some convenient methods to generate go file from proto, see file content for details.

```bash
# set package, such as controller, then generate code
package=controller make generate-pb
```

## Use Buf

### Install by Homebrew on macOS

```bash
brew install bufbuild/buf/buf
```

### Generate code

```bash
cd proto/proto

buf generate
```

### Editor integration

#### Visual Studio Code

The Visual Studio Code extension can be downloaded from the in-editor extension browser under the name "Buf" or manually via [the extension page](https://marketplace.visualstudio.com/items?itemName=bufbuild.vscode-buf). You need to have buf [installed](https://docs.buf.build/installation) to use it.

#### JetBrain IDEs

IntelliJ IDEA, GoLand and other JetBrains IDEs can be configured with a File Watcher that runs `buf lint --path` on save and optionally surface issues as warnings or errors in your editor.

See this [doc](https://docs.buf.build/editor-integration#jetbrains-ides) for more details.
