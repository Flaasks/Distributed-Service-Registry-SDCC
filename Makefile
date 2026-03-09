GO ?= go
PROTO_DIR := proto
PROTO_OUT := pkg/api
PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto')
TOOLS_DIR := .tools
PROTOC_VERSION ?= 27.2
PROTOC_ARCHIVE := protoc-$(PROTOC_VERSION)-linux-x86_64.zip
PROTOC_LOCAL_DIR := $(TOOLS_DIR)/protoc
PROTOC_LOCAL_BIN := $(PROTOC_LOCAL_DIR)/bin/protoc
GOBIN := $(shell $(GO) env GOPATH)/bin
PROTOC_GEN_GO := $(GOBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(GOBIN)/protoc-gen-go-grpc

ifeq ($(shell command -v protoc >/dev/null 2>&1 && echo yes),yes)
PROTOC_CMD := protoc
else ifneq ($(wildcard $(PROTOC_LOCAL_BIN)),)
PROTOC_CMD := $(PROTOC_LOCAL_BIN)
else
PROTOC_CMD :=
endif

.PHONY: tools protoc-local proto tidy build test

tools:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

protoc-local:
	mkdir -p $(TOOLS_DIR)
	curl -fsSL -o $(TOOLS_DIR)/$(PROTOC_ARCHIVE) https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/$(PROTOC_ARCHIVE)
	rm -rf $(PROTOC_LOCAL_DIR)
	unzip -q -o $(TOOLS_DIR)/$(PROTOC_ARCHIVE) -d $(PROTOC_LOCAL_DIR)

proto:
	@test -n "$(PROTOC_CMD)" || { echo "protoc not found. Install it system-wide or run 'make protoc-local'."; exit 1; }
	@test -x "$(PROTOC_GEN_GO)" || { echo "protoc-gen-go not found. Run 'make tools'."; exit 1; }
	@test -x "$(PROTOC_GEN_GO_GRPC)" || { echo "protoc-gen-go-grpc not found. Run 'make tools'."; exit 1; }
	mkdir -p $(PROTO_OUT)
	PATH="$(GOBIN):$$PATH" $(PROTOC_CMD) -I=$(PROTO_DIR) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

tidy:
	$(GO) mod tidy

build:
	$(GO) build ./...

test:
	$(GO) test ./...
