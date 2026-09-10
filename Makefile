BINARY := omni-infra-provider-oxide
BUILD_DIR ?= bin
GO ?= go

.DEFAULT_GOAL := build

.PHONY: build
build:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 $(GO) build -buildvcs=true -trimpath \
		-o $(BUILD_DIR)/$(BINARY) .

.PHONY: test
test:
	$(GO) test -v ./...

.PHONY: dev
dev: build
	$(BUILD_DIR)/$(BINARY)

.PHONY: generate
generate:
	@protoc --go_out=. --go_opt=paths=source_relative \
		internal/provider/spec/machine.proto

.PHONY: lint
lint:
	@golangci-lint run

.PHONY: fmt
fmt:
	@golangci-lint fmt

.PHONY: check
check: test lint

.PHONY: snapshot
snapshot:
	goreleaser release --snapshot --clean

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR) dist
