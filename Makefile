VERSION ?= v0.3.0
GO_CONTAINER_IMAGE ?= docker.io/golang:1.25.5

# Set this to non-empty when building and pushing a release.
RELEASE ?=

CONTAINER_RUNTIME ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
ifeq ($(CONTAINER_RUNTIME),)
$(error No container runtime found. Please install podman or docker)
endif

# Detect if using Docker or Podman.
IS_DOCKER := $(findstring docker,$(CONTAINER_RUNTIME))
IS_PODMAN := $(findstring podman,$(CONTAINER_RUNTIME))

GIT_COMMIT_SHORT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Container image configuration.
IMAGE_REGISTRY ?= ghcr.io/oxidecomputer
IMAGE_NAME ?= omni-infra-provider-oxide
IMAGE_TAG ?= $(patsubst v%,%,$(VERSION))$(if $(RELEASE),,-$(GIT_COMMIT_SHORT))
IMAGE_FULL ?= $(if $(IMAGE_REGISTRY),$(IMAGE_REGISTRY)/)$(IMAGE_NAME):$(IMAGE_TAG)

# Multi-arch build configuration for releases.
# Docker uses buildx with comma-separated platforms; Podman uses repeated
# --platform flags.
ifdef RELEASE
  ifdef IS_DOCKER
    BUILD_COMMAND := docker buildx build
    BUILD_PLATFORMS := --platform linux/amd64,linux/arm64
    BUILD_OUTPUT := --tag $(IMAGE_FULL)
  else
    BUILD_COMMAND := $(CONTAINER_RUNTIME) build
    BUILD_PLATFORMS := --platform linux/amd64 --platform linux/arm64
    BUILD_OUTPUT := --manifest $(IMAGE_FULL)
  endif
else
  BUILD_COMMAND := $(CONTAINER_RUNTIME) build
  BUILD_PLATFORMS :=
  BUILD_OUTPUT := --tag $(IMAGE_FULL)
endif

.PHONY: test
test:
	@echo "Running tests in container..."
	$(CONTAINER_RUNTIME) build \
		--file Containerfile \
		--build-arg GO_CONTAINER_IMAGE=$(GO_CONTAINER_IMAGE) \
		--build-arg VERSION=$(VERSION) \
		--target builder \
		--tag $(if $(IMAGE_REGISTRY),$(IMAGE_REGISTRY)/)$(IMAGE_NAME)-builder:$(IMAGE_TAG) \
		.
	$(CONTAINER_RUNTIME) run --rm $(IMAGE_NAME)-builder:$(IMAGE_TAG) go test -v ./...

.PHONY: build
build:
	@echo "Building container image: $(IMAGE_FULL)"
ifdef RELEASE
ifdef IS_PODMAN
	$(CONTAINER_RUNTIME) manifest rm $(IMAGE_FULL) 2>/dev/null || true
	$(CONTAINER_RUNTIME) rmi $(IMAGE_FULL) 2>/dev/null || true
endif
endif
	$(BUILD_COMMAND) \
		--file Containerfile \
		--build-arg GO_CONTAINER_IMAGE=$(GO_CONTAINER_IMAGE) \
		--build-arg VERSION=$(VERSION) \
		$(BUILD_PLATFORMS) \
		--annotation org.opencontainers.image.description='Oxide Omni Infrastructure Provider' \
		--annotation org.opencontainers.image.source=https://github.com/oxidecomputer/omni-infra-provider-oxide \
		$(BUILD_OUTPUT) \
		.

.PHONY: push
push:
	@echo "Pushing container image: $(IMAGE_FULL)"
ifdef RELEASE
ifdef IS_PODMAN
	$(CONTAINER_RUNTIME) manifest push $(IMAGE_FULL) $(IMAGE_FULL)
else
	$(CONTAINER_RUNTIME) push $(IMAGE_FULL)
endif
else
	$(CONTAINER_RUNTIME) push $(IMAGE_FULL)
endif

.PHONY: dev
dev: build
	$(CONTAINER_RUNTIME) run --rm \
		--env OMNI_ENDPOINT \
		--env OMNI_SERVICE_ACCOUNT_KEY \
		--env OXIDE_HOST \
		--env OXIDE_TOKEN \
		$(IMAGE_FULL)

.PHONY: generate
generate:
	@go tool -modfile tools/go.mod buf generate

.PHONY: lint
lint:
	@go tool -modfile tools/go.mod golangci-lint run

.PHONY: fmt
fmt:
	@go tool -modfile tools/go.mod golangci-lint fmt
