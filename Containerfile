ARG GO_CONTAINER_IMAGE

# Stage 1: Build the binary.
FROM ${GO_CONTAINER_IMAGE} AS builder

ARG VERSION

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build \
  -ldflags "-s -w -X main.version=${VERSION}" \
  .

# Stage 2: Minimal container image.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /src/omni-infra-provider-oxide /usr/bin/omni-infra-provider-oxide

ENTRYPOINT ["omni-infra-provider-oxide"]
