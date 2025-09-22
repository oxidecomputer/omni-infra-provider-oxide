package main

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"log"
	"os"

	"github.com/oxidecomputer/omni-infra-provider-oxide/internal/provider"
	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/client"
	"github.com/siderolabs/omni/client/pkg/infra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//go:embed oxide.svg
var icon []byte

//go:embed schema.json
var schema string

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalln(err)
	}
}

func run(ctx context.Context) error {
	logger, err := zap.NewProductionConfig().Build(
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	oxideClient, err := oxide.NewClient(nil)
	if err != nil {
		return fmt.Errorf("failed to create oxide client: %w", err)
	}

	provisioner := provider.NewProvisioner(oxideClient)

	provider, err := infra.NewProvider(provider.ID, provisioner, infra.ProviderConfig{
		Name:        "Oxide",
		Description: "Oxide infrastructure provider.",
		Icon:        base64.RawStdEncoding.EncodeToString(icon),
		Schema:      schema,
	})

	opts := []infra.Option{
		infra.WithOmniEndpoint(os.Getenv("OMNI_ENDPOINT")),
		infra.WithClientOptions(
			client.WithServiceAccount(os.Getenv("OMNI_SERVICE_ACCOUNT_KEY")),
		),
		infra.WithEncodeRequestIDsIntoTokens(),
	}

	logger.Info("starting...")

	if err := provider.Run(ctx, logger, opts...); err != nil {
		return fmt.Errorf("failed to run infrastructure provider: %w", err)
	}

	return nil
}
