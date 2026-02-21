package main

import (
	"context"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/ardanlabs/conf/v3"
	"github.com/oxidecomputer/omni-infra-provider-oxide/internal/provider"
	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/client"
	"github.com/siderolabs/omni/client/pkg/infra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Version represents the version of the binary. It can be overridden at
// compile-time as necessary.
var version = "dev"

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalf("%s", err.Error())
	}
}

func run(ctx context.Context) error {
	logger, err := zap.NewProductionConfig().Build(
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return fmt.Errorf("failed creating logger: %w", err)
	}

	cfg := Config{
		Version: conf.Version{
			Build: version,
			Desc:  "Oxide Omni infrastructure provider.",
		},
	}

	s, err := conf.Parse("", &cfg)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			fmt.Fprintf(os.Stdout, "%s", s)
			return nil
		}
		return fmt.Errorf("failed parsing configuration: %w", err)
	}

	configStr, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("failed stringifying configuration: %w", err)
	}

	logger.Info("successfully parsed configuration", zap.Any("configuration", configStr))

	oxideClient, err := oxide.NewClient(
		oxide.WithHost(cfg.OxideHost),
		oxide.WithToken(cfg.OxideToken),
	)
	if err != nil {
		return fmt.Errorf("failed creating oxide client: %w", err)
	}

	if len(cfg.Nameservers) == 0 {
		cfg.Nameservers = []string{"9.9.9.9", "8.8.8.8"}
	}

	provisioner := provider.NewProvisioner(
		oxideClient,
		slices.Clone(cfg.Nameservers),
	)

	// Explicitly set the provider ID. See [provider.ID] for more information.
	provider.ID = cfg.ProviderID

	provider, err := infra.NewProvider(provider.ID, provisioner, infra.ProviderConfig{
		Name:        cfg.ProviderName,
		Description: cfg.ProviderDescription,
		Icon:        base64.StdEncoding.EncodeToString(provider.Icon),
		Schema:      provider.MachineClassSchema,
	})
	if err != nil {
		return fmt.Errorf("failed creating infrastructure provider: %w", err)
	}

	opts := []infra.Option{
		infra.WithOmniEndpoint(cfg.OmniEndpoint),
		infra.WithClientOptions(
			client.WithServiceAccount(cfg.OmniServiceAccountKey),
		),
		infra.WithEncodeRequestIDsIntoTokens(),
		infra.WithConcurrency(cfg.ProvisionerConcurrency),
		infra.WithHealthCheckFunc(func(ctx context.Context) error {
			if _, err := oxideClient.CurrentUserView(ctx); err != nil {
				return fmt.Errorf("failed validating oxide api connection: %w", err)
			}

			return nil
		}),
	}

	logger.Info("starting...")

	if err := provider.Run(ctx, logger, opts...); err != nil {
		return fmt.Errorf("failed to run infrastructure provider: %w", err)
	}

	return nil
}
