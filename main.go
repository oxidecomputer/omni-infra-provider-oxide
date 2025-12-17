package main

import (
	"context"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"github.com/ardanlabs/conf/v3"
	"github.com/oxidecomputer/omni-infra-provider-oxide/internal/provider"
	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/client"
	"github.com/siderolabs/omni/client/pkg/infra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var version = "dev"

type Config struct {
	conf.Version
	OxideHost              string `conf:"required,notzero,help:Oxide API host (e.g.; https://oxide.sys.example.com)."`
	OxideToken             string `conf:"required,notzero,mask,help:Oxide API token."`
	ProviderID             string `conf:"default:oxide,help:Provider ID to use within Omni."`
	ProviderName           string `conf:"default:Oxide,help:Provider name to use within Omni."`
	ProviderDescription    string `conf:"default:Oxide Omni infrastructure provider.,help:Provider description to use within Omni."`
	ProvisionerConcurrency uint   `conf:"default:1,help:Number of concurrent provisioner operations."`
	OmniEndpoint           string `conf:"required,notzero,help:Omni API endpoint (e.g.; https://omni.example.com)."`
	OmniServiceAccountKey  string `conf:"required,notzero,mask,help:Omni service account key."`
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err.Error())
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

	oxideClient, err := oxide.NewClient(&oxide.Config{
		Host:  cfg.OxideHost,
		Token: cfg.OxideToken,
	})
	if err != nil {
		return fmt.Errorf("failed creating oxide client: %w", err)
	}

	provisioner := provider.NewProvisioner(oxideClient)

	// Explicitly set the provider ID. See [provider.ID] for more information.
	provider.ID = cfg.ProviderID

	provider, err := infra.NewProvider(provider.ID, provisioner, infra.ProviderConfig{
		Name:        cfg.ProviderName,
		Description: cfg.ProviderDescription,
		Icon:        base64.StdEncoding.EncodeToString(provider.Icon),
		Schema:      provider.MachineClassSchema,
	})

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
