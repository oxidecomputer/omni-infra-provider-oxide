package provider

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/infra/provision"
	"github.com/siderolabs/omni/client/pkg/omni/resources/infra"
	"go.uber.org/zap"
)

//go:embed assets/user-data.tmpl
var userdataTemplate string

// Ensure [Provisioner] implements the [provision.Provisioner] interface.
var _ provision.Provisioner[*Machine] = (*Provisioner)(nil)

// Provisioner implements the [provision.Provisioner] interface to provision and
// deprovision machines on Oxide.
type Provisioner struct {
	oxideClient *oxide.Client
}

// NewProvisioner builds and returns a new [Provisioner].
func NewProvisioner(oxideClient *oxide.Client) *Provisioner {
	return &Provisioner{
		oxideClient: oxideClient,
	}
}

// ProvisionSteps returns the steps necessary to provision a
// [Machine].
func (p *Provisioner) ProvisionSteps() []provision.Step[*Machine] {
	return []provision.Step[*Machine]{
		provision.NewStep("generate_schematic_id", p.generateSchematicID),
		provision.NewStep("generate_image_factory_url", p.generateImageFactoryURL),
		provision.NewStep("generate_image_name", p.generateImageName),
		provision.NewStep("fetch_image_id", p.fetchImageID),
		provision.NewStep("create_image", p.createImage),
		provision.NewStep("instance_create", p.createInstance),
		provision.NewStep("config_patch_provider_id", p.configPatchProviderID),
	}
}

// Deprovision destroys the Oxide machine that was created during
// [Provisioner.ProvisionSteps].
func (p *Provisioner) Deprovision(
	ctx context.Context,
	logger *zap.Logger,
	machine *Machine,
	req *infra.MachineRequest,
) error {
	if machine == nil ||
		machine.TypedSpec().Value.InstanceId == "" {
		logger.Warn("deprovision called without machine details",
			zap.Any("machine", machine),
		)
		return nil
	}

	instance, err := p.oxideClient.InstanceView(
		ctx, oxide.InstanceViewParams{
			Instance: oxide.NameOrId(
				machine.TypedSpec().Value.InstanceId,
			),
		},
	)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			logger.Info("instance not found, already deleted",
				zap.String("oxide.instance.id",
					machine.TypedSpec().Value.InstanceId,
				),
			)
			return nil
		}
		return fmt.Errorf(
			"failed viewing oxide instance: %w", err,
		)
	}

	if _, err := p.oxideClient.InstanceStop(
		ctx, oxide.InstanceStopParams{
			Instance: oxide.NameOrId(instance.Id),
		},
	); err != nil {
		return fmt.Errorf(
			"failed to stop oxide instance: %w", err,
		)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	for {
		select {
		case <-stopCtx.Done():
			return fmt.Errorf(
				"timed out waiting for instance to stop: %w",
				ctx.Err(),
			)
		default:
		}

		instance, err := p.oxideClient.InstanceView(
			stopCtx, oxide.InstanceViewParams{
				Instance: oxide.NameOrId(instance.Id),
			},
		)
		if err != nil {
			return fmt.Errorf(
				"failed refreshing oxide instance state: %w",
				err,
			)
		}

		if instance.RunState == oxide.InstanceStateStopped {
			break
		}

		logger.Info("waiting for instance to stop",
			zap.String("oxide.instance.id", instance.Id),
			zap.String("oxide.instance.run_state",
				string(instance.RunState),
			),
		)

		time.Sleep(3 * time.Second)
	}

	if err := p.oxideClient.InstanceDelete(
		ctx, oxide.InstanceDeleteParams{
			Instance: oxide.NameOrId(instance.Id),
		},
	); err != nil {
		return fmt.Errorf(
			"failed deleting oxide instance: %w", err,
		)
	}

	logger.Info("deleted instance",
		zap.String("oxide.instance.id", instance.Id),
	)

	if instance.BootDiskId != "" {
		if err := p.oxideClient.DiskDelete(
			ctx, oxide.DiskDeleteParams{
				Disk: oxide.NameOrId(instance.BootDiskId),
			},
		); err != nil {
			return fmt.Errorf(
				"failed deleting boot disk: %w", err,
			)
		}

		logger.Info("deleted boot disk",
			zap.String("oxide.instance.boot_disk_id",
				instance.BootDiskId,
			),
		)
	}

	return nil
}
