package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-uuid"
	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/infra/provision"
	"github.com/siderolabs/omni/client/pkg/omni/resources/infra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ provision.Provisioner[*Instance] = (*Provisioner)(nil)

// TODO: What other fields are needed?
type Provisioner struct {
	oxideClient *oxide.Client
}

func NewProvisioner(oxideClient *oxide.Client) *Provisioner {
	return &Provisioner{
		oxideClient: oxideClient,
	}
}

// TODO: Complete steps.
func (p *Provisioner) ProvisionSteps() []provision.Step[*Instance] {
	return []provision.Step[*Instance]{
		provision.NewStep(
			"instance_create",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Instance]) error {
				logger.Info("instance_create")

				if pctx.State.TypedSpec().Value.Uuid == "" {
					uuidStr, _ := uuid.GenerateUUID()
					pctx.State.TypedSpec().Value.Uuid = uuidStr
				}
				instanceUUID := pctx.State.TypedSpec().Value.Uuid


				if pctx.State.TypedSpec().Value.Id != "" {
					instanceID := pctx.State.TypedSpec().Value.Id
					if _, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
						Instance: oxide.NameOrId(instanceID),
					}); err != nil {
						return fmt.Errorf("failed to view existing instance: %w", err)
					}

					logger.Info("instance exists", zapcore.Field{
						Key:       "instance_id",
						Type:      zapcore.StringType,
						String:    instanceID,
					})
					return nil
				}
				
				oxideInstance, err := p.oxideClient.InstanceCreate(ctx, oxide.InstanceCreateParams{
					Project: "matthewsanabria",
					Body: &oxide.InstanceCreate{
						AntiAffinityGroups: []oxide.NameOrId{},
						AutoRestartPolicy:  "",
						BootDisk: &oxide.InstanceDiskAttachment{
							Description: "Boot disk.",
							DiskSource: oxide.DiskSource{
								BlockSize: 512,
								Type:      oxide.DiskSourceTypeImage,
								ImageId:   "f8f4b464-d510-418a-9a86-aacd825ded9f", // Talos Linux v1.11.1.
							},
							Name: oxide.Name(fmt.Sprintf("talos-%s", instanceUUID)),
							Size: 53687091200,
							Type: oxide.InstanceDiskAttachmentTypeCreate,
						},
						Description: "Talos Linux.",
						Disks:       []oxide.InstanceDiskAttachment{},
						ExternalIps: []oxide.ExternalIpCreate{},
						Hostname:    oxide.Hostname(fmt.Sprintf("talos-%s", instanceUUID)),
						Memory:      17179869184,
						Name:        oxide.Name(fmt.Sprintf("talos-%s", instanceUUID)),
						Ncpus:       4,
						NetworkInterfaces: oxide.InstanceNetworkInterfaceAttachment{
							Params: []oxide.InstanceNetworkInterfaceCreate{
								{
									Description: "Talos NIC.",
									Name:        oxide.Name(fmt.Sprintf("talos-%s", instanceUUID)),
									SubnetName:  "default",
									VpcName:     "default",
								},
							},
							Type: oxide.InstanceNetworkInterfaceAttachmentTypeCreate,
						},
						SshPublicKeys: []oxide.NameOrId{},
						Start:         oxide.NewPointer(true),
						UserData:      "",
					},
				})
				if err != nil {
					return fmt.Errorf("failed creating oxide instance: %w", err)
				}

				pctx.State.TypedSpec().Value.Id = oxideInstance.Id

				logger.Info("created instance", zapcore.Field{
					Key:    "instance_id",
					Type:   zapcore.StringType,
					String: oxideInstance.Id,
				})

				return nil
			},
		),
	}
}

// TODO: Implement.
func (p *Provisioner) Deprovision(
	ctx context.Context,
	logger *zap.Logger,
	resource *Instance,
	req *infra.MachineRequest,
) error {
	logger.Info("deprovision")
	return nil
}
