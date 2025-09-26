package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os/exec"
	"strings"

	"github.com/hashicorp/go-uuid"
	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/constants"
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
			"create_schematic",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Instance]) error {
				pctx.ConnectionParams.CustomDataEncoded = false
				schematicID, err := pctx.GenerateSchematicID(ctx, logger,
					provision.WithExtraKernelArgs(
						"console=ttyS0",
					),
					// provision.WithoutConnectionParams(),
					provision.WithExtraExtensions(
						"siderolabs/amd-ucode",
						"siderolabs/iscsi-tools",
						"siderolabs/util-linux-tools",
					),
				)
				if err != nil {
					return fmt.Errorf("failed generating schematic id: %w", err)
				}

				pctx.State.TypedSpec().Value.SchematicId = schematicID

				logger.Info("schematic information",
					zap.Any("connection_params", pctx.ConnectionParams),
				)

				return nil
			},
		),
		provision.NewStep(
			"create_image",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Instance]) error {
				pctx.State.TypedSpec().Value.TalosVersion = pctx.GetTalosVersion()

				imageURL, err := url.Parse(constants.ImageFactoryBaseURL)
				if err != nil {
					return fmt.Errorf("failed to parse image url: %w", err)
				}

				imageURL = imageURL.JoinPath(
					"image",
					pctx.State.TypedSpec().Value.SchematicId,
					pctx.GetTalosVersion(),
					"metal-amd64.raw.xz",
				)

				hash := sha256.New()
				if _, err := hash.Write([]byte(imageURL.String())); err != nil {
					return fmt.Errorf("failed to write to hash: %w", err)
				}
				schematicHash := hex.EncodeToString(hash.Sum(nil))[:8]

				imageName := fmt.Sprintf("omni-v%s-%s-metal-amd64",
					strings.ReplaceAll(pctx.GetTalosVersion(), ".", "-"),
					schematicHash,
				)

				logger.Info("image information",
					zap.String("image_url", imageURL.String()),
					zap.String("schematic_hash", schematicHash),
					zap.String("schematic_id", pctx.State.TypedSpec().Value.SchematicId),
					zap.String("talos_version", pctx.GetTalosVersion()),
					zap.String("image_name", imageName),
				)

				pctx.State.TypedSpec().Value.ImageName = imageName

				image, err := p.oxideClient.ImageView(ctx, oxide.ImageViewParams{
					Image:   oxide.NameOrId(imageName),
					Project: "matthewsanabria",
				})
				if err != nil {
					if !strings.Contains(err.Error(), "404") {
						return fmt.Errorf("failed viewing image: %w", err)
					}
				}

				if image != nil {
					pctx.State.TypedSpec().Value.ImageId = image.Id
					logger.Info("image exists",
						zap.Any("image", image),
					)
					return nil
				}

				cmd := exec.Command("./upload-image.sh", imageURL.String(), imageName)
				if err := cmd.Run(); err != nil {
					return fmt.Errorf("failed uploading image: %w", err)
				}

				image, err = p.oxideClient.ImageView(ctx, oxide.ImageViewParams{
					Image:   oxide.NameOrId(imageName),
					Project: "matthewsanabria",
				})
				if err != nil {
					return fmt.Errorf("failed viewing image again: %w", err)
				}
				pctx.State.TypedSpec().Value.ImageId = image.Id

				return nil
			},
		),
		provision.NewStep(
			"instance_create",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Instance]) error {
				logger.Info("instance_create")

				if pctx.State.TypedSpec().Value.Uuid == "" {
					uuidStr, _ := uuid.GenerateUUID()
					pctx.State.TypedSpec().Value.Uuid = uuidStr
				}
				// instanceUUID := pctx.State.TypedSpec().Value.Uuid

				if pctx.State.TypedSpec().Value.Id != "" {
					instanceID := pctx.State.TypedSpec().Value.Id
					if _, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
						Instance: oxide.NameOrId(instanceID),
					}); err != nil {
						return fmt.Errorf("failed to view existing instance: %w", err)
					}

					logger.Info("instance exists", zapcore.Field{
						Key:    "instance_id",
						Type:   zapcore.StringType,
						String: instanceID,
					})
					return nil
				}

				// 		userdata := fmt.Sprintf(`datasource:
				// NoCloud:
				//   user-data: |
				//     %s
				//   network-data:
				//     version: 1`,
				// 			pctx.ConnectionParams.JoinConfig,
				// 		)

				// 		logger.Info("about to create instance",
				// 			zap.String("userdata", base64.RawStdEncoding.EncodeToString([]byte(userdata))),
				// 		)

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
								ImageId:   pctx.State.TypedSpec().Value.ImageId,
							},
							Name: oxide.Name(pctx.GetRequestID()),
							Size: 53687091200,
							Type: oxide.InstanceDiskAttachmentTypeCreate,
						},
						Description: "Talos Linux.",
						Disks:       []oxide.InstanceDiskAttachment{},
						ExternalIps: []oxide.ExternalIpCreate{},
						Hostname:    oxide.Hostname(pctx.GetRequestID()),
						Memory:      17179869184,
						Name:        oxide.Name(pctx.GetRequestID()),
						Ncpus:       4,
						NetworkInterfaces: oxide.InstanceNetworkInterfaceAttachment{
							Params: []oxide.InstanceNetworkInterfaceCreate{
								{
									Description: "Talos NIC.",
									Name:        oxide.Name(pctx.GetRequestID()),
									SubnetName:  "default",
									VpcName:     "default",
								},
							},
							Type: oxide.InstanceNetworkInterfaceAttachmentTypeCreate,
						},
						SshPublicKeys: []oxide.NameOrId{},
						Start:         oxide.NewPointer(true),
						UserData:      "",
						// UserData:      base64.RawStdEncoding.EncodeToString([]byte(userdata)),
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
