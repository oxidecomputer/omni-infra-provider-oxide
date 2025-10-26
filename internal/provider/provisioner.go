package provider

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/constants"
	"github.com/siderolabs/omni/client/pkg/infra/provision"
	"github.com/siderolabs/omni/client/pkg/omni/resources/infra"
	"github.com/ulikunitz/xz"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
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

// ProvisionSteps returns the steps necessary to provision a [Machine].
func (p *Provisioner) ProvisionSteps() []provision.Step[*Machine] {
	return []provision.Step[*Machine]{
		provision.NewStep(
			"generate_schematic_id",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				schematicID, err := pctx.GenerateSchematicID(ctx, logger,
					provision.WithExtraKernelArgs(
						"-console",
						"console=ttyS0",
					),
					provision.WithExtraExtensions(
						"siderolabs/amd-ucode",
						"siderolabs/iscsi-tools",
						"siderolabs/util-linux-tools",
					),
					// Connection parameters must not be present in kernel arguments since this
					// infrastructure provider uses NoCloud images and passes the SideroLink
					// configuration via cloud-init instead.
					provision.WithoutConnectionParams(),
				)
				if err != nil {
					return fmt.Errorf("failed generating schematic id: %w", err)
				}

				logger.Info("generated schematic id",
					zap.String("talos.schematic_id", schematicID),
				)

				pctx.State.TypedSpec().Value.TalosSchematicId = schematicID

				return nil
			},
		),
		provision.NewStep(
			"generate_image_factory_url",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				imageFactoryURL, err := url.Parse(constants.ImageFactoryBaseURL)
				if err != nil {
					return fmt.Errorf("failed to parse image factory base url: %w", err)
				}

				imageFactoryURL = imageFactoryURL.JoinPath(
					"image",
					pctx.State.TypedSpec().Value.TalosSchematicId,
					pctx.GetTalosVersion(),
					"nocloud-amd64.raw.xz",
				)

				logger.Info("generated talos image url",
					zap.String("talos.image_url", imageFactoryURL.String()),
				)

				pctx.State.TypedSpec().Value.TalosImageUrl = imageFactoryURL.String()

				return nil
			},
		),
		provision.NewStep(
			"generate_image_name",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				talosImageUrl := pctx.State.TypedSpec().Value.TalosImageUrl

				hash := sha256.New()
				if _, err := hash.Write([]byte(talosImageUrl)); err != nil {
					return fmt.Errorf("failed to write to hash: %w", err)
				}
				schematicHash := hex.EncodeToString(hash.Sum(nil))[:8]

				imageName := fmt.Sprintf("talos-%s-%s-nocloud",
					strings.ReplaceAll(pctx.GetTalosVersion(), ".", "-"),
					schematicHash,
				)

				logger.Info("generated oxide image name",
					zap.String("oxide.image_name", imageName),
				)

				pctx.State.TypedSpec().Value.ImageName = imageName

				return nil
			},
		),
		provision.NewStep(
			"fetch_image_id",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				imageName := pctx.State.TypedSpec().Value.ImageName

				var machineClass MachineClass
				if err := pctx.UnmarshalProviderData(&machineClass); err != nil {
					return fmt.Errorf("failed unmarshaling provider data: %w", err)
				}

				image, err := p.oxideClient.ImageView(ctx, oxide.ImageViewParams{
					Image:   oxide.NameOrId(imageName),
					Project: oxide.NameOrId(machineClass.Project),
				})
				if err != nil {
					if strings.Contains(err.Error(), "404") {
						logger.Info("oxide image does not exist",
							zap.String("oxide.image_name", imageName),
						)
						return nil
					}

					return fmt.Errorf("failed viewing oxide image: %w", err)
				}

				logger.Info("fetched oxide image information",
					zap.String("oxide.image_id", image.Id),
					zap.String("oxide.image_name", imageName),
				)

				pctx.State.TypedSpec().Value.ImageId = image.Id

				return nil
			},
		),
		provision.NewStep(
			"create_image",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				if pctx.State.TypedSpec().Value.ImageId != "" {
					return nil
				}

				var machineClass MachineClass
				if err := pctx.UnmarshalProviderData(&machineClass); err != nil {
					return fmt.Errorf("failed unmarshaling provider data: %w", err)
				}

				imageURL := pctx.State.TypedSpec().Value.TalosImageUrl
				imageName := pctx.State.TypedSpec().Value.ImageName

				resp, err := http.Get(imageURL)
				if err != nil {
					return fmt.Errorf("failed downloading image: %w", err)
				}
				defer resp.Body.Close()

				xzReader, err := xz.NewReader(resp.Body)
				if err != nil {
					return fmt.Errorf("failed creating xz reader: %w", err)
				}

				f, err := os.CreateTemp("", imageName)
				if err != nil {
					return fmt.Errorf("failed creating decompressed temp file: %w", err)
				}
				defer f.Close()
				defer os.RemoveAll(f.Name())

				if _, err := io.Copy(f, xzReader); err != nil {
					return fmt.Errorf("failed decompressing image: %w", err)
				}

				if err := f.Sync(); err != nil {
					return fmt.Errorf("failed syncing decompressed file: %w", err)
				}

				if _, err := f.Seek(0, 0); err != nil {
					return fmt.Errorf("failed seeking to beginning of file: %w", err)
				}

				fi, err := f.Stat()
				if err != nil {
					return fmt.Errorf("failed retrieving file information: %w", err)
				}

				disk, err := p.oxideClient.DiskCreate(ctx, oxide.DiskCreateParams{
					Project: oxide.NameOrId(machineClass.Project),
					Body: &oxide.DiskCreate{
						Description: fmt.Sprintf("Temporary disk for Oxide infrastructure provider (%s).", pctx.GetRequestID()),
						DiskSource: oxide.DiskSource{
							BlockSize: 512,
							Type:      oxide.DiskSourceTypeImportingBlocks,
						},
						Name: oxide.Name(imageName),
						Size: oxide.ByteCount((fi.Size() + 1024*1024*1024 - 1) / (1024 * 1024 * 1024) * (1024 * 1024 * 1024)),
					},
				})
				if err != nil {
					return fmt.Errorf("failed creating disk: %w", err)
				}
				defer func() {
					p.oxideClient.DiskDelete(ctx, oxide.DiskDeleteParams{
						Disk: oxide.NameOrId(disk.Id),
					})
				}()

				if err := p.oxideClient.DiskBulkWriteImportStart(ctx, oxide.DiskBulkWriteImportStartParams{
					Disk: oxide.NameOrId(disk.Id),
				}); err != nil {
					return fmt.Errorf("failed starting bulk write import: %w", err)
				}

				// TODO: Refactor the bulk import code below.

				type chunk struct {
					offset int64
					data   []byte
				}

				buf := make([]byte, 512*1024)
				ch := make(chan chunk, 64)
				var offset int64

				var wg sync.WaitGroup
				var workerErr error
				var errMu sync.Mutex

				for worker := range 8 {
					wg.Add(1)
					go func(worker int) {
						defer wg.Done()
						for c := range ch {
							if err := p.oxideClient.DiskBulkWriteImport(ctx, oxide.DiskBulkWriteImportParams{
								Disk: oxide.NameOrId(disk.Id),
								Body: &oxide.ImportBlocksBulkWrite{
									Base64EncodedData: base64.StdEncoding.EncodeToString(c.data),
									Offset:            oxide.NewPointer(int(c.offset)),
								},
							}); err != nil {
								errMu.Lock()
								if workerErr == nil {
									workerErr = err
								}
								errMu.Unlock()
								logger.Error("failed uploading chunk",
									zap.String("error", err.Error()),
								)
								return
							}
						}
					}(worker)
				}

				ticker := time.NewTicker(3 * time.Second)
				defer ticker.Stop()

				for {
					n, err := f.Read(buf)
					if err != nil {
						if err == io.EOF {
							break
						}
						return fmt.Errorf("failed reading chunk: %w", err)
					}

					if n == 0 {
						break
					}

					allZeros := true
					for _, b := range buf[:n] {
						if b != 0 {
							allZeros = false
							break
						}
					}

					if !allZeros {
						data := make([]byte, n)
						copy(data, buf[:n])
						ch <- chunk{
							offset: offset,
							data:   data,
						}
					}
					offset += int64(n)

					select {
					case <-ticker.C:
						logger.Info("bulk writing to oxide disk",
							zap.String("disk", string(disk.Name)),
							zap.Int64("offset", offset),
						)
					default:
					}
				}
				close(ch)
				wg.Wait()

				if workerErr != nil {
					return fmt.Errorf("worker failed uploading chunks: %w", workerErr)
				}

				if err := p.oxideClient.DiskBulkWriteImportStop(ctx, oxide.DiskBulkWriteImportStopParams{
					Disk: oxide.NameOrId(disk.Id),
				}); err != nil {
					return fmt.Errorf("failed stopping bulk write import: %w", err)
				}

				if err := p.oxideClient.DiskFinalizeImport(ctx, oxide.DiskFinalizeImportParams{
					Disk: oxide.NameOrId(disk.Id),
					Body: &oxide.FinalizeDisk{
						SnapshotName: oxide.Name(imageName),
					},
				}); err != nil {
					return fmt.Errorf("failed finalizing disk import: %w", err)
				}

				snapshot, err := p.oxideClient.SnapshotView(ctx, oxide.SnapshotViewParams{
					Snapshot: oxide.NameOrId(imageName),
					Project:  oxide.NameOrId(machineClass.Project),
				})
				if err != nil {
					return fmt.Errorf("failed creating snapshot: %w", err)
				}
				defer func() {
					p.oxideClient.SnapshotDelete(ctx, oxide.SnapshotDeleteParams{
						Snapshot: oxide.NameOrId(snapshot.Id),
					})
				}()

				image, err := p.oxideClient.ImageCreate(ctx, oxide.ImageCreateParams{
					Project: oxide.NameOrId(machineClass.Project),
					Body: &oxide.ImageCreate{
						Description: fmt.Sprintf("Talos Linux v%s NoCloud (%s).", pctx.GetTalosVersion()),
						Name:        oxide.Name(imageName),
						Os:          "Talos Linux",
						Source: oxide.ImageSource{
							Id:   snapshot.Id,
							Type: oxide.ImageSourceTypeSnapshot,
						},
						Version: pctx.GetTalosVersion(),
					},
				})
				if err != nil {
					return fmt.Errorf("failed creating image: %w", err)
				}

				pctx.State.TypedSpec().Value.ImageId = image.Id

				return nil
			},
		),
		provision.NewStep(
			"instance_create",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				instanceID := pctx.State.TypedSpec().Value.InstanceId

				logger = logger.With(zap.String("omni.request_id", pctx.GetRequestID()))

				if instanceID != "" {
					logger.Info("confirming whether instance exists in oxide",
						zap.String("oxide.instance.id", instanceID),
						zap.Any("state", pctx.State.TypedSpec().Value),
					)

					instance, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
						Instance: oxide.NameOrId(instanceID),
					})
					if err != nil {
						return fmt.Errorf("failed viewing oxide instance: %w", err)
					}

					logger.Info("instance already exists",
						zap.String("oxide.instance.id", instanceID),
						zap.String("oxide.instance.run_state", string(instance.RunState)),
						zap.String("oxide.instance.name", string(instance.Name)),
					)

					pctx.State.TypedSpec().Value.Uuid = instance.Id
					pctx.State.TypedSpec().Value.InstanceId = instance.Id

					pctx.SetMachineUUID(pctx.State.TypedSpec().Value.Uuid)
					pctx.SetMachineInfraID(pctx.State.TypedSpec().Value.InstanceId)

					return nil
				}

				var machineClass MachineClass
				if err := pctx.UnmarshalProviderData(&machineClass); err != nil {
					return fmt.Errorf("failed unmarshaling provider data: %w", err)
				}

				params := oxide.InstanceCreateParams{
					Project: oxide.NameOrId(machineClass.Project),
					Body: &oxide.InstanceCreate{
						AntiAffinityGroups: []oxide.NameOrId{},
						AutoRestartPolicy:  "",
						BootDisk: &oxide.InstanceDiskAttachment{
							Description: fmt.Sprintf("Managed by the Oxide Omni infrastructure provider (%s).", ID),
							DiskSource: oxide.DiskSource{
								BlockSize: 512,
								Type:      oxide.DiskSourceTypeImage,
								ImageId:   pctx.State.TypedSpec().Value.ImageId,
							},
							Name: oxide.Name(pctx.GetRequestID()),
							Size: oxide.ByteCount(machineClass.DiskSize * 1024 * 1024 * 1024),
							Type: oxide.InstanceDiskAttachmentTypeCreate,
						},
						Description: fmt.Sprintf("Managed by the Oxide Omni infrastructure provider (%s).", ID),
						Disks:       []oxide.InstanceDiskAttachment{},
						ExternalIps: []oxide.ExternalIpCreate{},
						Hostname:    oxide.Hostname(pctx.GetRequestID()),
						Memory:      oxide.ByteCount(machineClass.Memory * 1024 * 1024 * 1024),
						Name:        oxide.Name(pctx.GetRequestID()),
						Ncpus:       oxide.InstanceCpuCount(machineClass.VCPUS),
						NetworkInterfaces: oxide.InstanceNetworkInterfaceAttachment{
							Params: []oxide.InstanceNetworkInterfaceCreate{
								{
									Description: fmt.Sprintf("Managed by the Oxide Omni infrastructure provider (%s).", ID),
									Name:        oxide.Name(pctx.GetRequestID()),
									VpcName:     oxide.Name(machineClass.VPC),
									SubnetName:  oxide.Name(machineClass.Subnet),
								},
							},
							Type: oxide.InstanceNetworkInterfaceAttachmentTypeCreate,
						},
						SshPublicKeys: []oxide.NameOrId{},
						Start:         oxide.NewPointer(true),
						// TODO: Stop using the template once https://github.com/siderolabs/talos/issues/11948 is released in v1.12.
						UserData: base64.StdEncoding.EncodeToString(
							[]byte(fmt.Sprintf(userdataTemplate, pctx.ConnectionParams.JoinConfig)),
						),
					},
				}

				instance, err := p.oxideClient.InstanceCreate(ctx, params)
				if err != nil {
					return fmt.Errorf("failed creating oxide instance: %w", err)
				}

				logger.Info("created instance",
					zap.String("oxide.instance.id", instance.Id),
					zap.String("oxide.instance.name", string(instance.Name)),
				)

				pctx.State.TypedSpec().Value.Uuid = instance.Id
				pctx.State.TypedSpec().Value.InstanceId = instance.Id

				pctx.SetMachineUUID(pctx.State.TypedSpec().Value.Uuid)
				pctx.SetMachineInfraID(pctx.State.TypedSpec().Value.InstanceId)

				return nil
			},
		),
	}
}

// Deprovision destroys the Oxide machine that was created during [Provisioner.ProvisionSteps].
func (p *Provisioner) Deprovision(ctx context.Context, logger *zap.Logger, machine *Machine, req *infra.MachineRequest) error {
	logger.Info("DEBUG",
		zap.Any("machine", machine),
		zap.Any("req", req),
		zap.Any("req.Metadata", req.Metadata()),
		zap.Any("req.Metadata.ID", req.Metadata().ID()),
		zap.Any("providerData", req.TypedSpec().Value.ProviderData),
	)

	var machineClass MachineClass
	if err := yaml.Unmarshal([]byte(req.TypedSpec().Value.ProviderData), &machineClass); err != nil {
		return fmt.Errorf("failed unmarshaling provider data: %w", err)
	}

	// TODO: Figure out why machine is `nil` and use that instead.
	// * https://github.com/siderolabs/omni/discussions/1633#discussioncomment-14756216
	instance, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
		Project:  oxide.NameOrId(machineClass.Project),
		Instance: oxide.NameOrId(req.Metadata().ID()),
	})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			logger.Info("instance not found, already deleted", zap.String("instance_id", machine.TypedSpec().Value.InstanceId))
			return nil
		}
		return fmt.Errorf("failed viewing oxide instance: %w", err)
	}

	bootDiskID := instance.BootDiskId

	if _, err := p.oxideClient.InstanceStop(ctx, oxide.InstanceStopParams{
		Instance: oxide.NameOrId(instance.Id),
	}); err != nil {
		return fmt.Errorf("failed to stop oxide instance: %w", err)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	for {
		select {
		case <-stopCtx.Done():
			return fmt.Errorf("timed out waiting for instance to stop: %w", ctx.Err())
		default:
		}

		instance, err := p.oxideClient.InstanceView(stopCtx, oxide.InstanceViewParams{
			Instance: oxide.NameOrId(instance.Id),
		})
		if err != nil {
			return fmt.Errorf("failed refreshing oxide instance state: %w", err)
		}

		if instance.RunState == oxide.InstanceStateStopped {
			break
		}

		time.Sleep(3 * time.Second)
	}

	if err := p.oxideClient.InstanceDelete(ctx, oxide.InstanceDeleteParams{
		Instance: oxide.NameOrId(instance.Id),
	}); err != nil {
		return fmt.Errorf("failed deleting oxide instance: %w", err)
	}

	logger.Info("deleted instance",
		zap.String("instance_id", instance.Id),
	)

	if bootDiskID != "" {
		if err := p.oxideClient.DiskDelete(ctx, oxide.DiskDeleteParams{
			Disk: oxide.NameOrId(bootDiskID),
		}); err != nil {
			return fmt.Errorf("failed deleting boot disk: %w", err)
		}

		logger.Info("deleted boot disk", zap.String("boot_disk_id", bootDiskID))
	}

	return nil
}
