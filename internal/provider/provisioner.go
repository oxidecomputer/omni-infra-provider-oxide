package provider

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"errors"

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
		// generate_schematic_id generates a Talos image factory schematic ID, which is
		// the consistent representation for all the kernel parameters, extensions, and
		// other configuration that can be passed to Talos Linux. This schematic ID is
		// later used to uniquely identify images and skip steps that were already done
		// for the given schematic ID.
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
					// infrastructure provider exclusively uses NoCloud images and passes the
					// SideroLink configuration via cloud-init instead.
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
		// generate_image_factory_url generates the URL to the specific Talos Linux
		// image needed by this provision request.
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
		// generate_image_name generates a consistent, unique name for the Talos Linux
		// image that will be used as the image name in Oxide. A hash of the Talos image
		// URL is used deduplicate images that use different schematics for the same
		// Talos Linux version.
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
					zap.String("oxide.image.name", imageName),
				)

				pctx.State.TypedSpec().Value.ImageName = imageName

				return nil
			},
		),
		// fetch_image_id checks whether the required Talos Linux image already exists
		// in Oxide. This allows us to skip downloading the image in subsequent steps.
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
							zap.String("oxide.image.name", imageName),
						)
						return nil
					}

					return fmt.Errorf("failed viewing oxide image: %w", err)
				}

				logger.Info("fetched oxide image information",
					zap.String("oxide.image.id", image.Id),
					zap.String("oxide.image.name", imageName),
				)

				pctx.State.TypedSpec().Value.ImageId = image.Id

				return nil
			},
		),
		// create_image downloads the Talos Linux image and uploads it to Oxide as a
		// project image.
		provision.NewStep(
			"create_image",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				// We already have an image to use. Skip downloading one.
				if pctx.State.TypedSpec().Value.ImageId != "" {
					return nil
				}

				// We'll need the machine class information later but we don't want to download
				// an entire image just to find out we couldn't unmarshal the machine class.
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

				// We're about to read the downloaded image to upload it to Oxide. We must start
				// at the beginning of the image.
				if _, err := f.Seek(0, 0); err != nil {
					return fmt.Errorf("failed seeking to beginning of file: %w", err)
				}

				// Retrieve the image size in bytes to create the right sized Oxide disk to hold
				// the image.
				fi, err := f.Stat()
				if err != nil {
					return fmt.Errorf("failed retrieving file information: %w", err)
				}

				disk, err := p.oxideClient.DiskCreate(ctx, oxide.DiskCreateParams{
					Project: oxide.NameOrId(machineClass.Project),
					Body: &oxide.DiskCreate{
						Description: fmt.Sprintf("Temporary disk for Oxide Omni infrastructure provider (%s).", pctx.GetRequestID()),
						DiskSource: oxide.DiskSource{
							BlockSize: 512,
							Type:      oxide.DiskSourceTypeImportingBlocks,
						},
						Name: oxide.Name(imageName),
						// Round up to the nearest 1 GiB since disks must be multiples of 1 GiB.
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

				if err := bulkImport(ctx, p.oxideClient, disk.Id, f); err != nil {
					return fmt.Errorf("failed bulk importing to disk: %w", err)
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
					return fmt.Errorf("failed viewing snapshot: %w", err)
				}
				defer func() {
					p.oxideClient.SnapshotDelete(ctx, oxide.SnapshotDeleteParams{
						Snapshot: oxide.NameOrId(snapshot.Id),
					})
				}()

				image, err := p.oxideClient.ImageCreate(ctx, oxide.ImageCreateParams{
					Project: oxide.NameOrId(machineClass.Project),
					Body: &oxide.ImageCreate{
						Description: fmt.Sprintf("Talos Linux v%s NoCloud (%s).", pctx.GetTalosVersion(), pctx.State.TypedSpec().Value.TalosSchematicId),
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
		// instance_create creates the Talos Linux instance and configures it to connect
		// to Omni using cloud-init user data.
		provision.NewStep(
			"instance_create",
			func(ctx context.Context, logger *zap.Logger, pctx provision.Context[*Machine]) error {
				instanceID := pctx.State.TypedSpec().Value.InstanceId

				if instanceID != "" {
					logger.Info("found existing instance id, confirming whether instance exists in oxide",
						zap.String("oxide.instance.id", instanceID),
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
						UserData: base64.StdEncoding.EncodeToString(
							fmt.Appendf(nil, userdataTemplate, pctx.ConnectionParams.JoinConfig),
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
	if machine == nil || machine.TypedSpec().Value.InstanceId == "" {
		logger.Warn("deprovision called without machine details",
			zap.Any("machine", machine),
		)
		return nil
	}

	instance, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
		Instance: oxide.NameOrId(machine.TypedSpec().Value.InstanceId),
	})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			logger.Info("instance not found, already deleted",
				zap.String("oxide.instance.id", machine.TypedSpec().Value.InstanceId),
			)
			return nil
		}
		return fmt.Errorf("failed viewing oxide instance: %w", err)
	}

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

		logger.Info("waiting for instance to stop",
			zap.String("oxide.instance.id", instance.Id),
			zap.String("oxide.instance.run_state", string(instance.RunState)),
		)

		time.Sleep(3 * time.Second)
	}

	if err := p.oxideClient.InstanceDelete(ctx, oxide.InstanceDeleteParams{
		Instance: oxide.NameOrId(instance.Id),
	}); err != nil {
		return fmt.Errorf("failed deleting oxide instance: %w", err)
	}

	logger.Info("deleted instance",
		zap.String("oxide.instance.id", instance.Id),
	)

	if instance.BootDiskId != "" {
		if err := p.oxideClient.DiskDelete(ctx, oxide.DiskDeleteParams{
			Disk: oxide.NameOrId(instance.BootDiskId),
		}); err != nil {
			return fmt.Errorf("failed deleting boot disk: %w", err)
		}

		logger.Info("deleted boot disk",
			zap.String("oxide.instance.boot_disk_id", instance.BootDiskId),
		)
	}

	return nil
}

// bulkImport imports bytes into an Oxide disk in chunks, skipping chunks that
// contain all zeroes.
func bulkImport(ctx context.Context, oxideClient *oxide.Client, diskID string, data io.Reader) error {
	type chunk struct {
		offset int64
		data   []byte
	}

	numWorkers := 8
	chunks := make(chan chunk, 2*numWorkers)

	var wg sync.WaitGroup
	workerErrors := make(chan error, numWorkers)

	for range numWorkers {
		wg.Go(func() {
			for chunk := range chunks {
				if err := oxideClient.DiskBulkWriteImport(ctx, oxide.DiskBulkWriteImportParams{
					Disk: oxide.NameOrId(diskID),
					Body: &oxide.ImportBlocksBulkWrite{
						Base64EncodedData: base64.StdEncoding.EncodeToString(chunk.data),
						Offset:            oxide.NewPointer(int(chunk.offset)),
					},
				}); err != nil {
					workerErrors <- err
					return
				}
			}
		})
	}

	var readerError error
	var workerError error
	var offset int64
	buffer := make([]byte, 512*1024) // 512 KiB.

readerLoop:
	for {
		n, err := data.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break readerLoop
			}

			readerError = fmt.Errorf("failed reading chunk: %w", err)
			break readerLoop
		}

		if n == 0 {
			break readerLoop
		}

		chunkHasData := false
		for _, b := range buffer[:n] {
			if b != 0 {
				chunkHasData = true
				break
			}
		}

		// This chunk is all zeroes. Advance the offset and continue reading.
		if !chunkHasData {
			offset += int64(n)
			continue readerLoop
		}

		// Copy the data so each chunk owns its data.
		data := make([]byte, n)
		copy(data, buffer[:n])

		c := chunk{
			offset: offset,
			data:   data,
		}

		// The multiple cases protect against a deadlock where all workers have returned
		// and the chunks channel is blocked.
		select {
		case <-ctx.Done():
			readerError = ctx.Err()
			break readerLoop
		case err := <-workerErrors:
			workerError = errors.Join(workerError, err)
			break readerLoop
		case chunks <- c:
		}
		offset += int64(n)
	}

	close(chunks)
	wg.Wait()
	close(workerErrors)

	if readerError != nil {
		return readerError
	}

	for err := range workerErrors {
		workerError = errors.Join(workerError, err)
	}

	if workerError != nil {
		return workerError
	}

	return nil
}
