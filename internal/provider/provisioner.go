package provider

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/omni/client/pkg/constants"
	"github.com/siderolabs/omni/client/pkg/infra/provision"
	"github.com/siderolabs/omni/client/pkg/omni/resources/infra"
	"github.com/ulikunitz/xz"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"gopkg.in/yaml.v3"
)

// Ensure [Provisioner] implements the [provision.Provisioner] interface.
var _ provision.Provisioner[*Machine] = (*Provisioner)(nil)

// Provisioner implements the [provision.Provisioner] interface to provision and
// deprovision machines on Oxide.
type Provisioner struct {
	oxideClient *oxide.Client
	httpClient  *http.Client

	// imageGroup is used to make [Provisioner.ensureImage] safe for concurrent
	// use, ensuring only one goroutine performs the expensive download and upload
	// operations.
	imageGroup singleflight.Group
}

// NewProvisioner builds and returns a new [Provisioner].
func NewProvisioner(oxideClient *oxide.Client) *Provisioner {
	return &Provisioner{
		oxideClient: oxideClient,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ProvisionSteps returns the steps necessary to provision a
// [Machine].
func (p *Provisioner) ProvisionSteps() []provision.Step[*Machine] {
	return []provision.Step[*Machine]{
		provision.NewStep("ensure_image", p.ensureImage),
		provision.NewStep("ensure_instance", p.ensureInstance),
		provision.NewStep("ensure_provider_id", p.ensureProviderID),
	}
}

// machineClassFromContext unmarshals the provider data from the
// provisioning context into a [MachineClass].
func machineClassFromContext(
	pctx provision.Context[*Machine],
) (MachineClass, error) {
	var machineClass MachineClass
	if err := pctx.UnmarshalProviderData(&machineClass); err != nil {
		return MachineClass{}, fmt.Errorf(
			"failed unmarshaling provider data: %w", err,
		)
	}
	return machineClass, nil
}

// ensureImage ensures that the requested Talos image exists in the target
// Oxide project, downloading and uploading it if it does not already exist.
func (p *Provisioner) ensureImage(
	ctx context.Context,
	logger *zap.Logger,
	pctx provision.Context[*Machine],
) error {
	machineClass, err := machineClassFromContext(pctx)
	if err != nil {
		return fmt.Errorf("failed retrieving machine class from context: %w", err)
	}

	talosImage, err := resolveTalosImage(ctx, logger, pctx)
	if err != nil {
		return fmt.Errorf("failed resolving talos image: %w", err)
	}

	_, err = p.oxideClient.ImageView(ctx, oxide.ImageViewParams{
		Image:   oxide.NameOrId(talosImage.Name),
		Project: oxide.NameOrId(machineClass.Project),
	})
	switch {
	case err == nil:
		// Image already exists. Nothing to do.
		return nil
	case !strings.Contains(err.Error(), "404"):
		return fmt.Errorf("failed viewing oxide image: %w", err)
	}

	// Key on both project and image name because Oxide images are project-scoped.
	// Concurrent requests for the same image in the same project will coalesce to
	// a single download/upload, while requests for the same image name in different
	// projects will each perform a download/upload.
	key := fmt.Sprintf("%s/%s", machineClass.Project, talosImage.Name)

	// Use DoChan so each caller can return when its own context is canceled without
	// aborting the shared work. The leader runs on a detached context so one caller
	// leaving does not cancel the download/upload for the others.
	ch := p.imageGroup.DoChan(key, func() (any, error) {
		imageCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 15*time.Minute,
		)
		defer cancel()

		f, err := downloadImageToTempFile(
			imageCtx, p.httpClient, talosImage.URL, talosImage.Name,
		)
		if err != nil {
			return nil, fmt.Errorf("failed downloading talos image: %w", err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		// Fetch the image size to create the correctly sized Oxide disk for the image.
		fi, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed retrieving file information: %w", err)
		}

		return nil, createOxideImage(
			imageCtx, logger, p.oxideClient, pctx, machineClass,
			talosImage, fi.Size(), f,
		)
	})

	select {
	case <-ctx.Done():
		return fmt.Errorf("timed out while creating image: %w", ctx.Err())
	case res := <-ch:
		if res.Err != nil {
			return fmt.Errorf("failed creating image: %w", res.Err)
		}
	}

	return nil
}

// ensureInstance ensures that the requested Talos instance exists in the target
// Oxide project, creating an Oxide instance if it does not already exist.
func (p *Provisioner) ensureInstance(
	ctx context.Context,
	logger *zap.Logger,
	pctx provision.Context[*Machine],
) error {
	machineClass, err := machineClassFromContext(pctx)
	if err != nil {
		return fmt.Errorf("failed retrieving machine class from context: %w", err)
	}

	instance, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
		Project:  oxide.NameOrId(machineClass.Project),
		Instance: oxide.NameOrId(pctx.GetRequestID()),
	})
	switch {
	case err == nil:
		// Instance already exists. Nothing to do.
		return nil
	case !strings.Contains(err.Error(), "404"):
		return fmt.Errorf("failed viewing oxide instance: %w", err)
	}

	talosImage, err := resolveTalosImage(ctx, logger, pctx)
	if err != nil {
		return fmt.Errorf("failed resolving talos image: %w", err)
	}

	image, err := p.oxideClient.ImageView(ctx, oxide.ImageViewParams{
		Project: oxide.NameOrId(machineClass.Project),
		Image:   oxide.NameOrId(talosImage.Name),
	})
	if err != nil {
		return fmt.Errorf("failed viewing oxide image: %w", err)
	}

	params := oxide.InstanceCreateParams{
		Project: oxide.NameOrId(machineClass.Project),
		Body: &oxide.InstanceCreate{
			AntiAffinityGroups: []oxide.NameOrId{},
			AutoRestartPolicy:  "",
			BootDisk: oxide.InstanceDiskAttachment{
				Value: &oxide.InstanceDiskAttachmentCreate{
					Description: fmt.Sprintf(
						"Managed by the Oxide Omni infrastructure provider (%s).",
						ID,
					),
					DiskBackend: oxide.DiskBackend{
						Value: &oxide.DiskBackendDistributed{
							DiskSource: oxide.DiskSource{
								Value: &oxide.DiskSourceImage{
									ImageId: image.Id,
								},
							},
						},
					},
					Name: oxide.Name(pctx.GetRequestID()),
					Size: oxide.ByteCount(machineClass.DiskSize * 1024 * 1024 * 1024),
				},
			},
			Description: fmt.Sprintf(
				"Managed by the Oxide Omni infrastructure provider (%s).",
				ID,
			),
			Disks:       []oxide.InstanceDiskAttachment{},
			ExternalIps: []oxide.ExternalIpCreate{},
			Hostname:    oxide.Hostname(pctx.GetRequestID()),
			Memory:      oxide.ByteCount(machineClass.Memory * 1024 * 1024 * 1024),
			Name:        oxide.Name(pctx.GetRequestID()),
			Ncpus:       oxide.InstanceCpuCount(machineClass.VCPUS),
			NetworkInterfaces: oxide.InstanceNetworkInterfaceAttachment{
				Value: &oxide.InstanceNetworkInterfaceAttachmentCreate{
					Params: []oxide.InstanceNetworkInterfaceCreate{
						{
							Description: fmt.Sprintf(
								"Managed by the Oxide Omni infrastructure provider (%s).",
								ID,
							),
							Name:       oxide.Name(pctx.GetRequestID()),
							VpcName:    oxide.Name(machineClass.VPC),
							SubnetName: oxide.Name(machineClass.Subnet),
							IpConfig: oxide.PrivateIpStackCreate{
								Value: oxide.PrivateIpStackCreateV4{
									Value: oxide.PrivateIpv4StackCreate{
										Ip: oxide.Ipv4Assignment{
											Value: &oxide.Ipv4AssignmentAuto{},
										},
									},
								},
							},
						},
					},
				},
			},
			SshPublicKeys: []oxide.NameOrId{},
			Start:         new(true),
			UserData: base64.StdEncoding.EncodeToString(
				[]byte(pctx.ConnectionParams.JoinConfig),
			),
		},
	}

	instance, err = p.oxideClient.InstanceCreate(ctx, params)
	if err != nil {
		return fmt.Errorf("failed creating oxide instance: %w", err)
	}

	logger.Info("created instance",
		zap.String("oxide.instance.id", instance.Id),
		zap.String("oxide.instance.name", string(instance.Name)),
	)

	return nil
}

// ensureProviderID creates an Omni configuration patch that sets the kubelet
// providerID extra config to the Oxide instance ID, allowing Kubernetes to
// associate the node with its Oxide instance.
func (p *Provisioner) ensureProviderID(
	ctx context.Context,
	logger *zap.Logger,
	pctx provision.Context[*Machine],
) error {
	machineClass, err := machineClassFromContext(pctx)
	if err != nil {
		return fmt.Errorf("failed retrieving machine class from context: %w", err)
	}

	instance, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
		Project:  oxide.NameOrId(machineClass.Project),
		Instance: oxide.NameOrId(pctx.GetRequestID()),
	})
	if err != nil {
		return fmt.Errorf("failed viewing oxide instance: %w", err)
	}

	patch := struct {
		Machine struct {
			Kubelet struct {
				ExtraConfig map[string]string `yaml:"extraConfig"`
			} `yaml:"kubelet"`
		} `yaml:"machine"`
	}{}
	patch.Machine.Kubelet.ExtraConfig = map[string]string{
		"providerID": fmt.Sprintf("oxide://%s", instance.Id),
	}

	b, err := yaml.Marshal(patch)
	if err != nil {
		return fmt.Errorf(
			"failed marshaling provider id patch: %w", err,
		)
	}

	if err := pctx.CreateConfigPatch(ctx, "providerID", b); err != nil {
		return fmt.Errorf(
			"failed creating providerID config patch: %w", err,
		)
	}

	return nil
}

// Deprovision destroys the [Machine] that was created during
// [Provisioner.ProvisionSteps].
func (p *Provisioner) Deprovision(
	ctx context.Context,
	logger *zap.Logger,
	machine *Machine,
	req *infra.MachineRequest,
) error {
	var machineClass MachineClass
	if err := yaml.Unmarshal(
		[]byte(req.TypedSpec().Value.ProviderData),
		&machineClass,
	); err != nil {
		return fmt.Errorf("failed to unmarshal provider data: %w", err)
	}

	instance, err := p.oxideClient.InstanceView(ctx, oxide.InstanceViewParams{
		Project:  oxide.NameOrId(machineClass.Project),
		Instance: oxide.NameOrId(req.Metadata().ID()),
	})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			logger.Info("instance not found, already deleted",
				zap.String("oxide.instance.name", req.Metadata().ID()),
				zap.String("oxide.project", machineClass.Project),
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
		}); err != nil && !strings.Contains(err.Error(), "404") {
			return fmt.Errorf("failed deleting boot disk: %w", err)
		}

		logger.Info("deleted boot disk",
			zap.String("oxide.instance.boot_disk_id", instance.BootDiskId),
		)
	}

	return nil
}

// roundToNearestGibibyte rounds n up to the nearest multiple of 1 GiB.
func roundToNearestGibibyte(n int64) int64 {
	const Gibibyte = 1024 * 1024 * 1024
	return (n + Gibibyte - 1) / Gibibyte * Gibibyte
}

// randomSuffix returns a hex-encoded random string built from n bytes of
// crypto/rand entropy. The returned string has length 2*n.
func randomSuffix(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed reading random bytes: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// TalosImage holds all the information needed for a requested Talos image.
type TalosImage struct {
	// Name is the name of the Talos image.
	Name string

	// URL is the Talos Image Factory URL where the image can be downloaded.
	URL string

	// SchematicID is the Talos schematic ID for the image.
	SchematicID string
}

// resolveTalosImage determines the information for the requested Talos image.
func resolveTalosImage(
	ctx context.Context,
	logger *zap.Logger,
	pctx provision.Context[*Machine],
) (*TalosImage, error) {
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
		return nil, fmt.Errorf("failed generating schematic id: %w", err)
	}

	talosVersion := pctx.GetTalosVersion()

	imageFactoryBaseURL, err := url.Parse(constants.ImageFactoryBaseURL)
	if err != nil {
		return nil, fmt.Errorf(
			"failed parsing talos image factory base url: %w", err,
		)
	}

	imageFactoryURL := imageFactoryBaseURL.JoinPath(
		"image",
		schematicID,
		talosVersion,
		"nocloud-amd64.raw.xz",
	)

	name := fmt.Sprintf("talos-%s-%s-nocloud",
		strings.ReplaceAll(talosVersion, ".", "-"),
		schematicID[:8],
	)

	return &TalosImage{
		SchematicID: schematicID,
		Name:        name,
		URL:         imageFactoryURL.String(),
	}, nil
}

// downloadImageToTempFile downloads and decompresses the image at url into a
// new temporary file and returns it open and ready for reading. Callers are
// responsible for closing and removing the returned file.
func downloadImageToTempFile(
	ctx context.Context,
	httpClient *http.Client,
	url string,
	namePrefix string,
) (_ *os.File, err error) {
	f, err := os.CreateTemp("", namePrefix)
	if err != nil {
		return nil, fmt.Errorf(
			"failed creating temporary file: %w", err,
		)
	}
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed creating image request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed downloading image: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"unexpected status downloading image: %s", resp.Status,
		)
	}

	xzReader, err := xz.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed creating xz reader: %w", err)
	}

	if _, err = io.Copy(f, xzReader); err != nil {
		return nil, fmt.Errorf("failed decompressing image: %w", err)
	}

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed seeking to start of image file: %w", err)
	}

	return f, nil
}

// createOxideImage uploads the contents of r as a new Oxide image. It does so
// by writing the bytes into a temporary disk, snapshotting it, and creating
// the image from the snapshot. The temporary disk and snapshot are scratch
// artifacts and are deleted before returning. Only the image is kept.
func createOxideImage(
	ctx context.Context,
	logger *zap.Logger,
	client *oxide.Client,
	pctx provision.Context[*Machine],
	machineClass MachineClass,
	talosImage *TalosImage,
	imageSize int64,
	r io.Reader,
) error {
	imageName := oxide.Name(talosImage.Name)
	project := oxide.NameOrId(machineClass.Project)
	talosVersion := pctx.GetTalosVersion()

	// Scratch artifacts are suffixed with a fresh random token per invocation so
	// they never collide with concurrent runs or with in-flight cleanups from a
	// previous attempt.
	suffix, err := randomSuffix(4)
	if err != nil {
		return fmt.Errorf("failed generating scratch name suffix: %w", err)
	}
	scratchName := oxide.Name(fmt.Sprintf("%s-%s", talosImage.Name, suffix))

	disk, err := client.DiskCreate(ctx, oxide.DiskCreateParams{
		Project: project,
		Body: &oxide.DiskCreate{
			Description: fmt.Sprintf(
				"Temporary disk for Oxide Omni infrastructure provider (%s).",
				pctx.GetRequestID(),
			),
			DiskBackend: oxide.DiskBackend{
				Value: &oxide.DiskBackendDistributed{
					DiskSource: oxide.DiskSource{
						Value: &oxide.DiskSourceImportingBlocks{
							BlockSize: 512,
						},
					},
				},
			},
			Name: scratchName,
			Size: oxide.ByteCount(roundToNearestGibibyte(imageSize)),
		},
	})
	if err != nil {
		return fmt.Errorf("failed creating disk: %w", err)
	}
	diskID := oxide.NameOrId(disk.Id)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 30*time.Second,
		)
		defer cancel()
		// A successful [oxide.DiskFinalizeImport] consumes the disk, so a 404 here is
		// the expected success case and should not be logged as an error.
		if err := client.DiskDelete(cleanupCtx, oxide.DiskDeleteParams{
			Disk: diskID,
		}); err != nil && !strings.Contains(err.Error(), "404") {
			logger.Error("failed deleting temporary disk",
				zap.String("oxide.disk.id", disk.Id),
				zap.Error(err),
			)
		}
	}()

	snapshotNameOrID := oxide.NameOrId(scratchName)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 30*time.Second,
		)
		defer cancel()
		// [importBytesToDisk] may fail before the snapshot is created, in which case a
		// 404 here is expected and should not be logged as an error.
		if err := client.SnapshotDelete(cleanupCtx, oxide.SnapshotDeleteParams{
			Snapshot: snapshotNameOrID,
			Project:  project,
		}); err != nil && !strings.Contains(err.Error(), "404") {
			logger.Error("failed deleting temporary snapshot",
				zap.String("oxide.snapshot.name", string(scratchName)),
				zap.Error(err),
			)
		}
	}()

	// If [importBytesToDisk] fails after starting but before stopping, server-side
	// import state will still be reset when the deferred [oxide.DiskDelete]
	// above removes the disk entirely, so an explicit stop on the error path is
	// unnecessary.
	if err := importBytesToDisk(ctx, client, diskID, scratchName, r); err != nil {
		return fmt.Errorf("failed importing bytes to disk: %w", err)
	}

	snapshot, err := client.SnapshotView(ctx, oxide.SnapshotViewParams{
		Snapshot: snapshotNameOrID,
		Project:  project,
	})
	if err != nil {
		return fmt.Errorf("failed viewing snapshot: %w", err)
	}

	if _, err := client.ImageCreate(ctx, oxide.ImageCreateParams{
		Project: project,
		Body: &oxide.ImageCreate{
			Description: fmt.Sprintf(
				"Talos Linux v%s NoCloud (%s).",
				talosVersion,
				talosImage.SchematicID,
			),
			Name: imageName,
			Os:   "Talos Linux",
			Source: oxide.ImageSource{
				Value: &oxide.ImageSourceSnapshot{
					Id: snapshot.Id,
				},
			},
			Version: talosVersion,
		},
	}); err != nil {
		// Another concurrent provisioner may have already created the image with
		// the same name. Treat that as a successful no-op since the image content
		// is deterministic for a given name. Our scratch disk and snapshot still get
		// cleaned up via the deferred deletes above.
		if !strings.Contains(err.Error(), "409") {
			return fmt.Errorf("failed creating image: %w", err)
		}
		logger.Info("image already exists, skipping creation",
			zap.String("oxide.image.name", string(imageName)),
		)
	}

	return nil
}

// importBytesToDisk writes r into the disk identified by diskID and finalizes
// the import as a snapshot with the given name. The disk must already exist
// and be ready to accept a bulk-write import.
func importBytesToDisk(
	ctx context.Context,
	client *oxide.Client,
	disk oxide.NameOrId,
	snapshotName oxide.Name,
	r io.Reader,
) error {
	if err := client.DiskBulkWriteImportStart(ctx, oxide.DiskBulkWriteImportStartParams{
		Disk: disk,
	}); err != nil {
		return fmt.Errorf("failed starting bulk write import: %w", err)
	}

	if err := bulkImport(ctx, client, disk, r); err != nil {
		return fmt.Errorf("failed bulk importing to disk: %w", err)
	}

	if err := client.DiskBulkWriteImportStop(ctx, oxide.DiskBulkWriteImportStopParams{
		Disk: disk,
	}); err != nil {
		return fmt.Errorf("failed stopping bulk write import: %w", err)
	}

	if err := client.DiskFinalizeImport(ctx, oxide.DiskFinalizeImportParams{
		Disk: disk,
		Body: &oxide.FinalizeDisk{SnapshotName: snapshotName},
	}); err != nil {
		return fmt.Errorf("failed finalizing disk import: %w", err)
	}

	return nil
}

// bulkImport imports bytes into an Oxide disk in chunks, skipping
// chunks that contain all zeroes.
func bulkImport(
	ctx context.Context,
	oxideClient *oxide.Client,
	disk oxide.NameOrId,
	data io.Reader,
) error {
	type chunk struct {
		offset int64
		data   []byte
	}

	const workers = 8
	const chunkSize = 512 * 1024

	g, ctx := errgroup.WithContext(ctx)
	chunks := make(chan chunk, 2*workers)

	for range workers {
		g.Go(func() error {
			for c := range chunks {
				if err := oxideClient.DiskBulkWriteImport(ctx, oxide.DiskBulkWriteImportParams{
					Disk: disk,
					Body: &oxide.ImportBlocksBulkWrite{
						Base64EncodedData: base64.StdEncoding.EncodeToString(c.data),
						Offset:            new(uint64(c.offset)),
					},
				}); err != nil {
					return fmt.Errorf("failed writing chunk at offset %d: %w", c.offset, err)
				}
			}
			return nil
		})
	}

	var readErr error
	var offset int64
	buffer := make([]byte, chunkSize)
	zeros := make([]byte, chunkSize)

readLoop:
	for {
		n, err := data.Read(buffer)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = fmt.Errorf("failed reading chunk: %w", err)
			}
			break
		}
		if n == 0 {
			break
		}

		// Skip chunks that are all zeroes. The disk is already zeroed.
		if bytes.Equal(buffer[:n], zeros[:n]) {
			offset += int64(n)
			continue
		}

		select {
		case <-ctx.Done():
			readErr = fmt.Errorf("timed out while reading chunk: %w", ctx.Err())
			break readLoop
		case chunks <- chunk{offset: offset, data: bytes.Clone(buffer[:n])}:
		}
		offset += int64(n)
	}

	close(chunks)

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed waiting for bulk import workers to exit: %w", err)
	}

	return readErr
}
