package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/resource/typed"
	"github.com/cosi-project/runtime/pkg/safe"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/inmem"
	"github.com/cosi-project/runtime/pkg/state/impl/namespaced"
	"github.com/oxidecomputer/omni-infra-provider-oxide/internal/provider/spec"
	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/siderolabs/image-factory/pkg/schematic"
	"github.com/siderolabs/omni/client/pkg/imagefactory"
	"github.com/siderolabs/omni/client/pkg/infra/provision"
	"github.com/siderolabs/omni/client/pkg/omni/resources/infra"
	"github.com/ulikunitz/xz"
	"go.uber.org/zap/zaptest"
)

const (
	testStorageKey = "cb0085e751f63465c8c27e82f83569251266355b3279a4313e822f46013e9e0b"
	testImageName  = "talos-v1-12-1-cb0085e751f63465"
	testProjectID  = "00000000-0000-0000-0000-00000000000a"
)

func TestOxideImageName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		talosVersion string
		storageKey   string
		want         string
		wantErr      string
	}{
		{
			name:         "version without v prefix",
			talosVersion: "1.12.1",
			storageKey:   testStorageKey,
			want:         testImageName,
		},
		{
			name:         "version with v prefix",
			talosVersion: "v1.12.1",
			storageKey:   testStorageKey,
			want:         testImageName,
		},
		{
			name:         "prerelease version",
			talosVersion: "1.13.0-alpha.1",
			storageKey:   testStorageKey,
			want:         "talos-v1-13-0-alpha-1-cb0085e751f63465",
		},
		{
			name:         "name longer than oxide allows",
			talosVersion: "1.13.0-" + strings.Repeat("a", 34),
			storageKey:   testStorageKey,
			wantErr:      "maximum is 63",
		},
		{
			name:         "storage key that is not lowercase hex",
			talosVersion: "1.12.1",
			storageKey:   strings.ToUpper(testStorageKey),
			wantErr:      "must contain only lowercase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := oxideImageName(tt.talosVersion, tt.storageKey)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v (name %q)",
						tt.wantErr, err, got)
				}

				return
			}

			if err != nil {
				t.Fatalf("oxideImageName() error = %v", err)
			}

			if string(got) != tt.want {
				t.Fatalf("oxideImageName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// fakeOxide serves the Oxide API calls ensureImage makes and fails the test on
// any other call. Its images map image IDs to names.
type fakeOxide struct {
	t       *testing.T
	mu      sync.Mutex
	images  map[string]string
	newID   string
	uploads int
}

func (f *fakeOxide) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	reply := func(status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
	notFound := func() {
		reply(http.StatusNotFound, oxide.Error{ErrorCode: "ObjectNotFound"})
	}

	path := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	switch {
	case r.Method == http.MethodGet && len(path) == 3 && path[1] == "projects":
		reply(http.StatusOK, oxide.Project{Id: testProjectID, Name: "omni"})
	case r.Method == http.MethodGet && len(path) == 3 && path[1] == "images":
		for id, name := range f.images {
			// Like Oxide, match by name only within the requested project.
			if id == path[2] ||
				(r.URL.Query().Get("project") != "" && name == path[2]) {
				reply(http.StatusOK, oxide.Image{Id: id, Name: oxide.Name(name)})
				return
			}
		}
		notFound()
	case r.Method == http.MethodPost && r.URL.Path == "/v1/disks":
		f.uploads++
		reply(http.StatusCreated, oxide.Disk{Id: "disk-id"})
	case r.Method == http.MethodPost && len(path) == 4 && path[1] == "disks":
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && len(path) == 3 && path[1] == "snapshots":
		reply(http.StatusOK, oxide.Snapshot{Id: "snapshot-id", DiskId: "disk-id"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/images":
		var body oxide.ImageCreate
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			f.t.Errorf("failed decoding image create: %v", err)
		}
		for _, name := range f.images {
			if name == string(body.Name) {
				reply(http.StatusBadRequest,
					oxide.Error{ErrorCode: "ObjectAlreadyExists"})
				return
			}
		}
		f.images[f.newID] = string(body.Name)
		reply(http.StatusCreated, oxide.Image{Id: f.newID, Name: body.Name})
	case r.Method == http.MethodDelete:
		notFound()
	default:
		f.t.Errorf("unexpected oxide request %s %s", r.Method, r.URL)
		reply(http.StatusInternalServerError, oxide.Error{ErrorCode: "Internal"})
	}
}

// TestEnsureImage checks that ensureImage only pins images this provider
// registered, and fails closed instead of adopting an image by name.
func TestEnsureImage(t *testing.T) {
	t.Parallel()

	const (
		registeredID = "00000000-0000-0000-0000-000000000001"
		attackerID   = "00000000-0000-0000-0000-000000000002"
		uploadedID   = "00000000-0000-0000-0000-000000000003"
	)

	tests := []struct {
		name         string
		registeredID string
		images       map[string]string
		wantImageID  string
		wantErr      string
		wantUpload   bool
	}{
		{
			name:         "reuses registered image",
			registeredID: registeredID,
			images:       map[string]string{registeredID: testImageName},
			wantImageID:  registeredID,
		},
		{
			name:        "uploads and registers image when name is free",
			images:      map[string]string{},
			wantImageID: uploadedID,
			wantUpload:  true,
		},
		{
			name:         "replaces deleted registered image",
			registeredID: registeredID,
			images:       map[string]string{},
			wantImageID:  uploadedID,
			wantUpload:   true,
		},
		{
			name:    "refuses unregistered image holding the name",
			images:  map[string]string{attackerID: testImageName},
			wantErr: "held by image " + attackerID,
		},
		{
			name:         "refuses image recreated after registered image deleted",
			registeredID: registeredID,
			images:       map[string]string{attackerID: testImageName},
			wantErr:      "held by image " + attackerID,
		},
	}

	var talosImage bytes.Buffer
	xzw, err := xz.NewWriter(&talosImage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := xzw.Write(bytes.Repeat([]byte{1}, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if err := xzw.Close(); err != nil {
		t.Fatal(err)
	}

	factory := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(talosImage.Bytes())
		},
	))
	t.Cleanup(factory.Close)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			fake := &fakeOxide{t: t, images: tt.images, newID: uploadedID}
			server := httptest.NewServer(fake)
			t.Cleanup(server.Close)

			oxideClient, err := oxide.NewClient(
				oxide.WithHost(server.URL), oxide.WithToken("token"),
			)
			if err != nil {
				t.Fatal(err)
			}

			st := state.WrapCore(namespaced.NewState(inmem.Build))
			registrationID := testProjectID + "." + testStorageKey
			rd := ImageRegistrationExtension{}.ResourceDefinition()

			if tt.registeredID != "" {
				if err := st.Create(ctx, typed.NewResource[
					ImageRegistrationSpec, ImageRegistrationExtension,
				](
					resource.NewMetadata(
						rd.DefaultNamespace, rd.Type, registrationID,
						resource.VersionUndefined,
					),
					protobuf.NewResourceSpec(&spec.ImageRegistration{
						Image: &spec.Image{Id: tt.registeredID},
					}),
				)); err != nil {
					t.Fatal(err)
				}
			}

			machineRequest := infra.NewMachineRequest("request")
			machineRequest.TypedSpec().Value.TalosVersion = "1.12.1"
			machineRequest.TypedSpec().Value.ProviderData = `
project: omni
cpus: 2
memory: 4
boot_disk_size: 20
cpu_platform: amd_milan
auto_restart_policy: never
network_interfaces:
  - ip_config: v4
    subnet_name: default
    vpc_name: default
`
			machine := typed.NewResource[MachineSpec, MachineExtension](
				resource.NewMetadata("ns", "machine", "request",
					resource.VersionUndefined),
				protobuf.NewResourceSpec(&spec.Machine{}),
			)

			pctx := provision.NewContext(
				machineRequest,
				infra.NewMachineRequestStatus("request"),
				machine,
				provision.ConnectionParams{},
				nil,
				func(
					context.Context, string, schematic.Schematic,
					provision.MediaSpec,
				) (imagefactory.InstallationMedia, error) {
					return imagefactory.InstallationMedia{
						URL:        factory.URL,
						StorageKey: testStorageKey,
					}, nil
				},
			)

			err = NewProvisioner(oxideClient, st).ensureImage(
				ctx, zaptest.NewLogger(t), pctx,
			)

			if got := fake.uploads > 0; got != tt.wantUpload {
				t.Errorf("uploaded = %v, want %v", got, tt.wantUpload)
			}

			registration, getErr := safe.StateGetByID[*ImageRegistration](
				ctx, st, registrationID,
			)

			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}

				if id := machine.TypedSpec().Value.GetImage().GetId(); id != "" {
					t.Errorf("pinned image %s despite error", id)
				}

				// The registration must be left as it was.
				if tt.registeredID == "" {
					if !state.IsNotFoundError(getErr) {
						t.Errorf("expected no registration, got %v", registration)
					}
				} else if id := registration.TypedSpec().Value.GetImage().GetId(); id != tt.registeredID {
					t.Errorf("registered image = %s, want %s", id, tt.registeredID)
				}

				return
			}

			if err != nil {
				t.Fatalf("ensureImage() error = %v", err)
			}

			if id := machine.TypedSpec().Value.GetImage().GetId(); id != tt.wantImageID {
				t.Errorf("pinned image = %s, want %s", id, tt.wantImageID)
			}

			if getErr != nil {
				t.Fatalf("failed reading registration: %v", getErr)
			}

			if id := registration.TypedSpec().Value.GetImage().GetId(); id != tt.wantImageID {
				t.Errorf("registered image = %s, want %s", id, tt.wantImageID)
			}
		})
	}
}

func TestProviderIDPatch(t *testing.T) {
	t.Parallel()

	const providerID = "oxide://instance-id"

	tests := []struct {
		name         string
		talosVersion string
		want         string
	}{
		{
			name:         "Talos 1.13 kubelet configuration",
			talosVersion: "1.13.9",
			want: strings.TrimPrefix(`
machine:
    kubelet:
        extraConfig:
            providerID: oxide://instance-id
`, "\n"),
		},
		{
			name:         "Talos 1.14 kubelet configuration",
			talosVersion: "1.14.0",
			want: strings.TrimPrefix(`
apiVersion: v1alpha1
kind: KubeletConfig
config:
    providerID: oxide://instance-id
`, "\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := providerIDPatch(tt.talosVersion, providerID)
			if err != nil {
				t.Fatalf("providerIDPatch() error = %v", err)
			}

			if string(got) != tt.want {
				t.Fatalf("providerIDPatch() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProviderIDPatchRejectsInvalidTalosVersion(t *testing.T) {
	t.Parallel()

	_, err := providerIDPatch("invalid", "oxide://instance-id")
	if err == nil {
		t.Fatal("providerIDPatch() expected an error")
	}
}

func TestRoundUpToGibibyte(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		n       int64
		want    int64
		wantErr bool
	}{
		{name: "zero", n: 0, want: 0},
		{name: "exact GiB", n: gibibyte, want: gibibyte},
		{name: "round up", n: gibibyte + 1, want: 2 * gibibyte},
		{
			name: "largest representable result",
			n:    math.MaxInt64 - gibibyte + 1,
			want: math.MaxInt64 - gibibyte + 1,
		},
		{name: "negative", n: -1, wantErr: true},
		{name: "overflow", n: math.MaxInt64, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := roundUpToGibibyte(tt.n)
			if (err != nil) != tt.wantErr {
				t.Fatalf("roundUpToGibibyte(%d) error = %v", tt.n, err)
			}

			if got != tt.want {
				t.Fatalf(
					"roundUpToGibibyte(%d) = %d, want %d",
					tt.n,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestProvisionStepsStartWithValidateRequest(t *testing.T) {
	t.Parallel()

	steps := NewProvisioner(nil, nil).ProvisionSteps()
	if len(steps) == 0 {
		t.Fatal("expected provision steps")
	}

	if got := steps[0].Name(); got != "validate_request" {
		t.Fatalf("expected first provision step %q, got %q", "validate_request", got)
	}
}

func TestValidateRequestIDForOxideNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		requestID string
		mc        MachineClass
		wantErr   string
	}{
		{
			name:      "valid request ID with longest derived name at limit",
			requestID: strings.Repeat("a", 54),
			mc: MachineClass{
				NetworkInterfaces: make([]NetworkInterface, 1),
			},
		},
		{
			name:      "empty request ID",
			requestID: "",
			wantErr:   "request ID \"\" is invalid: must not be empty",
		},
		{
			name:      "request ID starts with digit",
			requestID: "1machine",
			wantErr:   "must start with a lowercase ASCII letter",
		},
		{
			name:      "request ID contains uppercase",
			requestID: "machine-A",
			wantErr:   "must contain only lowercase ASCII letters, digits, or dashes",
		},
		{
			name:      "request ID contains underscore",
			requestID: "machine_a",
			wantErr:   "must contain only lowercase ASCII letters, digits, or dashes",
		},
		{
			name:      "request ID ends with dash",
			requestID: "machine-",
			wantErr:   "must not end with a dash",
		},
		{
			name:      "request ID exceeds direct name limit",
			requestID: strings.Repeat("a", 64),
			wantErr:   "maximum is 63",
		},
		{
			name:      "request ID exceeds derived interface name limit",
			requestID: strings.Repeat("a", 55),
			mc: MachineClass{
				NetworkInterfaces: make([]NetworkInterface, 1),
			},
			wantErr: "derived Oxide name",
		},
		{
			name:      "request ID exceeds derived disk name limit",
			requestID: strings.Repeat("a", 56),
			mc: MachineClass{
				Disks: make([]Disk, 1),
			},
			wantErr: "derived Oxide name",
		},
		{
			name:      "three digit interface index is included",
			requestID: strings.Repeat("a", 54),
			mc: MachineClass{
				NetworkInterfaces: make([]NetworkInterface, 101),
			},
			wantErr: "iface-100-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateRequestIDForOxideNames(tt.requestID, tt.mc)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}

			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err)
			}
		})
	}
}
