package provider

import (
	"math"
	"strings"
	"testing"
)

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

	steps := NewProvisioner(nil).ProvisionSteps()
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
