package provider

import (
	"strings"
	"testing"
)

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
