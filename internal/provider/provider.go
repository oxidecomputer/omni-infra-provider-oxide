package provider

import (
	_ "embed"
	"errors"
	"fmt"

	"github.com/oxidecomputer/oxide.go/oxide"
)

// ID uniquely identifies the infrastructure provider within Omni. Omni requires
// each infrastructure provider to have a unique ID, even if it's the same type
// of provider (e.g., Oxide).
//
// This is a mutable package-level variable to enable users to run multiple
// Oxide infrastructure providers in the same Omni installation (e.g.,
// to support multiple Oxide silos). Users set an ID when running the
// infrastructure provider which will in turn mutate this variable before
// registering the infrastructure provider with Omni.
var ID = "oxide"

// Icon is the bytes for the infrastructure provider icon.
//
//go:embed assets/oxide-icon.svg
var Icon []byte

// MachineClassSchema is the JSON schema for the fields users can set when
// creating an Omni machine class.
//
//go:embed assets/machine-class-schema.json
var MachineClassSchema string

// MachineClass is the type that the JSON schema represented by
// [MachineClassSchema] will be marshaled to and unmarshaled from.
type MachineClass struct {
	Project            oxide.NameOrId                  `json:"project" yaml:"project"`
	NCPUS              oxide.InstanceCpuCount          `json:"ncpus" yaml:"ncpus"`
	Memory             oxide.ByteCount                 `json:"memory" yaml:"memory"`
	BootDiskSize       oxide.ByteCount                 `json:"boot_disk_size" yaml:"boot_disk_size"`
	DataDisks          []DataDisk                      `json:"data_disks,omitempty" yaml:"data_disks,omitempty"`
	NetworkInterfaces  []NetworkInterface              `json:"network_interfaces,omitempty" yaml:"network_interfaces,omitempty"`
	AutoRestartPolicy  oxide.InstanceAutoRestartPolicy `json:"auto_restart_policy,omitempty" yaml:"auto_restart_policy,omitempty"`
	CPUPlatform        oxide.InstanceCpuPlatform       `json:"cpu_platform,omitempty" yaml:"cpu_platform,omitempty"`
	AntiAffinityGroups []oxide.NameOrId                `json:"anti_affinity_groups,omitempty" yaml:"anti_affinity_groups,omitempty"`
}

// Validate ensures the machine class has the required values needed to create
// an Oxide instance.
func (mc MachineClass) Validate() error {
	var errs []error

	if mc.Project == "" {
		errs = append(errs, errors.New("project is required"))
	}

	if mc.NCPUS < 1 {
		errs = append(errs, errors.New("ncpus must be at least 1"))
	}

	if mc.Memory < 1 {
		errs = append(errs, errors.New("memory must be at least 1 GiB"))
	}

	if mc.BootDiskSize < 1 {
		errs = append(errs, errors.New("boot_disk_size must be at least 1 GiB"))
	}

	if len(mc.NetworkInterfaces) == 0 {
		errs = append(errs, errors.New("network_interfaces must contain at least one interface"))
	}

	for i, disk := range mc.DataDisks {
		if disk.Size < 1 {
			errs = append(errs, fmt.Errorf("data_disks[%d].size must be at least 1 GiB", i))
		}
	}

	return errors.Join(errs...)
}

// NetworkInterface describes a network interface to attach to the machine.
type NetworkInterface struct {
	// IPConfig is the IP stack configuration for the network interface.
	IPConfig oxide.PrivateIpConfigType `json:"ip_config" yaml:"ip_config"`

	// SubnetName is the VPC subnet in which to create the interface.
	SubnetName oxide.Name `json:"subnet_name" yaml:"subnet_name"`

	// VPCName is the VPC in which to create the interface.
	VPCName oxide.Name `json:"vpc_name" yaml:"vpc_name"`
}

// DataDisk describes an additional data disk to attach to the machine.
type DataDisk struct {
	// Type is the backend type for the data disk.
	Type oxide.DiskBackendType `json:"type" yaml:"type"`

	// Size is the size of the data disk, in gibibytes.
	Size oxide.ByteCount `json:"size" yaml:"size"`
}
