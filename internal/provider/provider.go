package provider

import _ "embed"

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
	Project  string `json:"project" yaml:"project"`
	VCPUS    int    `json:"vcpus" yaml:"vcpus"`
	Memory   int    `json:"memory" yaml:"memory"`
	DiskSize int    `json:"disk_size" yaml:"disk_size"`
	VPC      string `json:"vpc" yaml:"vpc"`
	Subnet   string `json:"subnet" yaml:"subnet"`
}
