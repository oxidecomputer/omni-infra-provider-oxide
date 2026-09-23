package provider

import (
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/resource/meta"
	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/resource/typed"
	"github.com/oxidecomputer/omni-infra-provider-oxide/internal/provider/spec"
	"github.com/siderolabs/omni/client/pkg/infra"
)

// Machine is the resource that this infrastructure provider manages.
type Machine = typed.Resource[MachineSpec, MachineExtension]

// MachineSpec represents the protobuf structure for a [Machine].
type MachineSpec = protobuf.ResourceSpec[spec.Machine, *spec.Machine]

// MachineExtension soley exists to implement the [typed.Extension] interface.
type MachineExtension struct{}

// ResourceDefinition returns the resource definition for a [Machine].
func (MachineExtension) ResourceDefinition() meta.ResourceDefinitionSpec {
	return meta.ResourceDefinitionSpec{
		Type:             infra.ResourceType("Machine", ID),
		Aliases:          []resource.Type{},
		DefaultNamespace: infra.ResourceNamespace(ID),
		PrintColumns:     []meta.PrintColumn{},
	}
}

// ImageRegistration is the resource that records an Oxide image this
// infrastructure provider created. Only the provider can write resources in
// its Omni namespace, so a registration can't be forged by users.
type ImageRegistration = typed.Resource[
	ImageRegistrationSpec, ImageRegistrationExtension,
]

// ImageRegistrationSpec represents the protobuf structure for an
// [ImageRegistration].
type ImageRegistrationSpec = protobuf.ResourceSpec[
	spec.ImageRegistration, *spec.ImageRegistration,
]

// ImageRegistrationExtension soley exists to implement the [typed.Extension]
// interface.
type ImageRegistrationExtension struct{}

// ResourceDefinition returns the resource definition for an
// [ImageRegistration].
func (ImageRegistrationExtension) ResourceDefinition() meta.ResourceDefinitionSpec {
	return meta.ResourceDefinitionSpec{
		Type:             infra.ResourceType("ImageRegistration", ID),
		Aliases:          []resource.Type{},
		DefaultNamespace: infra.ResourceNamespace(ID),
		PrintColumns:     []meta.PrintColumn{},
	}
}
