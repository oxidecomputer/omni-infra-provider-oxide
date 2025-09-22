package provider

import (
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/resource/meta"
	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/resource/typed"
	oxideResource "github.com/oxidecomputer/omni-infra-provider-oxide/internal/resource"
	"github.com/siderolabs/omni/client/pkg/infra"
)

func NewInstance(namespace string, id string) *Instance {
	return typed.NewResource[InstanceSpec, InstanceExtension](
		resource.NewMetadata(namespace, infra.ResourceType("Instance", ID), id, resource.VersionUndefined),
		protobuf.NewResourceSpec(&oxideResource.Instance{}),
	)
}

// Instance implements [resource.Resource] since
// that's required for [Provisioner] to implement
// [github.com/siderolabs/omni/client/pkg/infra/provision.Provisioner].
//
// TODO: It's not clear what the concrete type for [resource.Resource] is meant
// to be. Is it an instance? A machine?
type Instance = typed.Resource[InstanceSpec, InstanceExtension]

type InstanceSpec = protobuf.ResourceSpec[oxideResource.Instance, *oxideResource.Instance]

// TODO: Does this type need fields?
type InstanceExtension struct{}

// TODO: What's the purpose of [meta.ResourceDefinitionSpec]?
func (InstanceExtension) ResourceDefinition() meta.ResourceDefinitionSpec {
	return meta.ResourceDefinitionSpec{
		Type:             infra.ResourceType("Instance", ID),
		Aliases:          []resource.Type{},
		DefaultNamespace: infra.ResourceNamespace(ID),
		PrintColumns:     []meta.PrintColumn{},
	}
}
