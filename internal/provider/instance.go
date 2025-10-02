package provider

import (
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/resource/meta"
	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/resource/typed"
	oxideResource "github.com/oxidecomputer/omni-infra-provider-oxide/internal/resource"
	"github.com/siderolabs/omni/client/pkg/infra"
)

type Instance = typed.Resource[InstanceSpec, InstanceExtension]

type InstanceSpec = protobuf.ResourceSpec[oxideResource.Instance, *oxideResource.Instance]

type InstanceExtension struct{}

func (InstanceExtension) ResourceDefinition() meta.ResourceDefinitionSpec {
	return meta.ResourceDefinitionSpec{
		Type:             infra.ResourceType("instance", ID),
		Aliases:          []resource.Type{},
		DefaultNamespace: infra.ResourceNamespace(ID),
		PrintColumns:     []meta.PrintColumn{},
	}
}
