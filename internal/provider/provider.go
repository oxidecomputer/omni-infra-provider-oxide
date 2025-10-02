package provider

// ID uniquely identifies the infrastructure provider within Omni. Omni requires
// each infrastructure provider to have a unique ID, even if it's the same  type
// of provider (e.g., Oxide).
//
// This is a mutable package-level variable to enable users to run multiple
// Oxide infrastructure providers in the same Omni installation (e.g., to
// support multiple Oxide deployments). Users will set this ID when running
// the infrastructure provider which will in turn mutate this variable before
// registering the infrastructure provider with Omni.
var ID = "oxide"
