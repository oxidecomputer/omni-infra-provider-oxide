package main

import "github.com/ardanlabs/conf/v3"

// Config holds the configuration necessary to run the infrastructure provider.
// Configuration fields can be set using CLI flags or environment variables.
type Config struct {
	conf.Version
	OxideHost              string `conf:"required,notzero,help:Oxide API host (e.g.; https://oxide.sys.example.com)."`
	OxideToken             string `conf:"required,notzero,mask,help:Oxide API token."`
	ProviderID             string `conf:"default:oxide,help:Provider ID."`
	ProviderName           string `conf:"default:Oxide,help:Provider name."`
	ProviderDescription    string `conf:"default:Oxide Omni infrastructure provider.,help:Provider description."`
	ProvisionerConcurrency uint   `conf:"default:1,help:Number of concurrent provisioner operations."`
	OmniEndpoint           string `conf:"required,notzero,help:Omni API endpoint (e.g.; https://omni.example.com)."`
	OmniServiceAccountKey  string `conf:"required,notzero,mask,help:Omni service account key."`
}
