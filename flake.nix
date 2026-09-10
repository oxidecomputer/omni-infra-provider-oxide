{
  description = "Oxide Omni infrastructure provider development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
    in
    {
      devShells = nixpkgs.lib.genAttrs systems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShellNoCC {
            packages = [
              pkgs.go
              pkgs.golangci-lint
              pkgs.goreleaser
              pkgs.gnumake
              pkgs.protobuf
              pkgs.protoc-gen-go
            ];

            CGO_ENABLED = "0";
            GOTOOLCHAIN = "local";
          };
        }
      );
    };
}
