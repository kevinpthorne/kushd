{
  description = "Kushd Container Image Suite";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        buildKushdBin = name: pkgs.buildGoModule {
          pname = name;
          version = "1.0.2";
          src = ./.;
          subPackages = [ "cmd/${name}" ];
          vendorHash = null;
          ldflags = [ "-s" "-w" "-extldflags '-static'" ];
        };

        kushdManager = buildKushdBin "kushd-manager";
        kushdAgent = buildKushdBin "kushd-agent";

        buildKushdImage = { name, package, extraContents ? [] }:
          pkgs.dockerTools.buildLayeredImage {
            inherit name;
            tag = "latest";
            contents = extraContents;
            config = {
              Entrypoint = [ "${package}/bin/${name}" ];
            };
          };

      in {
        packages = {
          manager-image = buildKushdImage {
            name = "kushd-manager";
            package = kushdManager;
            extraContents = [ pkgs.nut ];
          };
          agent-image = buildKushdImage {
            name = "kushd-agent";
            package = kushdAgent;
          };
        };
      }
    );
}
