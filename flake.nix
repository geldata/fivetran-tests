{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/25.05";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    gel = {
      url = "github:geldata/packages-nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-parts.inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      gel,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        fenix_pkgs = fenix.packages.${system};

        rust_toolchain = fenix_pkgs.stable;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            (rust_toolchain.withComponents [
              "cargo"
              "clippy"
              "rust-src"
              "rustc"
              "rustfmt"
              "rust-analyzer"
            ])
            pkg-config
            openssl

            gel.packages.${system}.gel-server-nightly
            gel.packages.${system}.gel-cli

            # for generating Fivetran client API (obsolete)
            just
            openapi-python-client
          ];
        };
      }
    );
}
