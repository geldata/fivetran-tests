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
          allowUnfree = true;

          buildInputs = with pkgs; [
            # for generating Fivetran client API
            just
            openapi-python-client

            # runner
            gel.packages.${system}.gel-server
            gel.packages.${system}.gel-cli
            (rust_toolchain.withComponents [
              "cargo"
              "clippy"
              "rust-src"
              "rustc"
              "rustfmt"
              "rust-analyzer"
            ])

            # development
            python312Packages.python-lsp-server
            python312Packages.python-lsp-ruff
          ];
        };
      }
    );
}
