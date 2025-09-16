{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-parts,
      rust-overlay,
      crane,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      perSystem =
        {
          self',
          inputs',
          pkgs,
          system,
          ...
        }:
        let
          rust-toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
          craneLib = (crane.mkLib pkgs).overrideToolchain (_: rust-toolchain);

          craneAttrs =
            let
              attrs = {
                src = craneLib.cleanCargoSource ./.;
                strictDeps = true;
                doCheck = false;
                cargoTestCommand = "cargo test"; # disable release
                cargoCheckCommand = "cargo clippy"; # use clippy, disable release
                cargoCheckExtraArgs = "--all-targets -- --deny=warnings";
                cargoClippyExtraArgs = "--all-targets -- --deny=warnings";
              };
            in
            attrs
            // {
              cargoArtifacts = craneLib.buildDepsOnly attrs;
            };
        in
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            config.allowUnfree = true;
            overlays = [
              rust-overlay.overlays.default
            ];
          };

          checks = {
            build = craneLib.buildPackage craneAttrs;
            clippy = craneLib.cargoClippy craneAttrs;
            test = craneLib.cargoTest craneAttrs;
          };

          devShells.default = pkgs.mkShell {
            buildInputs = [ rust-toolchain ];
          };
        };
    };
}
