{
  description = "Build a cargo workspace";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/24.11-beta";

    crane.url = "github:ipetkov/crane";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-analyzer-src.follows = "";
    };

    flake-utils.url = "github:numtide/flake-utils";

    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };

    # The wash CLI flag is always after the latest host release tag we want
    wasmcloud.url = "github:wasmCloud/wasmCloud/wash-cli-v0.37.0";
  };

  outputs =
    { self, nixpkgs, crane, fenix, flake-utils, advisory-db, wasmcloud, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        inherit (pkgs) lib;

        craneLib = crane.mkLib pkgs;
        src = craneLib.cleanCargoSource ./.;

        # Common arguments can be set here to avoid repeating them later
        commonArgs = {
          inherit src;
          strictDeps = true;

          buildInputs = [
            # Add additional build inputs here
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            # Additional darwin specific inputs can be set here
            pkgs.libiconv
          ];

          # Additional environment variables can be set directly
          # MY_CUSTOM_VAR = "some value";
        };

        craneLibLLvmTools = craneLib.overrideToolchain
          (fenix.packages.${system}.complete.withComponents [
            "cargo"
            "llvm-tools"
            "rustc"
          ]);

        # Get the lock file for filtering
        rawLockFile = builtins.fromTOML (builtins.readFile ./Cargo.lock);

        # Filter out the workspace members
        filteredLockFile = rawLockFile // {
          package = builtins.filter (x: !lib.strings.hasPrefix "wadm" x.name)
            rawLockFile.package;
        };

        cargoVendorDir =
          craneLib.vendorCargoDeps { cargoLockParsed = filteredLockFile; };

        cargoLock = craneLib.writeTOML "Cargo.lock" filteredLockFile;

        # Build *just* the cargo dependencies (of the entire workspace),
        # so we can reuse all of that work (e.g. via cachix) when running in CI
        # It is *highly* recommended to use something like cargo-hakari to avoid
        # cache misses when building individual top-level-crates
        cargoArtifacts = let
          commonArgs' = removeAttrs commonArgs ["src"];

          # Get the manifest file for filtering
          rawManifestFile = builtins.fromTOML (builtins.readFile ./Cargo.toml);

          # Filter out the workspace members from manifest
          filteredManifestFile = with lib; let
            filterWadmAttrs = filterAttrs (name: _: !strings.hasPrefix "wadm" name);

            workspace = removeAttrs rawManifestFile.workspace ["members"];
          in
            rawManifestFile
            // {
              workspace =
                workspace
                // {
                  dependencies = filterWadmAttrs workspace.dependencies;
                  package =
                    workspace.package
                    // {
                      # pin version to avoid rebuilds on bumps
                      version = "0.0.0";
                    };
                };

              dependencies = filterWadmAttrs rawManifestFile.dependencies;

              dev-dependencies = filterWadmAttrs rawManifestFile.dev-dependencies;

              build-dependencies = filterWadmAttrs rawManifestFile.build-dependencies;
            };

          cargoToml = craneLib.writeTOML "Cargo.toml" filteredManifestFile;

          dummySrc = craneLib.mkDummySrc {
            src = pkgs.runCommand "wadm-dummy-src" {} ''
              mkdir -p $out
              cp --recursive --no-preserve=mode,ownership ${src}/. -t $out
              cp ${cargoToml} $out/Cargo.toml
            '';
          };

          args =
            commonArgs'
            // {
              inherit
                cargoLock
                cargoToml
                cargoVendorDir
                dummySrc
                ;

              cargoExtraArgs = ""; # disable `--locked` passed by default by crane
            };
        in
          craneLib.buildDepsOnly args;

        individualCrateArgs = commonArgs // {
          inherit (craneLib.crateNameFromCargoToml { inherit src; }) version;
          # TODO(thomastaylor312) We run unit tests here and e2e tests later. The nextest step
          # wasn't letting me pass in the fileset
          doCheck = true;
        };

        fileSetForCrate = crate:
          lib.fileset.toSource {
            root = ./.;
            fileset = lib.fileset.unions [
              ./Cargo.toml
              # ./Cargo.lock
              ./tests
              ./oam
              (craneLib.fileset.commonCargoSources ./crates/wadm)
              (craneLib.fileset.commonCargoSources ./crates/wadm-client)
              (craneLib.fileset.commonCargoSources ./crates/wadm-types)
              (craneLib.fileset.commonCargoSources crate)
            ];
          };

        # Build the top-level crates of the workspace as individual derivations.
        # This allows consumers to only depend on (and build) only what they need.
        # Though it is possible to build the entire workspace as a single derivation,
        # so this is left up to you on how to organize things
        #
        # Note that the cargo workspace must define `workspace.members` using wildcards,
        # otherwise, omitting a crate (like we do below) will result in errors since
        # cargo won't be able to find the sources for all members.
        wadm-lib = craneLib.cargoBuild (individualCrateArgs // {
          pname = "wadm";
          cargoExtraArgs = "-p wadm";
          src = fileSetForCrate ./crates/wadm;
          cargoArtifacts = wadm-types;
          doInstallCargoArtifacts = true;
        });
        wadm = craneLib.buildPackage (individualCrateArgs // {
          pname = "wadm-cli";
          cargoExtraArgs = "--bin wadm";
          src = fileSetForCrate ./.;
          cargoArtifacts = wadm-lib;
        });
        wadm-client = craneLib.cargoBuild (individualCrateArgs // {
          pname = "wadm-client";
          cargoExtraArgs = "-p wadm-client";
          src = fileSetForCrate ./crates/wadm-client;
          cargoArtifacts = wadm-types;
          doInstallCargoArtifacts = true;
        });
        wadm-types = craneLib.cargoBuild (individualCrateArgs // {
          inherit cargoArtifacts;
          pname = "wadm-types";
          cargoExtraArgs = "-p wadm-types";
          src = fileSetForCrate ./crates/wadm-types;
          doInstallCargoArtifacts = true;
        });
      in {
        checks = {
          # Build the crates as part of `nix flake check` for convenience
          inherit wadm wadm-client wadm-types;

          # Run clippy (and deny all warnings) on the workspace source,
          # again, reusing the dependency artifacts from above.
          #
          # Note that this is done as a separate derivation so that
          # we can block the CI if there are issues here, but not
          # prevent downstream consumers from building our crate by itself.
          workspace-clippy = craneLib.cargoClippy (commonArgs // {
            # TODO: This will get better if we do the dummy build step mentioned above
            cargoArtifacts = wadm-lib;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          });

          workspace-doc =
            craneLib.cargoDoc (commonArgs // { cargoArtifacts = wadm-lib; });

          # Check formatting
          workspace-fmt = craneLib.cargoFmt { inherit src; };

          # Audit dependencies
          workspace-audit = craneLib.cargoAudit { inherit src advisory-db; };

          # Audit licenses
          # my-workspace-deny = craneLib.cargoDeny {
          #   inherit src;
          # };

          runE2ETests = pkgs.runCommand "e2e-tests" {
            nativeBuildInputs = with pkgs;
              [
                nats-server
                # wasmcloud.wasmcloud
              ];
          } ''

            # TODO: run test setup with wash and figure out how to expose it in the flake
            touch $out
          '';
        };

        packages = {
          inherit wadm wadm-client wadm-types wadm-lib cargoArtifacts cargoLock;
          default = wadm;
        } // lib.optionalAttrs (!pkgs.stdenv.isDarwin) {
          workspace-llvm-coverage = craneLibLLvmTools.cargoLlvmCov
            (commonArgs // { cargoArtifacts = wadm-lib; });
        };

        apps = {
          wadm = flake-utils.lib.mkApp { drv = wadm; };
          default = flake-utils.lib.mkApp { drv = wadm; };
        };

        devShells.default = craneLib.devShell {
          # Inherit inputs from checks.
          checks = self.checks.${system};

          RUST_SRC_PATH =
            "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = [
            pkgs.nats-server
            pkgs.natscli
            pkgs.docker
            pkgs.git
            wasmcloud.outputs.packages.${system}.default
          ];
        };
      });
}
