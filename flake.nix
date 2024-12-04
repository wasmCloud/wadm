{
  nixConfig.extra-substituters =
    [ "https://wasmcloud.cachix.org" "https://crane.cachix.org" ];
  nixConfig.extra-trusted-public-keys = [
    "wasmcloud.cachix.org-1:9gRBzsKh+x2HbVVspreFg/6iFRiD4aOcUQfXVDl3hiM="
    "crane.cachix.org-1:8Scfpmn9w+hGdXH/Q9tTLiYAE/2dnJYRJP7kl80GuRk="
  ];

  description = "A flake for building and running wadm";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-24.11";

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
            # Additional darwin specific inputs can be set here if needed
          ];

          # Additional environment variables can be set directly here if needed
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

        # Build *just* the cargo dependencies (of the entire workspace), but we don't want to build
        # any of the other things in the crate to avoid rebuilding things in the dependencies when
        # we change workspace crate dependencies
        cargoArtifacts = let
          commonArgs' = removeAttrs commonArgs [ "src" ];

          # Get the manifest file for filtering
          rawManifestFile = builtins.fromTOML (builtins.readFile ./Cargo.toml);

          # Filter out the workspace members from manifest
          filteredManifestFile = with lib;
            let
              filterWadmAttrs =
                filterAttrs (name: _: !strings.hasPrefix "wadm" name);

              workspace = removeAttrs rawManifestFile.workspace [ "members" ];
            in rawManifestFile // {
              workspace = workspace // {
                dependencies = filterWadmAttrs workspace.dependencies;
                package = workspace.package // {
                  # pin version to avoid rebuilds on bumps
                  version = "0.0.0";
                };
              };

              dependencies = filterWadmAttrs rawManifestFile.dependencies;

              dev-dependencies =
                filterWadmAttrs rawManifestFile.dev-dependencies;

              build-dependencies =
                filterWadmAttrs rawManifestFile.build-dependencies;
            };

          cargoToml = craneLib.writeTOML "Cargo.toml" filteredManifestFile;

          dummySrc = craneLib.mkDummySrc {
            src = pkgs.runCommand "wadm-dummy-src" { } ''
              mkdir -p $out
              cp --recursive --no-preserve=mode,ownership ${src}/. -t $out
              cp ${cargoToml} $out/Cargo.toml
            '';
          };

          args = commonArgs' // {
            inherit cargoLock cargoToml cargoVendorDir dummySrc;

            cargoExtraArgs = ""; # disable `--locked` passed by default by crane
          };
        in craneLib.buildDepsOnly args;

        individualCrateArgs = commonArgs // {
          inherit (craneLib.crateNameFromCargoToml { inherit src; }) version;
          # TODO(thomastaylor312) We run unit tests here and e2e tests externally. The nextest step
          # wasn't letting me pass in the fileset
          doCheck = true;
        };

        fileSetForCrate = lib.fileset.toSource {
          root = ./.;
          fileset = lib.fileset.unions [
            ./Cargo.toml
            ./Cargo.lock
            ./tests
            ./oam
            (craneLib.fileset.commonCargoSources ./crates/wadm)
            (craneLib.fileset.commonCargoSources ./crates/wadm-client)
            (craneLib.fileset.commonCargoSources ./crates/wadm-types)
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
        # TODO(thomastaylor312) I tried using `doInstallCargoArtifacts` and passing in things to the
        # next derivations as the `cargoArtifacts`, but that ended up always building things twice
        # rather than caching. We should look into it more and see if there's a way to make it work.
        wadm-lib = craneLib.cargoBuild (individualCrateArgs // {
          inherit cargoArtifacts;
          pname = "wadm";
          cargoExtraArgs = "-p wadm";
          src = fileSetForCrate;
        });
        wadm = craneLib.buildPackage (individualCrateArgs // {
          inherit cargoArtifacts;
          pname = "wadm-cli";
          cargoExtraArgs = "--bin wadm";
          src = fileSetForCrate;
        });
        wadm-client = craneLib.cargoBuild (individualCrateArgs // {
          inherit cargoArtifacts;
          pname = "wadm-client";
          cargoExtraArgs = "-p wadm-client";
          src = fileSetForCrate;
        });
        wadm-types = craneLib.cargoBuild (individualCrateArgs // {
          inherit cargoArtifacts;
          pname = "wadm-types";
          cargoExtraArgs = "-p wadm-types";
          src = fileSetForCrate;
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
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          });

          workspace-doc =
            craneLib.cargoDoc (commonArgs // { inherit cargoArtifacts; });

          # Check formatting
          workspace-fmt = craneLib.cargoFmt { inherit src; };

          # Audit dependencies
          workspace-audit = craneLib.cargoAudit { inherit src advisory-db; };

          # Audit licenses
          # my-workspace-deny = craneLib.cargoDeny {
          #   inherit src;
          # };

          # TODO: the wadm e2e tests use docker compose and things like `wash up` to test things 
          # (which accesses network currently). We would need to fix those tests to do something
          # else to work properly. The low hanging fruit here would be to use the built artifact
          # in the e2e tests so we can output those binaries from the nix build and then just
          # run the tests from a separate repo. We could also do something like outputting the
          # prebuilt artifacts out into the current directory to save on build time. But that is
          # for later us to figure out
          runE2ETests = pkgs.runCommand "e2e-tests" {
            nativeBuildInputs = with pkgs;
              [
                nats-server
                # wasmcloud.wasmcloud
              ];
          } ''
            touch $out
          '';
        };

        packages = {
          inherit wadm wadm-client wadm-types wadm-lib;
          default = wadm;
        } // lib.optionalAttrs (!pkgs.stdenv.isDarwin) {
          workspace-llvm-coverage = craneLibLLvmTools.cargoLlvmCov
            (commonArgs // { inherit cargoArtifacts; });
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
