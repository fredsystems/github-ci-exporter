{
  description = "Prometheus exporter for GitHub issues, pull requests, and Actions CI state";

  inputs = {
    precommit.url = "github:FredSystems/pre-commit-checks";
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      precommit,
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      inherit (nixpkgs) lib;
      systems = precommit.lib.supportedSystems;
      forAllSystems = lib.genAttrs systems;
    in
    {
      overlays.default = final: _prev: {
        github-ci-exporter = self.packages.${final.stdenv.hostPlatform.system}.github-ci-exporter;
      };

      nixosModules.default = import ./nix/nixos-module.nix { inherit self; };

      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          rustToolchain = pkgs.rust-bin.stable.latest.default;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };
        in
        rec {
          default = github-ci-exporter;
          github-ci-exporter = rustPlatform.buildRustPackage {
            pname = "github-ci-exporter";
            version = "0.1.0";
            src = lib.cleanSource ./.;
            cargoLock.lockFile = ./Cargo.lock;
            cargoBuildFlags = [
              "--package"
              "github-ci-exporter"
            ];
            # xtask is dev tooling and pulls in a much larger tree; it is not
            # part of the shipped artifact.
            cargoTestFlags = [
              "--package"
              "github-ci-exporter"
            ];
            nativeBuildInputs = [ pkgs.pkg-config ];
            meta = {
              description = "Prometheus exporter for GitHub issues, PRs, and Actions CI state";
              homepage = "https://github.com/fredsystems/github-ci-exporter";
              license = lib.licenses.mit;
              mainProgram = "github-ci-exporter";
            };
          };
        }
      );

      # NOTE: `nix flake check` will attempt to build `pre-commit-run`, which
      # runs the cargo-driven hooks (clippy, xtask-check) inside the Nix
      # sandbox. The sandbox has no network, so cargo cannot reach
      # index.crates.io and the build fails. This is inherent to the shared
      # ruleset and matches the other Rust repos using it.
      #
      # Validate with `pre-commit run --all-files` inside `nix develop`, which
      # is exactly what CI does. Use `nix flake check --no-build` for a
      # pure-evaluation check.
      checks = forAllSystems (
        system:
        let
          gitHooks = precommit.inputs.git-hooks;
          extraExcludes = [ "^Cargo\\.lock$" ];
          baseModule = precommit.lib.mkBaseCheck { inherit system extraExcludes; };
          rustModule = precommit.lib.mkRustCheck {
            inherit system extraExcludes;
            enableXtask = true;
            xtaskType = "pc";
          };
          mergedHooks = baseModule.hooks // rustModule.hooks;
          mergedExcludes = (baseModule.excludes or [ ]) ++ (rustModule.excludes or [ ]) ++ extraExcludes;
          run = gitHooks.lib.${system}.run {
            src = ./.;
            hooks = mergedHooks;
            excludes = mergedExcludes;
          };
          rustPassthru = rustModule.passthru or { };
        in
        {
          pre-commit-check = run // {
            passthru = {
              devPackages = (run.enabledPackages or [ ]) ++ (rustPassthru.devPackages or [ ]);
              libPath = rustPassthru.libPath or [ ];
              inherit (rustPassthru) rustToolchain;
            };
            shellHook = run.shellHook or "";
            enabledPackages = run.enabledPackages or [ ];
          };
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          chk = self.checks.${system}."pre-commit-check";
          corePkgs = chk.enabledPackages or [ ];
          extraDev = chk.passthru.devPackages or [ ];
          # Reuse the exact toolchain the hooks were built against. Mixing in
          # a separately-resolved one lets cargo and rustc drift by a patch
          # version, which surfaces as E0514 ("compiled by an incompatible
          # version of rustc").
          rustToolchain = chk.passthru.rustToolchain;
          ciRustTools = [
            pkgs.cargo-deny
            pkgs.cargo-machete
            pkgs.cargo-make
            pkgs.markdownlint-cli2
            pkgs.typos
          ]
          ++ lib.optionals pkgs.stdenv.isLinux [ pkgs.cargo-llvm-cov ];
          # `promtool` and `jq` are here because the alert rules and dashboards
          # consumed by the nixos repo are authored/validated against this
          # exporter's metric names.
          devOnlyTools = [
            pkgs.gh
            pkgs.jq
            pkgs.prometheus
          ];
          mkExporterShell =
            extraTools:
            pkgs.mkShell {
              buildInputs = extraDev ++ corePkgs ++ ciRustTools ++ extraTools;
              # Prepended rather than added to buildInputs so it wins over the
              # hook set's individually-packaged cargo/clippy/rustfmt.
              nativeBuildInputs = [ rustToolchain ];
              shellHook = ''
                export PATH="${rustToolchain}/bin:$PATH"
                ${chk.shellHook}
                alias pre-commit="pre-commit run --all-files"
              '';
            };
        in
        {
          default = mkExporterShell devOnlyTools;
          ci = mkExporterShell [ ];
        }
      );
    };
}
