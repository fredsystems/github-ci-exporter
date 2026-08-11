{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.github-ci-exporter;
  inherit (lib)
    mkEnableOption
    mkIf
    mkOption
    types
    ;

  settings = {
    inherit (cfg)
      orgs
      denylist
      listen
      interval
      ;
    ignore_pulls = cfg.ignorePulls;
    skip_repos_without_workflows = cfg.skipReposWithoutWorkflows;
    state_dir = "/var/lib/github-ci-exporter";
  }
  // lib.optionalAttrs (cfg.maxRepoAge != null) { max_repo_age = cfg.maxRepoAge; };

  configFile = (pkgs.formats.toml { }).generate "github-ci-exporter.toml" settings;
in
{
  options.services.github-ci-exporter = {
    enable = mkEnableOption "the GitHub CI Prometheus exporter";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.github-ci-exporter;
      defaultText = lib.literalMD "the flake's `github-ci-exporter` package";
      description = "Package providing the exporter binary.";
    };

    orgs = mkOption {
      type = types.listOf types.str;
      default = [ ];
      example = [
        "sdr-enthusiasts"
        "fredsystems"
        "fredclausen"
      ];
      description = ''
        GitHub repository owners to monitor. Each entry may be an
        organisation *or* a personal account; discovery resolves both
        through GraphQL's `RepositoryOwner` interface.

        The option keeps the name `orgs` because it is also the `org`
        metric label, which appears in every dashboard panel and alert
        rule. Read it as "owners".

        Note that a personal account's private repositories are only
        discovered if the token can see them, and that forks are more
        common on a personal account than in an organisation. Most forks
        self-filter as `no_workflows`; one with workflows still enabled
        needs a `denylist` entry.
      '';
    };

    denylist = mkOption {
      type = types.listOf types.str;
      default = [ ];
      example = [ "sdr-enthusiasts/sdr-enthusiast-website" ];
      description = ''
        Repositories to exclude, as `owner/name`. Applied on top of the
        automatic filters, which already drop archived repositories and
        repositories with no workflow files.
      '';
    };

    ignorePulls = mkOption {
      type = types.listOf types.str;
      default = [ ];
      example = [ "sdr-enthusiasts/docker-vesselalert#32" ];
      description = ''
        Individual pull requests to suppress, as `owner/name#number`.

        For a pull request that is genuinely stuck and genuinely not
        actionable: one opened against a repository you do not own, which the
        maintainer has not engaged with and which is not yours to close or
        convert to a draft. Nothing the exporter can measure distinguishes
        that from a pull request worth chasing, so it has to be declared.

        Suppressed pull requests are dropped from the per-PR series
        (`github_pull_needs_attention`, `github_pull_ready_to_merge`,
        `github_pull_created_timestamp_seconds`), so no alert can fire on
        them. They still count towards `github_repo_pulls_open`, and the
        number suppressed per repository is published as
        `github_repo_pulls_ignored` so the difference is explainable.

        Prefer this over adding the repository to `denylist`, which would also
        hide its CI state and every future pull request on it.

        A malformed entry is a startup error rather than a filter that
        silently never matches.
      '';
    };

    listen = mkOption {
      type = types.str;
      default = "127.0.0.1:9418";
      description = ''
        Address the `/metrics` endpoint binds to. Defaults to loopback; the
        exporter holds a GitHub token and should not be reachable off-host
        without a deliberate decision.
      '';
    };

    interval = mkOption {
      type = types.str;
      default = "5m";
      description = ''
        Poll interval. Must be at least 60s; the exporter refuses to start
        below that to stay within the API rate limits.
      '';
    };

    maxRepoAge = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "365d";
      description = ''
        Skip repositories with no push within this window. Null monitors
        every repository regardless of age.
      '';
    };

    skipReposWithoutWorkflows = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Drop repositories that define no workflow files. These are
        content-hosting repositories with no CI signal to report.
      '';
    };

    tokenFile = mkOption {
      type = types.path;
      example = "/run/secrets/github_exporter_token";
      description = ''
        File containing a GitHub token. A fine-grained, read-only PAT with
        Metadata, Issues, Pull requests, and Actions read access is
        sufficient.
      '';
    };

    openFirewall = mkOption {
      type = types.bool;
      default = false;
      description = "Open the exporter's listen port in the firewall.";
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.orgs != [ ];
        message = "services.github-ci-exporter.orgs must list at least one owner.";
      }
    ];

    systemd.services.github-ci-exporter = {
      description = "GitHub CI Prometheus exporter";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      serviceConfig = {
        ExecStart = "${lib.getExe cfg.package} --config ${configFile}";
        Restart = "on-failure";
        RestartSec = "30s";

        DynamicUser = true;
        StateDirectory = "github-ci-exporter";
        # The token is read via LoadCredential rather than a bind mount so it
        # works with DynamicUser, which has no stable uid to chown to.
        LoadCredential = [ "token:${cfg.tokenFile}" ];

        # Hardening. The exporter makes outbound HTTPS calls and writes only
        # its own state directory.
        AmbientCapabilities = [ ];
        CapabilityBoundingSet = [ ];
        DevicePolicy = "closed";
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        NoNewPrivileges = true;
        PrivateDevices = true;
        PrivateTmp = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectProc = "invisible";
        ProtectSystem = "strict";
        RemoveIPC = true;
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        RestrictNamespaces = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        SystemCallArchitectures = "native";
        SystemCallFilter = [
          "@system-service"
          "~@privileged"
          "~@resources"
        ];
        UMask = "0077";
      };

      environment = {
        GHCI_TOKEN_FILE = "%d/token";
        GHCI_LOG = "github_ci_exporter=info,warn";
      };
    };

    networking.firewall.allowedTCPPorts = mkIf cfg.openFirewall [
      (lib.toInt (lib.last (lib.splitString ":" cfg.listen)))
    ];
  };
}
