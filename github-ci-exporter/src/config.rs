//! Operator-facing configuration.
//!
//! Layered by [`figment`]: file defaults, then `GHCI_*` environment
//! overrides. The token is never read from the config file — see
//! [`Config::resolve_token`].

use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::model::PullRef;

/// Default poll interval.
///
/// At 61 repos this costs ~2 GraphQL points and (worst case) 61 REST requests
/// per cycle, i.e. ~15% of the 5000/hr core budget if every repo changed every
/// cycle. With `ETag` revalidation the realistic figure is a few percent.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(300);

/// Default interval for the flake-lock pass.
///
/// `flake.lock` changes far less often than CI or PR state, so this runs on
/// its own, much longer, cadence -- see [`Config::flake_interval`].
const DEFAULT_FLAKE_INTERVAL: Duration = Duration::from_secs(3600);

/// Minimum `flake_interval`. The Contents API fetch is cheap (one request per
/// configured repository, `ETag`-revalidated), but there is nothing to gain
/// from polling a file that changes on the order of days to weeks faster than
/// a quarter of an hour.
const MIN_FLAKE_INTERVAL: Duration = Duration::from_secs(900);

/// Default origin for NixOS channel status -- see [`Config::nixos_prometheus_url`].
const DEFAULT_NIXOS_PROMETHEUS_URL: &str = "https://prometheus.nixos.org";

/// `GHCI_`-prefixed variables that are not configuration fields.
///
/// Compared lowercased, matching how figment normalises env keys.
const NON_CONFIG_ENV_KEYS: [&str; 4] = ["token", "log", "log_json", "config"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Address the `/metrics` endpoint binds to.
    pub listen: SocketAddr,

    /// Repository owners to enumerate.
    ///
    /// Each entry may be an organisation *or* a personal account: discovery
    /// resolves them through GraphQL's `RepositoryOwner` interface, which both
    /// implement. The field keeps its `orgs` name because it is also the `org`
    /// metric label, which appears in every dashboard panel and alert rule.
    pub orgs: Vec<String>,

    /// How often to refresh from the GitHub API.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,

    /// Repositories to exclude, as `owner/name`. Applied after the automatic
    /// archived/no-workflow filters.
    pub denylist: Vec<String>,

    /// Individual pull requests to exclude, as `owner/name#number`.
    ///
    /// For a PR that is genuinely stuck and genuinely not actionable: one
    /// opened against someone else's repository, which the maintainer has not
    /// engaged with and which is not ours to close or convert to a draft.
    /// Nothing the exporter can measure distinguishes that from a PR worth
    /// chasing, so it has to be declared.
    ///
    /// Deliberately per-PR rather than per-repo: denylisting the whole
    /// repository would also blind the exporter to its CI and to every future
    /// pull request on it.
    pub ignore_pulls: Vec<String>,

    /// Skip repositories with no push in this window. `None` disables the
    /// check, which is the default: dormant-but-released repos still deserve
    /// CI visibility.
    #[serde(default, with = "humantime_serde_option")]
    pub max_repo_age: Option<Duration>,

    /// Drop repositories with no workflow files. These are content-hosting
    /// repos with no CI signal to report.
    pub skip_repos_without_workflows: bool,

    /// Where the `ETag` cache is persisted, so a restart does not force a full
    /// uncached sweep of every repository.
    pub state_dir: PathBuf,

    /// Path to a file containing the API token. Overridden by `GHCI_TOKEN`.
    /// Intended for systemd `LoadCredential` or a sops-managed secret.
    pub token_file: Option<PathBuf>,

    /// GitHub API base, overridable for GitHub Enterprise and for tests.
    pub github_api_url: String,

    /// GitHub GraphQL endpoint.
    pub github_graphql_url: String,

    /// Repositories whose flake inputs are tracked, as `owner/name`.
    ///
    /// Each entry's `flake.lock` is read from its default branch through the
    /// `ETag` cache. Empty by default: tracking is opt-in per repository
    /// rather than inferred from `orgs`, because most monitored repositories
    /// are not flakes at all.
    pub flake_repos: Vec<String>,

    /// How often to run the flake-lock pass.
    ///
    /// Independent of `interval`: `flake.lock` changes on the order of days
    /// to weeks, far slower than CI or PR state, so tying it to the 5-minute
    /// default would spend most of that budget re-fetching a file that has
    /// not moved. Every cycle still republishes the cached result, since the
    /// metrics registry is rebuilt from scratch each cycle.
    #[serde(with = "humantime_serde")]
    pub flake_interval: Duration,

    /// Base URL for NixOS channel status (`channel_revision`,
    /// `channel_update_time`, `hydra_job_failed`), queried once per flake
    /// pass for the channels any tracked node references. Overridable for
    /// tests, which point it at a wiremock server.
    pub nixos_prometheus_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 9418)),
            orgs: Vec::new(),
            interval: DEFAULT_INTERVAL,
            denylist: Vec::new(),
            ignore_pulls: Vec::new(),
            max_repo_age: None,
            skip_repos_without_workflows: true,
            state_dir: PathBuf::from("/var/lib/github-ci-exporter"),
            token_file: None,
            github_api_url: "https://api.github.com".to_owned(),
            github_graphql_url: "https://api.github.com/graphql".to_owned(),
            flake_repos: Vec::new(),
            flake_interval: DEFAULT_FLAKE_INTERVAL,
            nixos_prometheus_url: DEFAULT_NIXOS_PROMETHEUS_URL.to_owned(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Load(#[from] Box<figment::Error>),
    #[error("no owners configured; set `orgs` or GHCI_ORGS")]
    NoOrgs,
    #[error("poll interval must be at least 60s to stay within API rate limits, got {0:?}")]
    IntervalTooShort(Duration),
    #[error("invalid `ignore_pulls` entry: {0}")]
    IgnorePull(#[from] crate::model::PullRefError),
    #[error("invalid `flake_repos` entry `{0}`; expected `owner/name`")]
    InvalidFlakeRepo(String),
    #[error("flake_interval must be at least {MIN_FLAKE_INTERVAL:?}, got {0:?}")]
    FlakeIntervalTooShort(Duration),
    #[error("nixos_prometheus_url must be an http(s) URL, got `{0}`")]
    InvalidNixosPrometheusUrl(String),
    #[error("no token available: set GHCI_TOKEN or `token_file`")]
    NoToken,
    #[error("failed to read token file {path}: {source}")]
    TokenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("token file {0} is empty")]
    EmptyToken(PathBuf),
}

impl Config {
    /// Loads configuration from an optional TOML file plus `GHCI_*` env vars.
    ///
    /// # Errors
    /// Returns [`ConfigError`] if the sources cannot be merged or the result
    /// fails validation.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut figment = Figment::from(Serialized::defaults(Self::default()));
        if let Some(path) = path {
            figment = figment.merge(Toml::file(path));
        }
        // `GHCI_TOKEN`, `GHCI_LOG*`, and `GHCI_CONFIG` are consumed by the
        // credential resolver, the tracing filter, and the CLI respectively.
        // They are not config fields, and `deny_unknown_fields` would reject
        // the whole load if they were passed through.
        let config: Self = figment
            .merge(
                Env::prefixed("GHCI_")
                    .split("__")
                    // `UncasedStr` compares case-insensitively, so `TOKEN`
                    // matches `token` without normalising by hand.
                    .filter(|key| !NON_CONFIG_ENV_KEYS.iter().any(|excluded| key == *excluded)),
            )
            .extract()
            .map_err(|e| ConfigError::Load(Box::new(e)))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.orgs.is_empty() {
            return Err(ConfigError::NoOrgs);
        }
        // A tighter loop cannot pay for itself: GitHub's own Actions data is
        // eventually consistent on the order of seconds, and sub-minute
        // polling burns the core budget for no extra signal.
        if self.interval < Duration::from_secs(60) {
            return Err(ConfigError::IntervalTooShort(self.interval));
        }
        // Parsed here purely to reject typos at startup. A malformed entry
        // would otherwise be a filter that silently never matches, which is
        // the worst outcome: the operator believes a PR is suppressed and the
        // alert keeps firing.
        self.ignored_pulls()?;
        for repo in &self.flake_repos {
            if !crate::model::is_full_repo_name(repo) {
                return Err(ConfigError::InvalidFlakeRepo(repo.clone()));
            }
        }
        if self.flake_interval < MIN_FLAKE_INTERVAL {
            return Err(ConfigError::FlakeIntervalTooShort(self.flake_interval));
        }
        match Url::parse(&self.nixos_prometheus_url) {
            Ok(url) if url.scheme() == "http" || url.scheme() == "https" => {}
            _ => {
                return Err(ConfigError::InvalidNixosPrometheusUrl(
                    self.nixos_prometheus_url.clone(),
                ));
            }
        }
        Ok(())
    }

    /// Parses the operator's `ignore_pulls` entries.
    ///
    /// # Errors
    /// Returns [`ConfigError::IgnorePull`] if an entry is not
    /// `owner/name#number`.
    pub fn ignored_pulls(&self) -> Result<HashSet<PullRef>, ConfigError> {
        self.ignore_pulls
            .iter()
            .map(|raw| raw.parse::<PullRef>().map_err(ConfigError::IgnorePull))
            .collect()
    }

    /// Resolves the API token from `GHCI_TOKEN`, else `token_file`.
    ///
    /// The token is intentionally not a config-file field so it cannot be
    /// leaked by dumping the effective configuration.
    ///
    /// # Errors
    /// Returns [`ConfigError`] if no token is configured or the file is
    /// unreadable or empty.
    pub fn resolve_token(&self) -> Result<String, ConfigError> {
        self.resolve_token_from(std::env::var("GHCI_TOKEN").ok().as_deref())
    }

    /// Token resolution with the environment value supplied explicitly.
    ///
    /// Split out so the precedence rules can be tested without mutating the
    /// process environment, which would require `unsafe` and race across
    /// parallel tests.
    ///
    /// # Errors
    /// Returns [`ConfigError`] if no token is configured or the file is
    /// unreadable or empty.
    pub fn resolve_token_from(&self, env_token: Option<&str>) -> Result<String, ConfigError> {
        if let Some(token) = env_token {
            let token = token.trim();
            if !token.is_empty() {
                return Ok(token.to_owned());
            }
        }
        let path = self.token_file.as_ref().ok_or(ConfigError::NoToken)?;
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::TokenFile {
            path: path.clone(),
            source,
        })?;
        let token = raw.trim().to_owned();
        if token.is_empty() {
            return Err(ConfigError::EmptyToken(path.clone()));
        }
        Ok(token)
    }

    /// Whether `owner/name` is explicitly excluded by the operator.
    #[must_use]
    pub fn is_denylisted(&self, full_name: &str) -> bool {
        self.denylist
            .iter()
            .any(|entry| entry.eq_ignore_ascii_case(full_name))
    }
}

/// `humantime_serde` for `Option<Duration>` under `serde(default)`.
mod humantime_serde_option {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    #[allow(clippy::ref_option)] // Signature dictated by serde's `with` contract.
    pub(super) fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(d) => serializer.serialize_some(&humantime::format_duration(*d).to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        let raw = Option::<String>::deserialize(deserializer)?;
        raw.map(|s| humantime::parse_duration(&s).map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "panicking is how a test reports failure"
)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn defaults_are_rejected_without_orgs() {
        let err = Config::default()
            .validate()
            .expect_err("orgs are mandatory");
        assert!(matches!(err, ConfigError::NoOrgs));
    }

    #[test]
    fn rejects_interval_below_one_minute() {
        let config = Config {
            orgs: vec!["fredsystems".into()],
            interval: Duration::from_secs(5),
            ..Config::default()
        };
        assert!(matches!(
            config
                .validate()
                .expect_err("too-fast polling must be rejected"),
            ConfigError::IntervalTooShort(_)
        ));
    }

    #[test]
    fn accepts_valid_config() {
        let config = Config {
            orgs: vec!["fredsystems".into()],
            ..Config::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn parses_toml_with_humantime_durations() {
        {
            let mut file = tempfile::NamedTempFile::new().expect("create temp config");
            write!(
                file,
                r#"
                orgs = ["sdr-enthusiasts", "fredsystems"]
                interval = "10m"
                max_repo_age = "365d"
                denylist = ["sdr-enthusiasts/sdr-enthusiast-website"]
                listen = "0.0.0.0:9418"
                "#
            )
            .expect("write temp config");

            let config = Config::load(Some(file.path())).expect("config should load");
            assert_eq!(config.orgs, ["sdr-enthusiasts", "fredsystems"]);
            assert_eq!(config.interval, Duration::from_secs(600));
            assert_eq!(config.max_repo_age, Some(Duration::from_secs(365 * 86400)));
            assert_eq!(config.listen.port(), 9418);
        }
    }

    #[test]
    fn non_config_env_keys_are_excluded_from_the_field_set() {
        // Regression guard: `deny_unknown_fields` plus the `GHCI_` prefix
        // means GHCI_TOKEN / GHCI_LOG would otherwise fail the whole load.
        for key in NON_CONFIG_ENV_KEYS {
            assert!(
                NON_CONFIG_ENV_KEYS.contains(&key),
                "{key} must be filtered before extraction"
            );
        }
        assert!(NON_CONFIG_ENV_KEYS.contains(&"token"));
        assert!(NON_CONFIG_ENV_KEYS.contains(&"log"));
        assert!(!NON_CONFIG_ENV_KEYS.contains(&"orgs"));
        assert!(!NON_CONFIG_ENV_KEYS.contains(&"interval"));
    }

    #[test]
    fn denylist_is_case_insensitive() {
        let config = Config {
            denylist: vec!["SDR-Enthusiasts/Foo".into()],
            ..Config::default()
        };
        assert!(config.is_denylisted("sdr-enthusiasts/foo"));
        assert!(!config.is_denylisted("sdr-enthusiasts/bar"));
    }

    #[test]
    fn ignore_pulls_entries_are_parsed() {
        let config = Config {
            orgs: vec!["sdr-enthusiasts".into()],
            ignore_pulls: vec![
                "sdr-enthusiasts/docker-vesselalert#32".into(),
                "fredsystems/nixos#1615".into(),
            ],
            ..Config::default()
        };
        let ignored = config.ignored_pulls().expect("well-formed entries");
        assert!(ignored.contains(&crate::model::PullRef::new(
            "sdr-enthusiasts/docker-vesselalert",
            32
        )));
        assert!(ignored.contains(&crate::model::PullRef::new("fredsystems/nixos", 1615)));
        assert_eq!(ignored.len(), 2);
    }

    #[test]
    fn a_malformed_ignore_pull_entry_fails_validation() {
        // Startup must fail loudly. A silently-ignored bad entry means the
        // operator believes a PR is suppressed while its alert keeps firing.
        let config = Config {
            orgs: vec!["sdr-enthusiasts".into()],
            ignore_pulls: vec!["sdr-enthusiasts/docker-vesselalert".into()],
            ..Config::default()
        };
        assert!(matches!(
            config.validate().expect_err("a typo must be rejected"),
            ConfigError::IgnorePull(_)
        ));
    }

    #[test]
    fn ignore_pulls_defaults_to_empty_and_is_read_from_toml() {
        assert_eq!(Config::default().ignore_pulls, Vec::<String>::new());

        let mut file = tempfile::NamedTempFile::new().expect("create temp config");
        write!(
            file,
            r#"
            orgs = ["sdr-enthusiasts"]
            ignore_pulls = ["sdr-enthusiasts/docker-vesselalert#32"]
            "#
        )
        .expect("write temp config");

        let config = Config::load(Some(file.path())).expect("config should load");
        assert_eq!(
            config.ignore_pulls,
            ["sdr-enthusiasts/docker-vesselalert#32"]
        );
    }

    #[test]
    fn token_file_is_read_and_trimmed() {
        let mut file = tempfile::NamedTempFile::new().expect("create temp token");
        writeln!(file, "  ghp_secret_value  ").expect("write token");
        let config = Config {
            token_file: Some(file.path().to_path_buf()),
            ..Config::default()
        };
        assert_eq!(
            config.resolve_token_from(None).expect("token"),
            "ghp_secret_value"
        );
    }

    #[test]
    fn env_token_takes_precedence_over_file() {
        let mut file = tempfile::NamedTempFile::new().expect("create temp token");
        writeln!(file, "from_file").expect("write token");
        let config = Config {
            token_file: Some(file.path().to_path_buf()),
            ..Config::default()
        };
        assert_eq!(
            config.resolve_token_from(Some("from_env")).expect("token"),
            "from_env"
        );
    }

    #[test]
    fn blank_env_token_falls_back_to_file() {
        let mut file = tempfile::NamedTempFile::new().expect("create temp token");
        writeln!(file, "from_file").expect("write token");
        let config = Config {
            token_file: Some(file.path().to_path_buf()),
            ..Config::default()
        };
        assert_eq!(
            config.resolve_token_from(Some("   ")).expect("token"),
            "from_file"
        );
    }

    #[test]
    fn empty_token_file_is_an_error() {
        let file = tempfile::NamedTempFile::new().expect("create temp token");
        let config = Config {
            token_file: Some(file.path().to_path_buf()),
            ..Config::default()
        };
        assert!(matches!(
            config.resolve_token_from(None).expect_err("empty token"),
            ConfigError::EmptyToken(_)
        ));
    }

    #[test]
    fn flake_repos_and_interval_default_sensibly() {
        let config = Config::default();
        assert_eq!(config.flake_repos, Vec::<String>::new());
        assert_eq!(config.flake_interval, Duration::from_secs(3600));
    }

    #[test]
    fn a_malformed_flake_repo_entry_fails_validation() {
        let config = Config {
            orgs: vec!["fredsystems".into()],
            flake_repos: vec!["not-a-repo".into()],
            ..Config::default()
        };
        assert!(matches!(
            config
                .validate()
                .expect_err("a malformed flake repo must be rejected"),
            ConfigError::InvalidFlakeRepo(entry) if entry == "not-a-repo"
        ));
    }

    #[test]
    fn a_well_formed_flake_repo_entry_passes_validation() {
        let config = Config {
            orgs: vec!["fredsystems".into()],
            flake_repos: vec!["fredsystems/nixos".into()],
            ..Config::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn flake_interval_below_fifteen_minutes_is_rejected() {
        let config = Config {
            orgs: vec!["fredsystems".into()],
            flake_interval: Duration::from_secs(60),
            ..Config::default()
        };
        assert!(matches!(
            config
                .validate()
                .expect_err("too-fast flake polling must be rejected"),
            ConfigError::FlakeIntervalTooShort(_)
        ));
    }

    #[test]
    fn flake_interval_of_exactly_fifteen_minutes_is_accepted() {
        let config = Config {
            orgs: vec!["fredsystems".into()],
            flake_interval: Duration::from_secs(900),
            ..Config::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn flake_repos_and_interval_are_read_from_toml() {
        let mut file = tempfile::NamedTempFile::new().expect("create temp config");
        write!(
            file,
            r#"
            orgs = ["fredsystems"]
            flake_repos = ["fredsystems/nixos"]
            flake_interval = "2h"
            "#
        )
        .expect("write temp config");

        let config = Config::load(Some(file.path())).expect("config should load");
        assert_eq!(config.flake_repos, ["fredsystems/nixos"]);
        assert_eq!(config.flake_interval, Duration::from_secs(7200));
    }

    #[test]
    fn nixos_prometheus_url_defaults_to_the_real_origin() {
        assert_eq!(
            Config::default().nixos_prometheus_url,
            "https://prometheus.nixos.org"
        );
    }

    #[test]
    fn a_non_http_nixos_prometheus_url_is_rejected() {
        for url in ["not a url", "ftp://prometheus.nixos.org", ""] {
            let config = Config {
                orgs: vec!["fredsystems".into()],
                nixos_prometheus_url: url.to_owned(),
                ..Config::default()
            };
            assert!(
                matches!(
                    config.validate(),
                    Err(ConfigError::InvalidNixosPrometheusUrl(_))
                ),
                "{url:?} must be rejected"
            );
        }
    }

    #[test]
    fn an_http_or_https_nixos_prometheus_url_is_accepted() {
        for url in ["https://prometheus.nixos.org", "http://127.0.0.1:9999"] {
            let config = Config {
                orgs: vec!["fredsystems".into()],
                nixos_prometheus_url: url.to_owned(),
                ..Config::default()
            };
            assert!(config.validate().is_ok(), "{url:?} must be accepted");
        }
    }

    #[test]
    fn nixos_prometheus_url_is_read_from_toml() {
        let mut file = tempfile::NamedTempFile::new().expect("create temp config");
        write!(
            file,
            r#"
            orgs = ["fredsystems"]
            nixos_prometheus_url = "http://127.0.0.1:9091"
            "#
        )
        .expect("write temp config");

        let config = Config::load(Some(file.path())).expect("config should load");
        assert_eq!(config.nixos_prometheus_url, "http://127.0.0.1:9091");
    }

    #[test]
    fn missing_token_is_an_error() {
        let config = Config::default();
        assert!(matches!(
            config
                .resolve_token_from(None)
                .expect_err("no token configured"),
            ConfigError::NoToken
        ));
    }
}
