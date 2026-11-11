//! Persisted "first observed behind" timestamps, keyed by an arbitrary
//! identity string.
//!
//! A tag-pinned flake input has no REST compare to measure its oldest missing
//! change from -- there is no "next" tag to diff against, only "the newest
//! one is not this one" -- so how long it has been behind has to be
//! remembered rather than recomputed. This module is deliberately generic
//! over the identity string rather than over [`super::lock::TrackedNode`]
//! directly, so subtask 3.6 (channel tarballs, which have the same problem
//! for a different reason: no compare between tarball snapshots) can reuse it
//! with its own identity shape.
//!
//! Persisted the same write-then-rename way `github::client`'s `ETag` cache
//! is, for the same reason: a crash mid-write must not leave a truncated file
//! that then gets discarded as corrupt.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};
use tracing::warn;

/// Disambiguates one process's successive temp-file writes from each other.
/// Paired with the process id in [`unique_tmp_path`], so two saves -- by
/// this process or another one sharing the same `state_dir` -- never target
/// the same temp file and risk one rename clobbering the other's
/// still-being-written contents.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Builds a temp path for one `persist` call that no other call, in this
/// process or any other, will ever reuse.
fn unique_tmp_path(path: &Path) -> PathBuf {
    let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("json.{}.{unique}.tmp", std::process::id()))
}

/// On-disk format version. A mismatch discards the file wholesale, the same
/// policy the `ETag` cache uses, since the one thing worse than losing every
/// first-seen timestamp is misinterpreting one.
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Default, Serialize, Deserialize)]
struct FileFormat {
    version: u32,
    entries: HashMap<String, i64>,
}

/// In-memory, persisted map of identity -> the unix timestamp it was first
/// observed behind.
#[derive(Debug, Default)]
pub struct FirstSeenStore {
    entries: HashMap<String, i64>,
}

impl FirstSeenStore {
    /// Loads a previously persisted store, tolerating a missing or corrupt
    /// file. Either yields an empty store with a warning (for a corrupt file)
    /// or silently (for a missing one, which is simply the normal first
    /// start) -- matching how [`crate::github::client::Client::with_cache_file`]
    /// treats its own on-disk cache.
    #[must_use]
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(raw) => match serde_json::from_str::<FileFormat>(&raw) {
                Ok(file) if file.version == FORMAT_VERSION => Self {
                    entries: file.entries,
                },
                Ok(file) => {
                    warn!(
                        version = file.version,
                        expected = FORMAT_VERSION,
                        "flake first-seen state was written by a different format version; starting empty"
                    );
                    Self::default()
                }
                Err(error) => {
                    warn!(%error, "flake first-seen state is corrupt; starting empty");
                    Self::default()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(error) => {
                warn!(path = %path.display(), %error, "could not read flake first-seen state; starting empty");
                Self::default()
            }
        }
    }

    /// Writes the store to disk, write-then-rename so a crash mid-write
    /// cannot leave a truncated file behind.
    ///
    /// The temp file's name is unique per call (see [`unique_tmp_path`]),
    /// not a fixed `<path>.tmp`: a fixed name would let two saves in flight
    /// at once -- a future concurrent writer, or two exporter instances
    /// pointed at the same `state_dir` -- race on the same temp file, with
    /// one rename landing an incomplete write from the other.
    ///
    /// # Errors
    /// Returns an error if the containing directory cannot be created or the
    /// file cannot be written.
    pub fn persist(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let encoded = serde_json::to_string(&FileFormat {
            version: FORMAT_VERSION,
            entries: self.entries.clone(),
        })?;
        let tmp = unique_tmp_path(path);
        std::fs::write(&tmp, encoded)?;
        std::fs::rename(&tmp, path)
    }

    /// Records `identity` as behind, returning the timestamp it was *first*
    /// observed behind -- `now` only the first time this is called for a
    /// given identity, the stored value on every call after.
    pub fn observe_behind(&mut self, identity: &str, now: i64) -> i64 {
        *self.entries.entry(identity.to_owned()).or_insert(now)
    }

    /// Clears `identity`'s entry, e.g. because the node it tracks is now
    /// up to date against the upstream the identity names.
    pub fn clear(&mut self, identity: &str) {
        self.entries.remove(identity);
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
    use super::*;

    #[test]
    fn observing_the_same_identity_twice_keeps_the_first_timestamp() {
        let mut store = FirstSeenStore::default();
        assert_eq!(store.observe_behind("a", 100), 100);
        assert_eq!(
            store.observe_behind("a", 200),
            100,
            "the clock must not reset while still behind"
        );
    }

    #[test]
    fn clearing_removes_the_entry() {
        let mut store = FirstSeenStore::default();
        store.observe_behind("a", 100);
        store.clear("a");
        assert_eq!(
            store.observe_behind("a", 200),
            200,
            "a cleared entry restarts"
        );
    }

    #[test]
    fn two_saves_do_not_share_a_temp_path() {
        let path = Path::new("/tmp/does-not-need-to-exist/flake-first-seen.json");
        let a = unique_tmp_path(path);
        let b = unique_tmp_path(path);
        assert_ne!(
            a, b,
            "concurrent saves must not be able to race on the same temp file"
        );
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("flake-first-seen.json");

        let mut store = FirstSeenStore::default();
        store.observe_behind("a", 100);
        store.persist(&path).expect("persist");

        let restored = FirstSeenStore::load(&path);
        assert_eq!(restored.entries.get("a"), Some(&100));
    }

    #[test]
    fn a_missing_file_loads_as_empty_without_panicking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("does-not-exist.json");
        let store = FirstSeenStore::load(&path);
        assert!(store.entries.is_empty());
    }

    #[test]
    fn a_corrupt_file_loads_as_empty_without_panicking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("flake-first-seen.json");
        std::fs::write(&path, "not json").expect("write");
        let store = FirstSeenStore::load(&path);
        assert!(store.entries.is_empty());
    }

    #[test]
    fn a_different_format_version_loads_as_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("flake-first-seen.json");
        std::fs::write(
            &path,
            serde_json::json!({"version": FORMAT_VERSION + 1, "entries": {"a": 1}}).to_string(),
        )
        .expect("write");
        let store = FirstSeenStore::load(&path);
        assert!(store.entries.is_empty());
    }
}
