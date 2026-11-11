# github-ci-exporter

> Prometheus exporter for GitHub issues, pull requests, and Actions CI state
> across whole organisations and personal accounts.

[![CI](https://github.com/fredsystems/github-ci-exporter/actions/workflows/ci.yml/badge.svg)](https://github.com/fredsystems/github-ci-exporter/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

---

## What it does

Point it at one or more GitHub repository owners. It discovers the
repositories, filters out the ones with nothing to say, and exports:

- open issues and pull requests, split by **human vs bot** author
- the latest Actions run per workflow on each default branch
- the age of every open pull request
- its own health, including both GitHub API rate-limit budgets

## Why it is built this way

Three constraints shaped the design, each verified against the live API:

**The search API is unusable for polling.** It allows 30 requests per hour.
Repository discovery, issues, and pull requests therefore go through GraphQL,
where ~60 repositories cost about 2 points of a 5000/hour budget.

**Commit status rollups do not reflect CI here.** Most workflows in the target
organisations are triggered by `schedule` or `workflow_dispatch` and are never
attached to the head commit; `statusCheckRollup` was null for 30 of 31 sampled
repositories. Actions state is read from `/actions/runs` instead, one request
per repository, reduced to the newest run per workflow client-side.

**Bot filtering must not rely on a login list.** Author classification uses
GraphQL's `author.__typename`, which reports `Bot` for GitHub Apps. New
automation is classified correctly without a configuration change.

## The runs listing is only eventually consistent

Every query parameter that *filters* `/actions/runs` — `branch=`, `event=` — is
answered from an index that lags the run history. It intermittently returns a
**partial, stale, or empty** result set, as a well-formed `200` with a matching
`total_count`, so nothing in the response distinguishes it from a complete one.

Measured against `fredsystems/freminal`'s `nightly.yml`, whose true newest run
was known independently, 20 samples per query shape:

| Query shape                        | Correct | Wrong answers           |
| ---------------------------------- | ------- | ----------------------- |
| `?branch=main`                     | 16/20   | runs up to 6 days stale |
| `?event=schedule`                  | 13/20   | stale runs, and 3 empty |
| `?per_page=100` (no filter)        | 20/20   | —                       |
| `/workflows/{id}/runs` (no filter) | 20/20   | —                       |

On `sdr-enthusiasts/docker-beast-splitter`, whose `?branch=main` `total_count`
is truly 264, fifty samples returned 0, 23, 30, 92, 111, 133, 135, 190, 192,
206 and 264 — roughly one in ten empty. The unfiltered listing returned the
same complete answer on every sample of every repository tried.

This was not academic. A truncated page that drops a workflow's recent runs but
keeps an older failing one makes that failure the newest run the reducer can
see. `update-flakes` on `docker-beast-splitter` ran green on main on 07-26,
08-02, 08-09 and 08-16; a truncated page kept only the 07-12 failure, and at 38
days that is inside the 90-day staleness horizon, so it was not even masked as
`stale`. It published `conclusion="failure"` for a green workflow and paged. The
same truncation regresses `workflow_run_timestamp_seconds`, which fires the
staleness alert, and a page that omits a workflow entirely makes its series
vanish for a cycle — the "workflows appear for a round or two and then
disappear" shape.

Three mechanisms address it:

- **No server-side filters.** The runs listing is fetched unfiltered and the
  branch and event are selected client-side, against data the API reports
  consistently.
- **A per-workflow top-up.** Without `branch=`, the shared page is diluted by
  pull-request runs and no longer reaches back far enough on a busy repository —
  one page of `fredsystems/nixos` spans about a day and speaks for 5 of its 14
  workflows. Any workflow the shared page did not cover is fetched from
  `/actions/workflows/{id}/runs`, whose history is not diluted by its siblings.
  Workflows GitHub never runs against the default branch are excluded, which is
  what keeps this affordable.
- **Reconciliation across cycles.** Run history is append-only: a workflow's
  newest run can move forward or stay put, but it cannot move backwards, and it
  cannot vanish while the workflow file exists. A response violating either is
  not news, it is evidence of truncation, so the previous observation is kept.
  This turns every truncation into one cycle of staleness instead of a
  fabricated regression that pages. Ties go to the fresh observation, because a
  re-run keeps its original `created_at` while its conclusion changes.

## Rate-limit behaviour

Every request carries an `If-None-Match`. A `304 Not Modified` is not charged
against the rate limit, which is what makes a steady-state cycle nearly free
regardless of how many requests it issues.

A cycle's REST cost is, per repository, the workflow list plus one shared runs
page; and per workflow, the definition lookup plus at most one top-up runs
request. For the current fleet — 61 repositories, 239 workflows — that is a
worst case of a few hundred requests against a 5000/hour allowance, of which
only the ones whose content actually changed are charged. The pre-flight
estimate in `collector.rs` prices exactly these terms; it deliberately
overestimates, since the cost of guessing high is an early skip rather than a
wrong answer.

The `core` (REST) and `graphql` pools are tracked separately, since each has
its own independent 5000/hour allowance.

A reserve (250 requests by default) is withheld from each pool. Before a cycle
begins the exporter estimates its cost and, if the budget cannot cover it,
skips the cycle entirely rather than running out partway through and leaving
a half-rebuilt metric set. A skipped cycle is reported by
`github_exporter_budget_exhausted` and `github_exporter_cycles_bypassed_total`,
and previous values are retained so a transient shortage does not look like
CI disappearing.

The flake-lock pass (below) is priced and gated separately from CI. A cycle
is only bypassed outright if it cannot afford CI on its own; if CI fits but
CI plus a due flake pass does not, the cycle still runs, just without the
flake pass for that one cycle -- it stays due, so the next cycle retries it,
and whatever was last observed is still republished.

Because a `304` returns no body, the cache must be able to reproduce the value.
It stores a **projection** of each response rather than the raw payload; the
Actions runs endpoint alone returns ~1.5 MB per repository, which measured at
67 MB of cache before projection and 141 KB after.

That makes the cache's contents meaningful only to the code that wrote them,
which is a trap worth stating plainly: **when a projection's shape changes,
every persisted entry becomes undecodable while its `ETag` stays valid.** GitHub
then answers `304` forever and hands back a validator the exporter cannot use.
This is not hypothetical — it is how the trigger filter below came to be inert
in production for as long as the cache file survived, silently reverting to
"accept any event" on every cycle.

Two mechanisms close it, and the redundancy is deliberate:

- The cache file carries a **format version**. A mismatch discards it whole, so
  a projection change costs exactly one cold sweep.
- An undecodable entry is treated as a **cache miss**, not an error: the `ETag`
  is dropped and the resource refetched unconditionally. This recovers from a
  forgotten version bump, which is the failure mode that actually happened.

Bump `CACHE_FORMAT_VERSION` in `github-ci-exporter/src/github/client.rs`
whenever a cached projection's serialised shape changes.

## Owners: organisations and personal accounts

An entry in `orgs` may be either. Discovery resolves it through GraphQL's
`repositoryOwner(login:)`, the `RepositoryOwner` interface that both `User` and
`Organization` implement, so one query covers both with no branching. The
org-only `organization(login:)` root field resolves to `null` for a personal
account, which made a user entry fail discovery every cycle.

The field keeps the name `orgs`, and the `org` metric label is unchanged, because
that label appears in every dashboard panel and alert rule. Read both as
"owner".

Discovery passes `ownerAffiliations: [OWNER]`, which matters more than it
looks. A `User`'s `repositories` connection includes `COLLABORATOR` by default,
so it returns repositories belonging to *other* accounts — measured on one
personal account, roughly 40% of the default result was unowned, and included
repositories already monitored under their own owner. Each node's owner is also
read back from the response rather than assumed to be the login that was
queried, because `nodes.name` is bare: pairing it with the queried login
reported `fredsystems/nixos` as `fredclausen/nixos` — a duplicate series, under
an `org` label that does not exist, for a repository that already had one.

The invariant to check a discovery change against is that the result matches
`gh repo list <owner>` in both directions.

Two further things follow from pointing this at a personal account:

- **Private repositories need a token that can see them.** A `public_repo`
  classic PAT — or a fine-grained token without private access — silently
  discovers only the public ones.
- **Forks are far more common than in an org.** Most self-filter, because a
  fork usually has no enabled workflows and is dropped as `no_workflows`. A
  fork you once ran CI on does not, and its scheduled workflows will read as
  stale. Those are `denylist` entries.

## Repository selection

Repositories are discovered automatically and dropped when they are:

| Reason         | Meaning                                                     |
| -------------- | ----------------------------------------------------------- |
| `archived`     | Archived upstream. These still return open issues and PRs from the search API, so they are filtered during discovery. |
| `no_workflows` | No files under `.github/workflows`; content-hosting repos with no CI. |
| `denylisted`   | Listed in `denylist`.                                        |
| `inactive`     | No push within `max_repo_age`, when that is set.             |

Counts are exported as `github_repos_skipped`, so an unexpectedly small
dashboard is explainable without reading logs.

Fork status is deliberately **not** a filter: actively-maintained
infrastructure forks and dormant upstream forks are indistinguishable by that
flag alone.

## The repository index (`/repos.json`)

Every repository the exporter *discovers* is also served as JSON, separately
from the metrics:

```json
{
  "generated_at": "2026-08-19T12:00:00Z",
  "repos": [
    {
      "owner": "fredsystems",
      "name": "nixos",
      "description": "NixOS flake for the fleet",
      "archived": false,
      "pushed_at": "2026-08-19T09:14:02Z"
    }
  ]
}
```

This is the **pre-filter** discovery result, not the monitored set. The table
above exists to answer "what CI should I be watching?", and every one of its
exclusions is wrong for a lookup: a documentation repository with no workflows
is exactly the sort of thing a human wants to find. Archived repositories are
included and flagged rather than dropped, so old work stays reachable.

It costs no additional API calls. Discovery already enumerates all of this on
every cycle to feed the filter, and `description` is a scalar field on a node
the query already fetches, so it adds nothing to the GraphQL point cost.

Two behaviours worth knowing:

- **Only a complete sweep replaces it.** If one owner's discovery fails while
  the others succeed, the previous index is kept and a warning is logged.
  Publishing a partial result would silently drop every repository of the
  failed owner, which a client cannot distinguish from upstream deletion.
  `generated_at` is `null` until the first complete sweep finishes.
- **It is a snapshot, not a search endpoint.** There is no `?q=`. The intended
  consumer is a type-ahead box, and a query endpoint would mean an HTTP round
  trip per keystroke to filter a list that fits in a few kilobytes. Fetch it
  once and match locally.

A repository that has never received a commit does not appear, because
discovery drops repositories with no default branch.

## Exported metrics

| Metric                                             | Type    | Labels                                              |
| -------------------------------------------------- | ------- | --------------------------------------------------- |
| `github_repo_issues_open`                          | gauge   | `org`, `repo`, `author_kind`                        |
| `github_repo_pulls_open`                           | gauge   | `org`, `repo`, `author_kind`                        |
| `github_repo_pulls_draft`                          | gauge   | `org`, `repo`, `author_kind`                        |
| `github_repo_pulls_ignored`                        | gauge   | `org`, `repo`                                       |
| `github_pull_created_timestamp_seconds`            | gauge   | `org`, `repo`, `number`, `author`, `author_kind`, `draft`, `checks`, `mergeable`, `auto_merge` |
| `github_pull_needs_attention`                      | gauge   | same as above                                       |
| `github_pull_ready_to_merge`                       | gauge   | same as above                                       |
| `github_workflow_run_status`                       | gauge   | `org`, `repo`, `workflow`, `event`, `conclusion`    |
| `github_workflow_run_stale`                        | gauge   | `org`, `repo`, `workflow`                           |
| `github_workflow_enabled`                          | gauge   | `org`, `repo`, `workflow`, `state`                  |
| `github_workflow_run_timestamp_seconds`            | gauge   | `org`, `repo`, `workflow`                           |
| `github_workflow_last_success_timestamp_seconds`   | gauge   | `org`, `repo`, `workflow`                           |
| `github_workflow_expected_interval_seconds`        | gauge   | `org`, `repo`, `workflow`                           |
| `github_repo_monitored`                            | gauge   | `org`, `repo`                                       |
| `github_repos_skipped`                             | gauge   | `reason`                                            |
| `github_flake_input_locked_timestamp_seconds`      | gauge   | `repo`, `input`, `path`, `ref`, `channel`           |
| `github_flake_input_upstream_timestamp_seconds`    | gauge   | `repo`, `input`, `path`, `ref`, `channel`           |
| `github_flake_input_up_to_date`                    | gauge   | `repo`, `input`, `path`, `ref`, `channel`           |
| `github_flake_input_outdated_since_timestamp_seconds` | gauge | `repo`, `input`, `path`, `ref`, `channel`          |
| `github_flake_input_commits_behind`                | gauge   | `repo`, `input`, `path`, `ref`                      |
| `github_flake_input_update_pr_open`                | gauge   | `repo`, `input`, `number`                           |
| `github_flake_input_impact`                        | gauge   | `repo`, `input`, `os`, `kind`                       |
| `github_exporter_rate_limit_remaining`             | gauge   | `resource`                                          |
| `github_exporter_rate_limit_limit`                 | gauge   | `resource`                                          |
| `github_exporter_rate_limit_reset_timestamp_seconds` | gauge | `resource`                                          |
| `github_exporter_rate_limit_reserve`               | gauge   | --                                                  |
| `github_exporter_budget_exhausted`                 | gauge   | --                                                  |
| `github_exporter_cycles_bypassed_total`            | counter | --                                                  |
| `github_exporter_scrape_success`                   | gauge   | --                                                  |
| `github_exporter_scrape_duration_seconds`          | gauge   | --                                                  |
| `github_exporter_last_success_timestamp_seconds`   | gauge   | --                                                  |
| `github_exporter_api_requests`                     | gauge   | --                                                  |
| `github_exporter_api_not_modified`                 | gauge   | --                                                  |
| `github_exporter_api_requests_skipped`             | gauge   | --                                                  |

Every metric above lives in the exporter's own `github_`-prefixed registry.
`/metrics` also serves a second, **unprefixed** `nixos_channel_*` registry in
the same response -- see "NixOS channel status" under "Flake input tracking"
below.

`author_kind` is `human` or `bot`. This is what allows a dashboard to show bot
activity for visibility while alert rules select only `author_kind="human"`.

`github_workflow_run_status` is `1` for the current conclusion of each
workflow. An in-flight run reports `conclusion="running"` and does not clear a
previous failure.

For `github_flake_input_locked_timestamp_seconds`, `input` is the root `flake.lock`
input (`niri`), `path` is the full input path including any owned nixpkgs
(`niri` or `niri/nixpkgs`), `ref` is the branch or tag it is locked to (empty
for the default branch or for anything not locked through `github`), and
`channel` is the NixOS channel it tracks, when it tracks one (empty
otherwise). See "Flake input tracking" above.

The remaining `github_flake_input_*` families compare that lock against its
upstream: a GitHub-locked node's upstream comes from a single aliased
GraphQL query every flake pass, and a channel-tarball node's comes from
NixOS channel status -- see "NixOS channel status" below.

- `github_flake_input_upstream_timestamp_seconds` is the upstream commit time
  for a branch-tracking input, the newest release's commit time for a
  tag-pinned one, or the channel's last-moved time for a channel tarball.
- `github_flake_input_up_to_date` is `1` when the lock matches that upstream,
  `0` when it does not. When a node's upstream lookup fails this pass -- a
  transport error, or the whole GraphQL batch rejected -- the previously
  published status is retained exactly as it was: a replay of the last
  successful comparison, not a fresh one, which can go stale if the failure
  persists across many passes. A repository that resolves to an explicit
  `null` this pass (deleted, or GraphQL's `NOT_FOUND` for that alias) is
  different: that is confirmed gone rather than merely unreachable, so any
  previously cached status for it is dropped instead of kept. "No series at
  all" therefore covers two cases, not every failed lookup: an input that
  has never once resolved -- including a channel tarball before any NixOS
  channel status has been cached for it -- and one whose repository has just
  been confirmed gone.
- `github_flake_input_outdated_since_timestamp_seconds` is published only
  when an input is behind, and is measured from the **oldest** change it is
  missing, not the newest, so a busy upstream cannot keep resetting the
  clock. For a branch this comes from a REST compare between the locked
  revision and the upstream head; for a tag-pinned input or a channel
  tarball, neither of which has a compare to measure from, it is the first
  time the exporter observed that input behind the revision (a tag, or a
  channel-tarball node's own locked revision) it is currently pinned
  against, persisted to `<state_dir>/flake-first-seen.json` and cleared
  again once it catches up. A channel tarball's identity is deliberately its
  *own* locked revision, not the channel's -- the channel moves far more
  often than the lock does, and keying the clock to it would restart "how
  long has this been outdated" on every channel update even though the node
  stayed exactly as behind as before.
- `github_flake_input_commits_behind` is published only for a branch-tracking
  input that is behind, from the same REST compare.
- `github_flake_input_update_pr_open` is `1` for each open
  `update-<input>` pull request found against the configured repository.
  `number` matches the label on the existing `github_pull_*` series, so an
  update PR joins to its own age and check state.
- `github_flake_input_impact` republishes the flake-watch Action's status
  file (`status/flake-inputs.json` on the `flake-watch` branch): `1` for
  each operating system (`linux`, `darwin`) the Action classified a moved
  input's impact for, with `kind` being `none`, `rebuild`, or `source`. A
  repository whose watch Action has never run yet (a 404) publishes no
  impact series rather than failing the pass.

All of this runs on the same `flake_interval` as the lock pass, folded into
the same hourly request budget -- see "Rate-limit behaviour" above. A cycle
whose budget cannot cover the pass at all skips it entirely, before a single
request goes out, so every previously published value is kept exactly as it
was. That all-or-nothing guarantee applies only to a pass that never starts:
once it is running, each repository's `flake.lock` fetch and each GraphQL
batch is its own independent request with its own fallback -- the
per-repository and per-node behaviour documented throughout this section --
so a transient failure partway through a pass can update some repositories,
or some aliases within a batch, while others keep their older, previously
published values. It is not a single atomic unit once started.

## What counts as the current CI state

Getting this right matters more than it sounds. Naively taking the newest run
per workflow produced 38 "failures", of which fewer than half were real:

| Filter                        | Why                                                                                                                                                                                                    |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Workflow must still exist     | GitHub keeps run history after a workflow file is deleted. A removed `Update pre-commit hooks` reported a permanent failure across 10 repositories.                                                    |
| Identity is the file path     | Runs predating a workflow's `name:` report the path instead, splitting one workflow into two series.                                                                                                   |
| Default branch only           | Selected client-side, because `?branch=` is answered from an eventually-consistent index (see above). A run's `head_branch` must equal the repository's default branch.                                 |
| Branch-state events only      | A pull request's `head_branch` equals the default branch whenever it is opened from a fork's own default branch, so branch matching alone lets PR runs in. A merged PR's last pre-merge failure would otherwise be the branch's CI state forever. |
| Runs cannot move backwards    | Run history is append-only, so a newest run older than one already observed proves the response was truncated. The earlier observation is kept, costing one cycle of freshness instead of a false alert. |
| Runs age out after 90 days    | Some workflows only fire on `pull_request`, leaving a branch-state run many months old. Those report `conclusion="stale"` and set `github_workflow_run_stale`, rather than an unclearable failure.      |
| Event must still be declared  | Run history outlives a trigger change. `frext` and `bike-fitter-1000` both showed failing `push` runs for a `ci.yml` that now declares only `pull_request`. Each run's event is checked against the workflow file's current `on:` block. |
| Disabled workflows are kept   | A workflow auto-disabled by GitHub after 60 days of inactivity has stopped running silently. That is the fault worth alerting on, so it is reported rather than filtered out.                          |

## Pull requests that need action

Default-branch health says nothing about whether an open PR is stuck. Two
states are worth acting on, and both are exported:

| State | Meaning |
| --- | --- |
| `github_pull_needs_attention` | Non-draft PR that is failing checks, conflicting with the base branch, or green and awaiting a manual merge. |
| `github_pull_ready_to_merge` | Checks pass, mergeable, and **no auto-merge armed** — sitting there waiting for someone to press the button. |

The `checks` label carries the head commit's `statusCheckRollup`. Unlike on a
default-branch commit, that rollup is reliable here: PR-triggered workflows
attach to the head commit by construction.

`mergeable="unknown"` is deliberately not treated as mergeable. GitHub computes
mergeability lazily, so a first query often returns `UNKNOWN` for a PR that is
in fact conflicting — observed on two PRs that both resolved to `CONFLICTING`
on re-query.

### Suppressing a pull request

Some PRs are stuck and will stay stuck. A PR opened against a repository you do
not own, which the maintainer has not engaged with, is not yours to close or
convert to a draft — and nothing the exporter can measure distinguishes it from
a PR worth chasing. Those are declared:

```toml
ignore_pulls = ["sdr-enthusiasts/docker-vesselalert#32"]
```

A suppressed PR is dropped from `github_pull_needs_attention`,
`github_pull_ready_to_merge`, and `github_pull_created_timestamp_seconds`, so no
alert can fire on it. It still counts towards `github_repo_pulls_open`, because
the repository really does have it open, and the number suppressed is published
per repository as `github_repo_pulls_ignored` — a published zero included, so
"nothing is hidden here" is an assertion rather than an assumption.

This is deliberately per-PR. Adding the repository to `denylist` would also
blind the exporter to its CI and to every future pull request on it.

A malformed entry is a startup error, not a filter that silently never matches:
the worst outcome would be an operator believing a PR is suppressed while its
alert keeps firing.

## Flake input tracking

For repositories named in `flake_repos`, the exporter reads `flake.lock` from
the default branch through the Contents API -- the same `ETag`-cached path
`list_workflows` and the workflow-definition lookup already use -- and parses
it into the set of inputs worth watching:

- every root input in `flake.lock`, except one locked through a `follows`
  path rather than its own node, since that input has nothing of its own to
  watch;
- for each of those, its own `nixpkgs` input, if it locks one independently
  rather than following ours. This is where a pin going stale or a rebuild no
  one asked for tends to come from (`niri/nixpkgs`,
  `catppuccin/nixpkgs`), so it is tracked under its own path rather than
  folded into its parent.

Each tracked input is classified from how it is locked -- a GitHub branch
(including "no ref", the default branch), a GitHub release tag, or a
`channels.nixos.org` tarball -- and, where it applies, labelled with the NixOS
channel it tracks (`nixos-unstable`, `nixos-26.05`, ...).

This pass runs on its own interval (`flake_interval`, default `1h`, minimum
`15m`), separate from and much slower than the CI poll interval:
`flake.lock` changes on the order of days to weeks, so polling it every few
minutes would spend request budget for no new signal. Every cycle still
republishes whatever was last observed, since each cycle rebuilds the metrics
registry from scratch and discards it only once the new one is complete (see
"Rate-limit behaviour" above).

### Upstream comparison

Every GitHub-locked tracked node (a branch or a release tag) is compared
against its upstream in the same pass, through **one** aliased GraphQL query
per cycle, batched to at most 50 aliases per request. A channel-tarball node
is compared separately, against NixOS channel status -- see "NixOS channel
status" below.

- A **branch** node's upstream is its branch head (or the default branch's
  head, for a node with no `ref`).
- A **tag** node's upstream is the newest release, read from
  `latestRelease`, falling back to the newest tag by commit date when the
  repository has no GitHub Releases. Tag-pinned inputs are compared against
  the newest tag, never the default branch, or they would read as
  permanently outdated -- `nix flake update` cannot move a tag, and Renovate
  owns them.
- Each configured repository's open `update-<input>` pull requests are
  looked up in the same query, one alias per root input.

Identical lookups -- the same owner, repository, and ref -- are deduplicated
before the query is built, so an owned nixpkgs shared by coincidence costs
nothing extra. A repository that no longer exists resolves to an explicit
`null` -- a bare one, or GitHub's `NOT_FOUND` error for that alias -- and is
logged, not treated as a failed pass; every other alias in the same batch
still resolves normally, and that node's own previously published status is
dropped rather than republished (see `github_flake_input_up_to_date` above).

A node that is behind gets one more fetch: a branch gets a REST compare
between its locked revision and the upstream head, cached the same way every
other REST response in this exporter is, and a tag or a channel tarball gets
its first-seen timestamp recorded (or left alone, if already recorded) --
see `github_flake_input_outdated_since_timestamp_seconds` above.

### NixOS channel status

Prometheus treats a series as stale after 5 minutes without a sample, so
federating `prometheus.nixos.org` on this exporter's hourly cadence would
leave gaps, and federating it every few minutes would not be a good
neighbour. Instead, once per flake pass -- and only when at least one
tracked node across every `flake_repos` entry actually tracks a channel --
the exporter runs three instant queries against `nixos_prometheus_url`
(`https://prometheus.nixos.org` by default) for exactly the channels those
nodes reference:

- `channel_revision` for each channel's current revision and whether it is
  still supported (`current="1"`, `current="0"` for end of life).
- `channel_update_time` for when that revision last changed.
- `hydra_job_failed{exported_job=~".*tested"}` for whether any of a
  channel's `tested`-suffixed Hydra jobs -- the gate every package in the
  channel sits behind -- is currently failing.

The result is cached and republished every scrape regardless of whether the
pass itself ran this cycle, exactly like every other flake metric. A failed
fetch (transport error, a non-success status, or an unparsable response,
after one short retry) keeps the previously cached values and is reported
through `nixos_channel_status_up`, rather than either blocking the flake
pass or publishing a false `EndOfLife`/`Failing` reading.

This is also the exporter's first non-GitHub origin: a separate,
unauthenticated HTTP client identifies itself with its own `User-Agent` and
shares none of the GitHub client's `ETag` cache or rate-limit bookkeeping.

A **channel-tarball** node's (`catppuccin/nixpkgs`, say) upstream is its
channel's current revision from `channel_revision`, compared against the
short revision embedded in the node's own locked `releases.nixos.org` URL
(`nixpkgs-26.11pre1104102.e62746b0b472` -> `e62746b0b472`): up to date if the
channel's revision starts with it, behind otherwise. These feed the same
`github_flake_input_*` families every other tracked node does -- no separate
metric family exists for a channel tarball's own freshness.

Published **unprefixed**, alongside the exporter's `github_`-prefixed
registry in the same `/metrics` response, because these describe NixOS
infrastructure the exporter merely observes:

| Metric                                   | Type  | Labels    |
| ----------------------------------------- | ----- | --------- |
| `nixos_channel_update_timestamp_seconds` | gauge | `channel` |
| `nixos_channel_current`                  | gauge | `channel` |
| `nixos_channel_tested_failed`            | gauge | `channel` |
| `nixos_channel_status_up`                | gauge | --        |

`nixos_channel_current` is `1` if the channel is still supported, `0` if it
is end of life. `nixos_channel_tested_failed` is `1` if any of a channel's
`tested`-suffixed Hydra jobs is failing, `0` if all are passing, and absent
if the channel has never been evaluated by one. `nixos_channel_status_up` is
`1` if the last fetch succeeded, `0` if it failed and cached values are
being served, and absent entirely before the exporter has ever attempted a
fetch -- which, since the fetch itself only runs when a tracked node
references a channel, includes every deployment that never configures one.

### Impact

For each repository in `flake_repos`, the exporter also reads
`status/flake-inputs.json` from the dedicated `flake-watch` branch, written
by a GitHub Action (not yet part of this exporter) that classifies each moved
input by evaluating, never building, what it would change. That file is
published as `github_flake_input_impact`; until the Action exists for a given
repository, its absence (a 404) is normal and produces no impact series.

## Configuration

TOML file, with `GHCI_`-prefixed environment overrides:

```toml
# Organisations, personal accounts, or a mix of both.
orgs = ["sdr-enthusiasts", "fredsystems", "fredclausen"]
interval = "5m"
listen = "127.0.0.1:9418"
state_dir = "/var/lib/github-ci-exporter"
denylist = []
ignore_pulls = []
skip_repos_without_workflows = true
# max_repo_age = "365d"
flake_repos = []
flake_interval = "1h"
nixos_prometheus_url = "https://prometheus.nixos.org"
```

The token is never a config-file field. Supply it with `GHCI_TOKEN` or
`GHCI_TOKEN_FILE`.

A fine-grained, **read-only** PAT is sufficient: Metadata, Issues, Pull
requests, and Actions, all read.

```bash
GHCI_TOKEN="$(gh auth token)" github-ci-exporter --config config.toml
```

`--check` validates configuration and credentials, then exits.

## NixOS

The flake exports a NixOS module:

```nix
{
  inputs.github-ci-exporter.url = "github:fredsystems/github-ci-exporter";

  # ...

  imports = [ inputs.github-ci-exporter.nixosModules.default ];

  services.github-ci-exporter = {
    enable = true;
    orgs = [ "sdr-enthusiasts" "fredsystems" "fredclausen" ];
    tokenFile = config.sops.secrets."monitoring/github_exporter_token".path;

    # Optional: track these repositories' flake inputs (see "Flake input
    # tracking"). flakeInterval defaults to "1h" (minimum 15m);
    # nixosPrometheusUrl defaults to https://prometheus.nixos.org.
    flakeRepos = [ "fredsystems/nixos" ];
  };
}
```

The service runs as a `DynamicUser` with the token supplied through
`LoadCredential`, and binds to loopback by default.

## Development

```bash
nix develop          # or `direnv allow`
cargo xtask ci       # fmt, clippy, test, deny, machete
cargo xtask test
```

## License

MIT
