# Changelog

## 0.3.0

Concurrency and distribution. Databases written by 0.1 and 0.2 are
migrated automatically on open (schema versions 8-11); checkpoints of an
interrupted 0.2 run still resume.

### Changed

- The crawl loop is replaced by a staged pipeline: a dispatcher leases
  requests, fetch tasks talk to servers, parse tasks extract items and
  links, and one writer commits results in batched transactions. The
  stages are connected by bounded queues with backpressure.
- The frontier hands out work through leases (owner, expiry, attempt
  count) with compare-and-set settling; expired leases are reclaimed and
  requests that keep losing their lease are failed.
- Per-host politeness (spacing, Crawl-delay, concurrency slots, back-off,
  quarantine) moved from process memory into the `host_state` and
  `host_slots` tables, and spacing is anchored on actual send times.
- Page budget charges and sitemap `lastmod` high-water marks moved into
  the database; checkpoints are per process and hold that process's
  statistics. The journal records one `batch` record per writer
  transaction, confirmed through the worker id and sequence number stored
  on the batch's fetch events.
- `items` and `item_versions` became `item_history`, a type 2 slowly
  changing dimension (`valid_from`, `valid_to`, current-row flag); item
  writes are batched. `items` remains as a view. Version exports carry the
  validity interval.
- URL canonicalisation rules version 2 is the default: tracking parameters
  and path session ids are removed and query parameters sorted by name.
  Stored keys are recomputed and duplicates merged by the migration, and
  again when a spider's canonicalisation settings change.
- `HostScheduler` takes a database connection; `PolitenessPolicy` lives in
  `skein.politeness.policy`; page analysis moved to `skein.pipeline`.

### Added

- `skein worker` and `skein crawl --workers N` (with a supervisor that
  reaps, optionally restarts and kills hung workers); a `workers` registry
  with heartbeats; reaping of dead workers on the same machine at once and
  of remote ones when their heartbeat goes stale; merged run statistics,
  rebuilding those of dead workers from their checkpoints; a spider-wide
  `global_concurrency` over all processes; `skein workers`, `worker
  --follow`, `runs --stop`, `frontier release --leased-by`,
  `checkpoint --worker`.
- A persisted seen-URL Bloom filter with a versioned, checksummed file
  format, atomic saves, rebuild on damage or growth, lock-protected merging
  between workers and sampled false-positive verification.
- SimHash near-duplicate detection with a banded index in SQLite and
  `record`/`skip_links`/`skip` actions; `skein dedup`.
- WARC 1.1 archiving: request, response, metadata and revisit records (for
  304s and identical payloads, also across URLs), one gzip member per
  record, size-based rollover, CDX sidecars; a tolerant reader, CDX/CDXJ
  indexes with exact/prefix/host/domain queries (also served from the
  record locations in the database), replay following revisits, an HTTP
  replay server with link rewriting and Memento TimeMaps, sealing of files
  left open by a crash; `skein warc`.
- Prometheus text exposition: per-worker textfiles and `/metrics`
  endpoint (with `/healthz`), merging of worker textfiles, database state
  metrics; `skein metrics`.
- Retrying of writer batches and claims when other processes hold the
  database lock.
- `export --as-of`, `db rekey`, `db merges`; status shows workers, leases,
  hosts, near duplicates and canonicalisation rules.
- Fixture site: tracking-parameter and session-id links, near-duplicate
  story editions, links to other fixture hosts; a database written by
  skein 0.2 as a test fixture.

## 0.2.0

Persistence and incremental crawling. Databases written by 0.1 are
migrated automatically on open; version-1 configuration files still load.

### Changed

- The in-memory frontier is replaced by a SQLite frontier: every
  discovered URL keeps its state (`pending`, `active`, `done`, `failed`,
  `deferred`), attempts and earliest retry time across runs.
- The `pages` table is split into `fetch_events` (every attempt, with error
  class, attempt number, validators, fingerprint and whether it changed)
  and `documents` (current state and revisit schedule per URL).
- Items carry a SHA-256 content hash and a version number that only
  increases when the hash changes; every version is kept in
  `item_versions`. Items that disappear from their page are tombstoned and
  restored when they return.
- Configuration version 2 groups spider settings into `robots`,
  `politeness`, `retry`, `canonical`, `extract` and `incremental`, and adds
  project-level `robots` and `checkpoint` groups. Version-1 files are
  translated with deprecation warnings; their errors still name the
  original keys.
- Retries: failures are classified as transient or permanent; transient
  ones are retried with exponential back-off and seedable jitter, honouring
  `Retry-After` (delta-seconds or HTTP-date, relative to the response
  `Date`) on 429 and 503. Long waits defer the URL to a later run.
  Host-level failures pause the host; repeated ones quarantine it.
- Run statistics moved to `skein.metrics` and are rebuilt by replaying
  journal deltas; they include per-host counters.
- The CLI became a package; `export --what pages` is now `--what events`.
- DOM text extraction separates block elements on both sides
  (`<p>a</p>b` reads `a b`).

### Added

- Versioned schema migrations with history table, automatic backup before
  upgrading, dry runs on an in-memory copy and integrity checks.
- Conditional requests (`If-None-Match`, `If-Modified-Since`) and 304
  handling; content fingerprints with configurable noise patterns.
- Adaptive revisit intervals from each document's change history, with
  per-URL-pattern rules, sitemap `changefreq` hints and server freshness
  (`Cache-Control: max-age`, `Expires`) as a lower bound.
- Run modes `full`, `incremental`, `sitemap` and `auto`; per-seed
  watermarks; sitemap `lastmod` promotion of changed URLs; urgency
  ordering of re-queued URLs; a link graph so that URLs nothing links to
  any more are not revisited.
- Crash-safe resume: append-only journal with length + CRC-32 records and
  configurable fsync policy, atomically replaced snapshots, replay that
  tolerates a torn final record and skips records whose database
  transaction never committed. `--fresh` and `--max-runtime` for crawls.
- robots.txt entries persisted with validators and revalidated
  conditionally.
- Change feed with field-level diffs and named export cursors.
- Commands `plan`, `status`, `history`, `frontier`, `checkpoint`, `db`
  and `upgrade-config`; `runs --show`; exports of item versions, changes,
  fetch events and documents.
- Fixture site: ETag/Last-Modified validators with 304 answers, content
  revisions under test control, a controllable clock, 429/503 with
  `Retry-After`, `Cache-Control`, response latency and noisy footers.

## 0.1.0

First release: single-process crawler.

- HTTP/1.1 client on asyncio streams: per-origin keep-alive pool with
  transparent replay on stale connections, chunked transfer coding with
  trailers, gzip/deflate (including raw deflate and multi-member gzip),
  redirect following with loop detection and a per-request filter,
  connect/read/total timeouts, body size limits on the wire and after
  decompression, charset detection from BOM, `Content-Type`, `<meta>` and
  XML declarations.
- URL parsing and RFC 3986 reference resolution; canonicalisation with
  lower-cased IDNA hosts, numeric IPv4 normalisation, default-port removal,
  percent-encoding normalisation, dot-segment removal, fragment removal and
  a configurable query policy (sort, remove, keep-only, drop empty).
- robots.txt (RFC 9309) parser and per-origin cache with TTL, 4xx = allow
  all, 5xx/429/unreachable = disallow all, stale-copy fallback; `Crawl-delay`
  and `Sitemap` support; `X-Robots-Tag` and robots meta directives.
- Sitemap parsing: urlset, sitemap index, RSS 2.0 and Atom feeds, plain text,
  gzip, `lastmod` in all W3C Datetime profiles, entry and size limits, DTDs
  refused.
- Politeness scheduler (minimum per-host spacing, capped `Crawl-delay`,
  per-host concurrency, back-off on retries) and an in-memory priority
  frontier with depth limit and seen set.
- Tolerant DOM on `html.parser` and a CSS selector engine (type, class, id,
  attribute operators, descendant/child/sibling combinators, `:nth-child`
  family, `:first-/last-/only-child`, `:empty`, `:not`).
- Extraction rules with scopes, text/HTML/attribute sources, transforms,
  regex post-processing, type coercion, multi-value and joined fields, item
  keys; link rules with allow/deny, depth limits, priorities, sitemap links
  and extension filtering.
- SQLite store (WAL) for runs, fetch records and items with first/last seen.
- JSON configuration version 1 with full validation and environment
  variable expansion.
- CLI: `crawl`, `export` (JSONL/CSV, items or fetch log), `validate`,
  `extract`, `robots`, `runs`.
- Asyncio fixture site for tests and demos.
