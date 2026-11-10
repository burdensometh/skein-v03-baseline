# skein

Polite, incremental web harvesting for sites you are authorised to crawl, built on the
Python standard library only (Python 3.10+). skein fetches pages over its own
asyncio HTTP/1.1 client, honours robots.txt and per-host politeness, extracts
structured items with CSS-selector rules and keeps everything in SQLite: a
persistent crawl frontier, every fetch attempt, the current state of every
document and the full history of every item (as a type 2 slowly changing
dimension). Recrawls are incremental (conditional requests, sitemap
`lastmod`, adaptive revisit intervals) and an interrupted crawl resumes where
it stopped.

A crawl runs as a staged pipeline (fetch, parse, batched write) and can be
shared by several worker processes that lease work from the database and
coordinate per-host politeness through it. Responses can be archived as
WARC 1.1 files and replayed; a persisted Bloom filter and SimHash
fingerprints de-duplicate URLs and near-identical pages; run statistics are
exported in the Prometheus text format.

## Quick start

```console
$ python -m tests.fixtures_site --port 8000          # demo site, in another terminal
$ python -m skein validate examples/fixture-shop.json
$ python -m skein crawl examples/fixture-shop.json   # first run: full crawl
$ python -m skein crawl examples/fixture-shop.json   # later runs: incremental
$ python -m skein export --config examples/fixture-shop.json --what changes --cursor warehouse
$ python -m skein crawl examples/fixture-shop.json --workers 2 --warc   # three processes, archived
$ python -m skein warc replay http://127.0.0.1:8000/product/1 --config examples/fixture-shop.json
```

`pip install .` additionally provides a `skein` console script.

## Commands

| command | purpose |
|---|---|
| `skein crawl CONFIG [-s SPIDER] [--mode full\|incremental\|sitemap\|auto] [--fresh] [--max-runtime S] [--db PATH] [--checkpoint-dir DIR] [--fsync always\|batch\|never] [--max-pages N] [--max-depth N] [--min-delay S] [--concurrency N] [--workers N] [--restart-workers N] [--warc [DIR]] [--metrics-file F] [--metrics-port P] [--lease-ttl S] [--stats text\|json]` | run a spider, or resume its interrupted run; `--workers` adds worker processes |
| `skein worker CONFIG [-s SPIDER] [--id ID] [--wait-for-run S] [--follow] [--max-runtime S] [--db PATH] [--warc [DIR]] [--metrics-file F] [--metrics-port P] [--lease-ttl S]` | join the running run of a spider as an additional process (`--follow`: keep joining later runs) |
| `skein workers list\|reap (--db\|--config) [-s SPIDER] [--json]` | registered workers with liveness, leases held and heartbeat age; reclaim the work of dead ones |
| `skein plan CONFIG [-s SPIDER] [--mode M] [--json]` | what the next crawl would queue (read-only) |
| `skein export (--db PATH \| --config CONFIG) [--what items\|versions\|changes\|events\|documents] [-f jsonl\|csv] [-o FILE] [-t TYPE] [--since ISO] [--changed-since ISO] [--as-of ISO] [--cursor NAME] [--fields a,b]` | export current items (or the versions valid at `--as-of`), item versions with validity intervals, the change feed, fetch events or document state |
| `skein history (--db\|--config) TYPE KEY [--json]` | every version of one item with field-level differences |
| `skein status (--db\|--config) [-s SPIDER] [--json]` | frontier states, leases, workers, fetch statuses, revisit schedule, watermarks, near duplicates, canonicalisation rules, checkpoints, host politeness state, robots.txt cache |
| `skein runs (--db\|--config) [-s SPIDER] [--json] [--show RUN_ID] [--stop RUN_ID]` | list runs; one run's statuses, hosts, errors and workers; stop a running run in every process |
| `skein frontier list\|release (--db\|--config) [--state S] [--host ORIGIN] [--limit N] [--leased-by WORKER]` | inspect the queue; `release` queues failed (or deferred) URLs again, or takes back a worker's leases |
| `skein checkpoint verify\|dump\|discard (--db\|--config) [--worker ID]` | check a journal, print records after the snapshot, or abandon the interrupted run (or a worker's checkpoint) |
| `skein db status\|migrate\|prune\|rekey\|merges (--db\|--config) [--check] [--dry-run] [--no-backup] [--older-than DAYS] [--vacuum] [-s SPIDER]` | schema version and history, integrity check, migration (with backup, or as a dry run on an in-memory copy), pruning of old fetch events, recomputing URL keys with the configured canonicalisation, the log of renamed and merged URLs |
| `skein warc list\|verify\|index\|extract\|query\|replay\|serve ...` | list records, check digests, write a CDX/CDXJ index, extract a record by offset, query captures by URL/prefix/host/domain and time, rebuild an archived response, serve the archive over HTTP |
| `skein dedup bloom\|near (--db\|--config) [-s SPIDER] [--rebuild] [--check URL] [--max-distance N] [--json]` | seen-URL filter statistics, rebuild and membership tests; near-duplicate clusters, relinking from stored SimHashes |
| `skein metrics (--db\|--config\|--merge FILE...) [--drop-label L] [-o FILE]` | Prometheus exposition of the database state, or of several worker textfiles merged |
| `skein validate CONFIG` | validate a configuration (version 1 files list their deprecations) |
| `skein upgrade-config OLD [-o NEW]` | rewrite a version-1 configuration as version 2 |
| `skein extract CONFIG URL [--file HTML] [--no-links]` | apply a spider's rules to one page; prints items with content hashes and the page fingerprint |
| `skein robots URL... [--config CONFIG] [--user-agent UA]` | robots.txt state, crawl-delay, sitemaps and the allow/deny decision per URL |

Exit codes: `0` success, `1` runtime error, `2` invalid configuration.
The first Ctrl-C (or SIGTERM) during `crawl` finishes in-flight requests
(in its worker processes too) and records the run as `stopped`; the next
`crawl` resumes it.

## Configuration (version 2)

Configuration is a JSON file with `"version": 2`. Every problem is reported
with its JSON path, and unknown keys get a "did you mean" suggestion.
Strings may reference environment variables as `${NAME}` or
`${NAME:-default}` (`$$` is a literal `$`).

Top level: `version`, `project`, `database` (relative paths are resolved
against the config file's directory), `user_agent`, `http`, `robots`,
`checkpoint`, `pipeline`, `workers`, `dedup`, `warc`, `metrics` and
`spiders`.

- `http`: `connect_timeout`, `read_timeout`, `total_timeout`, `max_body_bytes`,
  `max_redirects`, `max_connections_per_host`, `verify_tls`, `headers`.
- `robots`: `ttl` (seconds a fetched robots.txt is trusted).
- `checkpoint`: `directory` (default `<database>.checkpoints`), `fsync`
  (`always`, `batch`, `never`), `fsync_batch`, `fsync_interval`,
  `snapshot_every` (records), `snapshot_interval` (seconds),
  `sqlite_synchronous` (`normal` or `full`).
- `pipeline`: `fetch_queue` (32), `parse_queue` (64), `write_queue` (128)
  -- the bounds of the queues between stages --, `write_batch` (64 outcomes
  per writer transaction at most), `parse_tasks` (2), `parse_threads` (0:
  parse on the event loop; more runs HTML parsing in a thread pool).
- `workers`: `lease_ttl` (60 s), `heartbeat_interval` (10 s, shorter than
  the lease), `max_lease_attempts` (5), `reclaim_interval` (5 s),
  `poll_interval` (0.5 s).
- `dedup`: `bloom` (`enabled` true, `directory` (default
  `<database>.seen`), `capacity` 200000, `error_rate` 1e-6,
  `verify_every` 64) and `near_duplicates` (`enabled` true,
  `max_distance` 3, `shingle_size` 3, `min_words` 8, `action` `record`,
  `skip_links` or `skip`).
- `warc`: `enabled` (false), `directory` (default `<database>.warc`),
  `prefix`, `max_size` (100 MiB), `metadata` (true), `revisits` (true),
  `compresslevel` (6).
- `metrics`: `textfile` (rewritten atomically every `interval` seconds),
  `interval` (15), `port` (serve `/metrics`; 0 picks a free port).

Each spider has `name`, `seeds`, `sitemaps`, `allowed_domains` (default:
hosts of seeds and sitemaps), `max_depth`, `max_pages`, `concurrency`
(fetch tasks per process), `global_concurrency` (requests in flight over
all processes together) and these groups:

| group | keys (defaults) |
|---|---|
| `robots` | `obey` (`true`), `use_sitemaps` (`false`: also crawl sitemaps announced in robots.txt) |
| `politeness` | `min_delay` (`1.0`), `max_crawl_delay` (`60`), `per_host_concurrency` (`1`), `respect_crawl_delay` (`true`) |
| `retry` | `max_retries` (`2`), `backoff_base` (`5`), `backoff_factor` (`2`), `backoff_max` (`300`), `jitter` (`0.25`), `respect_retry_after` (`true`), `max_retry_after` (`3600`), `host_failure_limit` (`10`), `quarantine` (`3600`), `seed` (RNG seed for jitter) |
| `canonical` | `rules` (`2`), `query_order` (`sort`/`preserve`, rules 2), `tracking_params` (extra patterns, rules 2), `sort_query` (rules 1), `remove_params`, `keep_params`, `drop_empty_params`, `keep_fragment` |
| `extract` | `items`, `links`, `obey_nofollow`, `obey_noindex`, `deny_extensions` |
| `incremental` | `mode` (`auto`), `conditional_requests` (`true`), `use_sitemap_lastmod` (`true`), `fingerprint_ignore` (regexes), `revisit` (`initial_interval` 1 day, `min_interval` 1 hour, `max_interval` 30 days, `target_probability` 0.5, `retry_interval`, `honour_freshness` `true`), `revisit_rules` (`pattern` plus any revisit bound) |

Canonicalisation rules version 2 (the default) remove tracking parameters
(`utm_*`, `gclid`, `fbclid`, `msclkid`, `mc_eid`, `_hsenc` and others, plus
`tracking_params`), drop session ids from path parameters
(`;jsessionid=...`) and sort query parameters by name, keeping the order of
repeated names, unless `query_order` is `preserve`. Rules version 1 keeps
the keys skein 0.1 and 0.2 produced. The database records which rules and
settings produced each spider's keys; when they change, the next crawl
recomputes the stored keys and merges the URLs that now coincide (going
back to rules version 1 is refused).

Link rules: `allow`, `deny` (regexes), `selector`, `attr`, `max_depth`,
`priority`, `kind` (`page`/`sitemap`), `from` (regex on the source page URL).
Item rules: `name`, `url_pattern`, `scope`, `key` and `fields`; a field is a
selector string or an object with `selector`, `source` (`text`, `own_text`,
`html`, `outer_html`, `attr`), `attr`, `transforms`, `regex`, `type`
(`string`, `int`, `float`, `number`, `bool`, `date`, `datetime`, `url`),
`formats`, `multiple`, `join`, `required` and `default`. See
`examples/fixture-shop.json`.

Version-1 files (flat spider keys such as `min_delay`, `items`,
`remove_params`, `retry_delay`, and a top-level `robots_ttl`) still load:
they are translated to version 2 with a `ConfigDeprecationWarning` per
moved key, and validation errors keep naming the keys as written in the
file. A v1 `retry_delay` becomes a constant back-off (`backoff_factor` 1,
no jitter), as in v1. `skein upgrade-config` writes the translated file.

## How a crawl works

1. **Planning.** The frontier is a SQLite table holding every URL the
   spider has discovered and its state (`pending`, `active`, `done`,
   `failed`, `deferred`). A run starts by re-queueing known URLs: all of
   them in `full` mode; in `incremental` mode the ones whose revisit time
   has come and that some page still links to; in `sitemap` mode only the
   sitemaps. `auto` is `full` until every seed and sitemap has a completed
   run (a per-seed *watermark*), then `incremental`. Re-queued documents
   are ordered by urgency: never-fetched URLs first, then documents that
   change often or are long overdue. Seeds and sitemaps are always visited.
2. **The pipeline.** A dispatcher leases requests from the frontier and
   feeds `concurrency` fetch tasks; responses go to the parse tasks, and
   every result to one writer. The stages are connected by bounded queues:
   when the writer falls behind, parsers wait, fetchers wait and the
   dispatcher stops leasing new work (backpressure). The writer commits
   whatever has accumulated (up to `write_batch` results) in one SQLite
   transaction.
3. **Politeness.** A request is only leased when its host is ready:
   `min_delay`/`Crawl-delay` start-to-start spacing, per-host concurrency
   and back-off after failures. Host state lives in the database
   (`host_state`, `host_slots`), so the limits hold across every process
   crawling the same hosts; spacing is measured between actual sends.
4. **robots.txt** (RFC 9309) is fetched on first contact with an origin and
   stored in the database with its validators, so later runs (and other
   workers) reuse it and refresh it with a conditional request. 4xx means
   everything is allowed, 5xx/429/network failure means everything is
   disallowed (for at least the server's `Retry-After`).
5. **Fetching.** Known documents are requested with `If-None-Match` /
   `If-Modified-Since`; a `304` keeps the stored content and marks its
   items as seen. Failures are classified: timeouts, resets, 408/429/5xx
   are transient and retried with exponential back-off and jitter (at least
   the server's `Retry-After`, either delta-seconds or an HTTP-date,
   measured against the response's `Date`); 4xx, oversized bodies,
   decoding errors and redirect loops are permanent. A `Retry-After`
   longer than `max_retry_after` defers the URL to a later run. Overload
   statuses and connection failures pause the whole host; after
   `host_failure_limit` consecutive such failures the host's queue is
   deferred for `quarantine` seconds.
6. **Parsing.** Pages yield items, links and a SimHash of their visible
   text; sitemaps (urlset, index, RSS/Atom, gzip, text) yield URLs. Links
   the seen-URL Bloom filter already holds are dropped right away (every
   `verify_every`-th such hit is checked against the frontier, which
   measures the filter's real false-positive rate).
7. **Change detection.** Each response gets a fingerprint of its visible
   text, title and link targets (scripts, comments, whitespace and
   `fingerprint_ignore` matches excluded). Comparing fingerprints, or a
   `304`, tells whether the document changed; the change history drives
   the next revisit interval (bias-reduced change-rate estimate, bounded
   growth and shrink, sitemap `changefreq` as the first hint,
   `Cache-Control: max-age`/`Expires` as a lower bound). In incremental and
   sitemap modes an entry whose sitemap `lastmod` is newer than the root's
   watermark and the document's last successful fetch is fetched in this
   run even if its revisit time has not come.
8. **Items.** Item data is stored as canonical JSON with a SHA-256 content
   hash. `item_history` keeps one row per version with its validity
   interval (`valid_from`, `valid_to`) and a current-row flag; a new
   version is only written when the hash changes. Items a page no longer
   carries (or whose page returns 404/410) get a tombstone version; if they
   come back they are restored as a new version. `skein history` and
   `export --what changes` show field-level diffs, named cursors let a
   consumer read only what changed since its last export, and
   `export --as-of` gives the versions valid at any moment.
9. **Commit.** For every result of a batch, the fetch event, document
   state, SimHash index entry, WARC record location, items, discovered
   links, link graph and frontier state change are written in one
   transaction together with the worker's journal record.

## Several processes

`skein crawl --workers N` starts N `skein worker` processes next to the
local crawler; more workers can join a running run at any time with
`skein worker CONFIG`. Every process leases frontier rows (`lease_owner`,
`lease_expires`, `lease_attempts`), renews its leases and host slots with
a heartbeat and registers in the `workers` table. A process that dies is
noticed by its peers -- at once if it ran on the same machine (its process
is gone), otherwise when its heartbeat goes stale -- and its leased
requests return to the queue; so do requests whose lease simply expired.
A request leased `max_lease_attempts` times without being settled is
failed with an explanatory fetch event.

Delivery is at least once: a worker whose lease was taken over still
records its result, but the frontier row belongs to the new owner
(settling is a compare-and-set on the lease). Writes are idempotent where
it matters: re-processing a page adds a fetch event but no item version.
SQLite lock contention between processes is waited out (batches are
retried as a whole). The supervisor of `crawl --workers` restarts killed
workers if asked to (`--restart-workers`) and kills workers whose
heartbeat stops while their process lives.

The last process to leave a run closes it: when the frontier holds no
pending or leased request and no other worker of the run is alive, it
merges every worker's statistics (rebuilding those of dead workers from
their checkpoints), advances the watermarks and records the run summary.
`skein runs --stop RUN_ID` stops a run in every process.

## Archiving

With `warc.enabled` (or `--warc`) every exchange is written to WARC 1.1
files: a `request` record, a `response` record (HTTP head as received,
content-coded payload, `WARC-Block-Digest` and `WARC-Payload-Digest` in
SHA-1 base32) and a `metadata` record (fetch time, referrer, redirects,
outlinks, SimHash). A `304`, or a payload identical to the last one
archived for the URL or for any other URL, becomes a `revisit` record
(profiles server-not-modified and identical-payload-digest) referring to
the original capture. robots.txt responses are archived too. Each record
is its own gzip member; files roll over at `max_size`, start with a
`warcinfo` record and get a CDX sidecar index when closed. Fetch events
store the file and offset of their record.

The reader iterates gzip-per-record and plain WARC files, reads any record
by offset and stops at a truncated final record instead of failing.
`skein warc index` writes sorted CDX (or CDXJ) indexes; `skein warc query`
searches them by URL, prefix, host or domain and time range;
`skein warc replay` rebuilds a response (following revisits, decoding
content codings) and `skein warc serve` replays captures over HTTP at
`/web/<timestamp>/<url>`, with links rewritten into the archive, plus a
`/cdx` endpoint and Memento TimeMaps at `/timemap/link/<url>`. Given a
database, replay and queries use the record locations stored on fetch
events instead of reading the archive. A file a killed writer left open is
sealed when its worker starts again: a torn final record is cut off and
the sidecar index written.

## De-duplication

The seen-URL filter is a Bloom filter persisted per spider in
`<database>.seen/<spider>.bloom`: a 72-byte header (magic `SKBF`, format
version, hash scheme, k, m, count, capacity, error rate, the database's
identity, payload and header CRC-32) followed by the bit array, replaced
atomically. A missing, damaged, foreign or outgrown file is rebuilt from
the frontier. Workers sharing a file merge their bits under a file lock
when saving.

Near-duplicate pages are found with 64-bit SimHashes of their text stored
in SQLite together with four 16-bit bands; pages within `max_distance`
bits share at least one band, so candidates come from indexed lookups.
A near duplicate is linked to the earlier page it resembles; with
`action` `skip_links` its links are not followed, with `skip` neither its
links nor its items are used.

## Metrics

Every worker can rewrite a Prometheus textfile (`metrics.textfile`) and
serve `/metrics` (`metrics.port`): request, response, error, item,
document, de-duplication, archive and lease counters, the fetch latency
histogram, queue depth, capacity, blocked puts and queue waiting time per
stage, writer batch sizes and commit durations. The endpoint also answers
`/healthz` with a JSON liveness report (503 when requests are in flight
but nothing was committed for a lease period). The local crawler of
`crawl --workers` folds its workers' textfiles into its own.
`skein metrics` exports the shared database state (frontier states,
leases per worker, worker heartbeats, host back-off and busy slots, runs,
the latest run's figures, live items, WARC files).

## Crash safety and resuming

Each process keeps a checkpoint of what is not shared through SQLite --
its share of the run statistics and the origins whose robots.txt sitemaps
it queued: the local crawler in `<database>.checkpoints/<spider>/`, each
worker in `.../<spider>/workers/<id>/`. A checkpoint is a `snapshot.bin`,
replaced atomically (temporary file, fsync, rename, directory fsync), plus
an append-only journal of `journal-<seq>.log` segments in which every
record carries its length and CRC-32. The journal is fsynced per record,
per batch or never (`checkpoint.fsync`). Each writer batch is journaled
inside its database transaction and the record's sequence number is
stored on the batch's fetch events with the worker id, so on restart
records whose transaction never committed are recognised and skipped.
Replay tolerates a torn final record (it is cut off); damage followed by
more records is reported as corruption. Page budget charges, host
politeness and back-off and sitemap high-water marks are in the database.

After `kill -9`, the next `skein crawl` resumes the same run: leases held
by the killed process return to the queue, committed results are kept and
not fetched again, and no item version is written twice. `--fresh`
abandons the interrupted run instead. A checkpoint written by skein 0.2
still resumes; its budget charges, host state and lastmod marks are moved
into the database.

## Database

The schema is versioned (`PRAGMA user_version` plus a `schema_migrations`
table). Opening an older database migrates it automatically, one
transaction per step, after writing a backup copy
(`<db>.schema-v<N>.bak`). A skein 0.1 database is upgraded by splitting
its `pages` table into `fetch_events` (every attempt, with error class,
validators, fingerprint) and `documents` (current state per URL), hashing
and versioning its items, and turning every known URL into a settled
frontier entry. A skein 0.2 database gets lease columns, the worker and
host tables, its `items`/`item_versions` folded into `item_history` (row
ids preserved, so change-feed cursors stay valid; `items` remains as a
view of the current rows) and its URL keys recomputed with
canonicalisation rules version 2, merging duplicates: fetch events are all
kept and re-pointed; of the frontier rows the one with outstanding work
wins (shallowest depth, highest priority, earliest discovery kept); the
document with the latest success provides the content state while fetch,
check and change counters are summed; duplicate links are merged; items
are keyed by item key and only change their URL. Every rename is logged
in `canonical_merges` (`skein db merges`).

Tables: `runs`, `fetch_events`, `documents`, `item_history`, `frontier`,
`watermarks`, `robots_cache`, `links`, `export_cursors`, `workers`,
`host_state`, `host_slots`, `simhashes`, `simhash_bands`, `warc_files`,
`canonical_rules`, `canonical_merges`, `skein_meta`.

## Library use

```python
import asyncio
from skein.config import load_config
from skein.crawler import Crawler
from skein.store import Store

config = load_config("examples/fixture-shop.json")
with Store(config.database) as store:
    crawler = Crawler(config.spider("shop"), store, http=config.http, runtime=config.runtime,
                      checkpoint_dir=config.checkpoint_root(), checkpoint=config.checkpoint)
    result = asyncio.run(crawler.run())
print(result.status, result.mode, result.stats["items"])
```

`Crawler(..., role="worker", worker_id=..., cooperative=True)` joins a run
in progress instead; `crawler.metrics()` returns the worker's metric
families.

All time-dependent code takes a `skein.clock.Clock`; `FakeClock` makes
politeness delays and revisit schedules instantaneous in tests. Back-off
jitter comes from an injectable `random.Random` (or `retry.seed`).

## Layout

```
skein/net          HTTP/1.1 client: wire protocol, pool, content codings, charsets, HTTP dates; small HTTP server
skein/urls         RFC 3986 parsing/resolution, canonicalisation rules 1 and 2 (IDNA, IPv4, query policy, tracking)
skein/robots       robots.txt parser, matcher, per-origin cache with persistence, page directives
skein/sitemaps     sitemap parsing and W3C datetimes
skein/politeness   politeness policy and the cross-process host scheduler (spacing, slots, back-off)
skein/retry        failure classification, Retry-After, back-off and retry decisions
skein/frontier     SQLite-backed crawl frontier with leases
skein/incremental  run modes and planning, validators, fingerprints, freshness, revisit intervals
skein/checkpoint   framed journal, atomic snapshots, worker session state
skein/html         html.parser-based DOM and CSS selector engine
skein/extract      field/item/link rules, coercion, content hashes
skein/pipeline     dispatcher, fetch, parse and batched write stages with bounded queues
skein/crawler      run lifecycle of the local crawler and of workers
skein/workers      liveness, heartbeats and reaping, worker processes and their supervisor
skein/dedup        Bloom filter and its file format, seen-URL filter, SimHash and its banded index
skein/warc         WARC records, writer, reader, CDX index, replay, link rewriting, replay server
skein/store        SQLite schema migrations and repositories (runs, events, documents, item history, rekeying, ...)
skein/config       JSON schema validation, v2 settings, runtime settings, v1 compatibility
skein/metrics      replayable run statistics, Prometheus exposition, collectors, /metrics server
skein/export       JSONL/CSV writers
skein/cli          command line
tests/fixtures_site  asyncio fixture web site used by the tests and the demo
tests/data         databases written by skein 0.1 and 0.2, and a version-1 configuration
```

## Development

```console
$ python -m pytest -q
```

The suite is offline and deterministic: it starts fixture sites on
ephemeral 127.0.0.1 ports, uses temporary SQLite files and checkpoint
directories, and includes real `skein crawl` subprocesses that are killed
with SIGKILL and resumed, and a `skein crawl --workers 2` whose worker
process is SIGKILLed while it holds leases. The fixture site also serves
links with tracking parameters and session ids (`/tracked`), stories in
near-duplicate editions (`/articles`) and links to other fixture hosts.
`tests/data/v02_shop.db` was written by the 0.2 code (from a `git
worktree` of the v0.2 commit) crawling `/tracked`.
