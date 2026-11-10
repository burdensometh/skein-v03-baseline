"""Filling a Prometheus :class:`~skein.metrics.prometheus.Registry` from crawl state.

Three sources feed it:

* a worker's :class:`~skein.metrics.stats.CrawlStats` (counters, status and
  error breakdowns, the fetch latency histogram, per-host counters);
* a running pipeline's stage counters (queue depth and capacity, blocked
  puts, batch sizes, commit durations, requests in flight);
* the database, which every process shares (frontier rows per state,
  leases per worker, worker liveness, host back-off, open runs).

Worker processes export the first two for themselves; ``skein metrics``
exports the third for the whole database.
"""

from __future__ import annotations

import json
import sqlite3

from skein.metrics.prometheus import Registry
from skein.metrics.stats import CrawlStats
from skein.pipeline.runner import Pipeline

_STAT_COUNTERS = {
    "requests": "HTTP requests sent (including robots.txt).",
    "responses": "HTTP responses received.",
    "retries": "Requests scheduled for another attempt.",
    "deferred": "Requests deferred to a later run.",
    "gave_up": "Requests abandoned after transient failures.",
    "host_backoffs": "Host-level back-offs imposed.",
    "bytes_received": "Bytes received on the wire.",
    "bytes_decoded": "Body bytes after content decoding.",
    "pages_stored": "Fetch results committed.",
    "conditional_requests": "Requests sent with validators.",
    "not_modified": "304 Not Modified answers.",
    "links_found": "Links extracted from pages.",
    "links_enqueued": "Links that were new to the frontier.",
    "links_filtered": "Links dropped by the seen-URL Bloom filter.",
    "near_duplicates": "Pages recognised as near duplicates.",
    "robots_denied": "Requests refused by robots.txt.",
    "warc_records": "WARC records written.",
    "warc_revisits": "WARC revisit records written.",
    "warc_bytes": "Compressed WARC bytes written.",
    "leases_lost": "Results recorded after the lease had been reclaimed.",
    "write_batches": "Writer transactions committed.",
}
_ITEM_KINDS = ("new", "updated", "unchanged", "touched", "deleted", "dropped")


def add_crawl_stats(registry: Registry, stats: CrawlStats, *, spider: str, worker: str) -> None:
    """Export one worker's run statistics, labelled by spider and worker."""
    labels = {"spider": spider, "worker": worker}
    for name, help_text in _STAT_COUNTERS.items():
        registry.counter(f"{name}_total", help_text, ("spider", "worker")).set(getattr(stats, name), **labels)
    responses = registry.counter("responses_by_status_total", "Responses by HTTP status.",
                                 ("spider", "worker", "status"))
    for status, count in stats.status_counts.items():
        responses.set(count, status=status, **labels)
    errors = registry.counter("errors_total", "Failed requests by error kind.", ("spider", "worker", "kind"))
    for kind, count in stats.errors.items():
        errors.set(count, kind=kind, **labels)
    items = registry.counter("items_total", "Item writes by outcome.", ("spider", "worker", "outcome"))
    for kind in _ITEM_KINDS:
        items.set(getattr(stats, f"items_{kind}"), outcome=kind, **labels)
    documents = registry.counter("documents_total", "Documents by change outcome.", ("spider", "worker", "outcome"))
    for kind in ("new", "changed", "unchanged"):
        documents.set(getattr(stats, f"documents_{kind}"), outcome=kind, **labels)
    registry.histogram("fetch_latency_seconds", "Time from request to complete response.",
                       stats.latency.buckets, ("spider", "worker")).set_histogram(stats.latency, **labels)
    per_host = registry.counter("host_requests_total", "Requests per origin.", ("spider", "worker", "host"))
    for host, counts in stats.per_host.items():
        per_host.set(counts.get("requests", 0), host=host, **labels)
    registry.gauge("budget_exhausted", "1 when the run's page budget is used up.",
                   ("spider", "worker")).set(int(stats.budget_exhausted), **labels)


def add_pipeline_stats(registry: Registry, pipeline: Pipeline, *, spider: str, worker: str) -> None:
    """Export a running pipeline's queue and stage counters."""
    labels = {"spider": spider, "worker": worker}
    stages = pipeline.stage_stats()
    names = ("spider", "worker", "stage")
    depth = registry.gauge("queue_depth", "Items waiting in a stage's input queue.", names)
    capacity = registry.gauge("queue_capacity", "Bound of a stage's input queue.", names)
    high = registry.gauge("queue_high_water", "Largest depth a stage's input queue reached.", names)
    blocked = registry.counter("queue_blocked_puts_total", "Puts that waited for room (backpressure).", names)
    blocked_time = registry.counter("queue_blocked_seconds_total", "Time producers waited for room.", names)
    for stage, queue in stages["queues"].items():
        depth.set(queue["depth"], stage=stage, **labels)
        capacity.set(queue["capacity"], stage=stage, **labels)
        high.set(queue["high_water"], stage=stage, **labels)
        blocked.set(queue["blocked_puts"], stage=stage, **labels)
        blocked_time.set(queue["blocked_seconds"], stage=stage, **labels)
    waits = registry.histogram("queue_wait_seconds", "Time items spent in a stage's input queue.",
                               pipeline.jobs.waits.buckets, names)
    for queue in (pipeline.jobs, pipeline.parses, pipeline.writes):
        waits.set_histogram(queue.waits, stage=queue.name, **labels)
    processed = registry.counter("stage_processed_total", "Items a stage finished.", names)
    busy = registry.gauge("stage_busy", "Items a stage is working on right now.", names)
    for stage in ("fetch", "parse"):
        processed.set(stages[stage]["processed"], stage=stage, **labels)
        busy.set(stages[stage]["busy"], stage=stage, **labels)
    processed.set(stages["write"]["outcomes"], stage="write", **labels)
    registry.gauge("in_flight", "Claimed requests not yet committed.", ("spider", "worker")).set(
        stages["dispatch"]["in_flight"], **labels)
    registry.counter("claims_total", "Requests leased from the frontier.", ("spider", "worker")).set(
        stages["dispatch"]["claims"], **labels)
    writer = pipeline.writer
    registry.histogram("write_batch_size", "Outcomes committed per writer transaction.",
                       writer.batch_sizes.buckets, ("spider", "worker")).set_histogram(writer.batch_sizes, **labels)
    registry.histogram("write_commit_seconds", "Duration of writer transactions.",
                       writer.commit_seconds.buckets, ("spider", "worker")).set_histogram(writer.commit_seconds,
                                                                                         **labels)


def add_database_state(registry: Registry, db: sqlite3.Connection, now: float) -> None:
    """Export state every process shares: frontier, leases, workers, hosts and runs."""
    frontier = registry.gauge("frontier_requests", "Frontier rows by state.", ("spider", "state"))
    for spider, state, count in db.execute("SELECT spider, state, COUNT(*) FROM frontier GROUP BY spider, state"):
        frontier.set(count, spider=spider, state=state)
    leases = registry.gauge("leases_held", "Frontier rows leased per worker.", ("spider", "worker"))
    expired = registry.gauge("leases_expired", "Leased rows whose lease has run out.", ("spider",))
    for spider, owner, count in db.execute(
            "SELECT spider, lease_owner, COUNT(*) FROM frontier WHERE state = 'active' GROUP BY spider, lease_owner"):
        leases.set(count, spider=spider, worker=owner or "")
    for spider, count in db.execute(
            "SELECT spider, COUNT(*) FROM frontier WHERE state = 'active' AND lease_expires <= ? GROUP BY spider",
            (now,)):
        expired.set(count, spider=spider)
    workers = registry.gauge("workers", "Registered workers by state.", ("spider", "state"))
    heartbeat = registry.gauge("worker_heartbeat_age_seconds", "Seconds since a running worker's heartbeat.",
                               ("spider", "worker"))
    for spider, state, count in db.execute("SELECT spider, state, COUNT(*) FROM workers GROUP BY spider, state"):
        workers.set(count, spider=spider, state=state)
    for spider, worker, beat in db.execute("SELECT spider, id, heartbeat_at FROM workers WHERE state = 'running'"):
        heartbeat.set(max(0.0, now - beat), spider=spider, worker=worker)
    backoff = registry.gauge("host_backoff_seconds", "Remaining back-off of a host.", ("host",))
    failures = registry.gauge("host_failure_streak", "Consecutive host-level failures.", ("host",))
    slots = registry.gauge("host_slots_busy", "Request slots of a host held under a live lease.", ("host",))
    for host, until, streak in db.execute("SELECT host_key, backoff_until, failures FROM host_state"):
        backoff.set(max(0.0, until - now), host=host)
        failures.set(streak, host=host)
    for host, busy in db.execute(
            "SELECT host_key, COUNT(*) FROM host_slots WHERE owner IS NOT NULL AND lease_expires > ? "
            "GROUP BY host_key", (now,)):
        slots.set(busy, host=host)
    runs = registry.gauge("runs", "Runs by status.", ("spider", "status"))
    for spider, status, count in db.execute("SELECT spider, status, COUNT(*) FROM runs GROUP BY spider, status"):
        runs.set(count, spider=spider, status=status)
    last = registry.gauge("last_run_seconds", "Duration of the latest closed run.", ("spider",))
    last_pages = registry.gauge("last_run_pages", "Pages stored by the latest closed run.", ("spider",))
    last_new = registry.gauge("last_run_items_new", "Items created by the latest closed run.", ("spider",))
    finished_at = registry.gauge("last_run_finished_timestamp_seconds", "When the latest closed run ended.",
                                 ("spider",))
    for spider, started, finished, stats in db.execute(
            """SELECT r.spider, r.started_at, r.finished_at, r.stats FROM runs r
               WHERE r.id = (SELECT MAX(id) FROM runs x WHERE x.spider = r.spider AND x.finished_at IS NOT NULL)"""):
        summary = json.loads(stats) if stats else {}
        last.set(max(0.0, finished - started), spider=spider)
        finished_at.set(finished, spider=spider)
        last_pages.set(summary.get("pages_stored", 0), spider=spider)
        last_new.set(summary.get("items", {}).get("new", 0), spider=spider)
    items = registry.gauge("items_current", "Live items (current SCD rows, not deleted).", ("spider",))
    for spider, count in db.execute(
            "SELECT spider, COUNT(*) FROM item_history WHERE is_current = 1 AND deleted = 0 GROUP BY spider"):
        items.set(count, spider=spider)
    archive = registry.gauge("warc_files", "WARC files recorded, by whether they are closed.", ("closed",))
    for closed, count in db.execute(
            "SELECT closed_at IS NOT NULL, COUNT(*) FROM warc_files GROUP BY closed_at IS NOT NULL"):
        archive.set(count, closed="true" if closed else "false")

