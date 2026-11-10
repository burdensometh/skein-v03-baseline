"""End-to-end assembly of one announcement from a listing and its detail page.

The site mirrors the regulatory bulletin in the bug report: the paginated
listing owns status/update-date/membership, detail pages own the body, the
issuing agency and attachments. Two same-named partial rules must produce
exactly one record per document number, new versions only on real content
changes, and deletions only when an item leaves the whole listing.
"""

from __future__ import annotations

import asyncio
import random
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from skein.clock import FakeClock
from skein.config import parse_config
from skein.crawler import Crawler
from skein.store import Store

DAY = 86400.0


@dataclass
class Announcement:
    doc_no: str
    status: str
    updated: str
    title: str
    agency: str
    body: str
    attachments: list[str] = field(default_factory=list)


def listing_html(items: list[Announcement], page: int, pages: int) -> bytes:
    rows = "".join(
        f'<li class="notice">'
        f'<a class="link" href="/notice/detail/{a.doc_no}" data-doc="{a.doc_no}">{a.title}</a>'
        f'<span class="status">{a.status}</span>'
        f'<span class="updated">{a.updated}</span></li>'
        for a in items
    )
    nav = "".join(f'<a href="/notice/list?page={n}">{n}</a>' for n in range(1, pages + 1) if n != page)
    return (f"<html><body><ul class=listing>{rows}</ul><div class=nav>{nav}</div></body></html>").encode()


def detail_html(a: Announcement) -> bytes:
    links = "".join(f'<a class="attachment" href="{href}">att</a>' for href in a.attachments)
    return (
        "<html><body><article>"
        f'<span class="doc-no">{a.doc_no}</span>'
        f'<h1>{a.title}</h1><div class="agency">{a.agency}</div>'
        f'<div class="body">{a.body}</div>{links}'
        "</article></body></html>"
    ).encode()


class AnnouncementSite:
    """A tiny mutable bulletin site served on an ephemeral localhost port."""

    def __init__(self, per_page: int = 2) -> None:
        self.per_page = per_page
        self.announcements: list[Announcement] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        assert self._server is not None
        return f"http://127.0.0.1:{self._server.server_port}"

    def pages(self) -> int:
        return max(1, (len(self.announcements) + self.per_page - 1) // self.per_page)

    def start(self) -> "AnnouncementSite":
        site = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_: object) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802
                path, _, query = self.path.partition("?")
                if path == "/notice/list":
                    page = int(dict(p.split("=") for p in query.split("&")) .get("page", "1")) if query else 1
                    pages = site.pages()
                    start = (page - 1) * site.per_page
                    body = listing_html(site.announcements[start:start + site.per_page], page, pages)
                elif path.startswith("/notice/detail/"):
                    doc_no = path.rsplit("/", 1)[-1]
                    found = next((a for a in site.announcements if a.doc_no == doc_no), None)
                    if found is None:
                        self.send_response(404)
                        self.end_headers()
                        return
                    body = detail_html(found)
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        assert self._server is not None
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def spider_spec(site: AnnouncementSite) -> dict[str, Any]:
    return {
        "name": "bulletin",
        "seeds": [site.url + "/notice/list?page=1"],
        "max_depth": 3,
        "politeness": {"min_delay": 0.0},
        "extract": {
            "links": [{"allow": [r"/notice/list\?page=\d+$", r"/notice/detail/"]}],
            "items": [
                {
                    "name": "notice",
                    "url_pattern": r"/notice/list",
                    "scope": "li.notice",
                    "partial": True,
                    "authority": True,
                    "key": ["doc_no"],
                    "fields": {
                        "doc_no": {"selector": ".link", "source": "attr", "attr": "data-doc"},
                        "status": ".status",
                        "updated": ".updated",
                    },
                },
                {
                    "name": "notice",
                    "url_pattern": r"/notice/detail/",
                    "partial": True,
                    "key": ["doc_no"],
                    "fields": {
                        "doc_no": ".doc-no",
                        "title": "h1",
                        "agency": ".agency",
                        "body": ".body",
                        "attachments": {"selector": ".attachment", "attr": "href", "type": "url",
                                        "multiple": True},
                    },
                },
            ],
        },
    }


def items_of(db: Path) -> dict[str, Any]:
    with Store(db) as store:
        return {i.key: i for i in store.items.iter("bulletin")}


def run(site: AnnouncementSite, db: Path, clock: FakeClock) -> Any:
    config = parse_config(
        {"version": 2, "spiders": [spider_spec(site)]}, base_dir=db.parent,
    )

    async def go() -> Any:
        with Store(db) as store:
            runner = Crawler(config.spiders[0], store, http=config.http, clock=clock,
                             rng=random.Random(0), runtime=config.runtime)
            return await runner.run()

    return asyncio.run(go())


def test_one_record_per_announcement_with_merged_fields(tmp_path: Path) -> None:
    clock = FakeClock()
    site = AnnouncementSite().start()
    site.announcements = [
        Announcement("N-2026-001", "published", "2026-11-01", "First", "Agency A", "body one", ["/f/1.pdf"]),
        Announcement("N-2026-002", "published", "2026-11-02", "Second", "Agency B", "body two"),
        Announcement("N-2026-003", "draft", "2026-11-03", "Third", "Agency A", "body three"),
    ]
    db = tmp_path / "bulletin.db"
    try:
        result = run(site, db, clock)
        assert result.status == "finished"
        records = items_of(db)
        assert set(records) == {"N-2026-001", "N-2026-002", "N-2026-003"}
        first = records["N-2026-001"]
        assert set(records) == {"N-2026-001", "N-2026-002", "N-2026-003"}
        assert first.data == {
            "doc_no": "N-2026-001", "status": "published", "updated": "2026-11-01",
            "title": "First", "agency": "Agency A", "body": "body one",
            "attachments": [site.url + "/f/1.pdf"],
        }
        assert all(r.version >= 1 for r in records.values())
        # No listing-only fields leak into the record from the detail rule or vice versa.
        assert records["N-2026-002"].data["status"] == "published"
        assert records["N-2026-003"].data["agency"] == "Agency A"
    finally:
        site.stop()


def test_status_change_versions_but_unchanged_detail_does_not(tmp_path: Path) -> None:
    clock = FakeClock()
    site = AnnouncementSite().start()
    site.announcements = [
        Announcement("N-1", "published", "2026-11-01", "T", "A", "body"),
        Announcement("N-2", "published", "2026-11-02", "T2", "A", "body2"),
    ]
    db = tmp_path / "bulletin.db"
    try:
        run(site, db, clock)
        before = items_of(db)
        n1_version, n2_version = before["N-1"].version, before["N-2"].version

        clock.advance(DAY)
        # Only the listing changes: status flips, detail page byte-identical.
        site.announcements[0].status = "withdrawn"
        site.announcements[0].updated = "2026-11-09"
        run(site, db, clock)
        after = items_of(db)
        assert after["N-1"].version == n1_version + 1, "a real listing-side change makes exactly one new version"
        assert after["N-1"].data["status"] == "withdrawn"
        assert after["N-1"].data["body"] == "body", "detail fields survive untouched"
        assert after["N-2"].version == n2_version, "an unchanged record gains no version"

        # A second recrawl with nothing changed produces no further versions.
        clock.advance(DAY)
        run(site, db, clock)
        assert items_of(db)["N-1"].version == n1_version + 1
    finally:
        site.stop()


def test_pushed_to_later_page_is_not_deleted_withdrawal_is(tmp_path: Path) -> None:
    clock = FakeClock()
    site = AnnouncementSite(per_page=2).start()
    site.announcements = [
        Announcement("N-1", "published", "d1", "T1", "A", "b1"),
        Announcement("N-2", "published", "d2", "T2", "A", "b2"),
        Announcement("N-3", "published", "d3", "T3", "A", "b3"),
    ]
    db = tmp_path / "bulletin.db"
    try:
        run(site, db, clock)
        assert set(items_of(db)) == {"N-1", "N-2", "N-3"}

        # A new announcement pushes N-3 from page 1 onto page 2; nothing is withdrawn.
        clock.advance(DAY)
        site.announcements.insert(0, Announcement("N-0", "published", "d0", "T0", "A", "b0"))
        run(site, db, clock)
        assert set(items_of(db)) == {"N-0", "N-1", "N-2", "N-3"}, "pagination is not deletion"

        # N-3 is removed from the listing entirely.
        clock.advance(DAY)
        site.announcements = [a for a in site.announcements if a.doc_no != "N-3"]
        run(site, db, clock)
        live = items_of(db)
        assert "N-3" not in live and set(live) == {"N-0", "N-1", "N-2"}
        with Store(db) as store:
            n3 = list(store.items.versions("bulletin", key="N-3"))
            assert n3[-1].deleted is True
    finally:
        site.stop()
