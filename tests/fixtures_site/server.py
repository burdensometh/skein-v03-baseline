"""Raw asyncio HTTP/1.1 server with deliberately misbehaving routes.

It is written against the socket API instead of ``http.server`` so that it can
send chunked bodies, trickle bytes, reset connections and lie about lengths.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import threading
import time
import zlib
from dataclasses import dataclass, field
from email.utils import formatdate, parsedate_to_datetime
from typing import Awaitable, Callable, Iterator
from urllib.parse import parse_qs, urlsplit

from tests.fixtures_site import content


@dataclass
class SiteOptions:
    """Knobs that let a test shape the site's behaviour."""

    catalog_pages: int = 3
    products_per_page: int = 4
    crawl_delay: float | None = None
    robots_status: int = 200
    robots_body: str | None = None
    product_lastmod: str = "2024-05-01"
    keep_alive: bool = True
    validators: str = "none"
    wall_clock: Callable[[], float] = time.time
    epoch: float = 1_600_000_000.0
    latency: float = 0.0
    noisy_footer: bool = False
    cache_control: str | None = None
    extra_links: tuple[str, ...] = ()
    articles: int = 4
    editions: tuple[str, ...] = ("morning", "evening")


@dataclass
class Reply:
    """Instructions for writing one response."""

    status: int = 200
    body: bytes = b""
    headers: list[tuple[str, str]] = field(default_factory=list)
    content_type: str | None = "text/html; charset=utf-8"
    chunked: bool = False
    chunk_size: int = 64
    delay: float = 0.0
    trickle: float = 0.0
    close: bool = False
    abort: bool = False
    declared_length: int | None = None
    http10: bool = False
    silent_close: bool = False


@dataclass
class RequestRecord:
    method: str
    path: str
    headers: dict[str, str]
    connection_id: int


REASONS = {
    200: "OK", 301: "Moved Permanently", 302: "Found", 303: "See Other", 304: "Not Modified",
    307: "Temporary Redirect", 308: "Permanent Redirect", 403: "Forbidden", 404: "Not Found",
    410: "Gone", 429: "Too Many Requests", 500: "Internal Server Error", 502: "Bad Gateway",
    503: "Service Unavailable",
}

Handler = Callable[[str, dict[str, list[str]], dict[str, str]], "Reply | Awaitable[Reply]"]


class FixtureSite:
    """The fixture web site. Start with :meth:`start` inside a running loop.

    Product content is versioned: :meth:`revise` bumps a product's revision
    (its price changes) and its modification time. With ``validators`` set
    to ``"both"``, ``"etag"`` or ``"last-modified"`` the home, catalog and
    product pages send the corresponding validators and answer conditional
    requests with 304. All dates come from ``options.wall_clock``, so tests
    can drive the site with the same fake clock as the crawler.
    """

    def __init__(self, options: SiteOptions | None = None) -> None:
        self.options = options or SiteOptions()
        self.requests: list[RequestRecord] = []
        self.hits: dict[str, int] = {}
        self.revisions: dict[int, int] = {}
        self.modified: dict[int, float] = {}
        self.not_modified = 0
        self._server: asyncio.base_events.Server | None = None
        self._connections = 0
        self._writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task] = set()
        self.port = 0
        self._routes: list[tuple[re.Pattern[str], Handler]] = [
            (re.compile(p), h)
            for p, h in [
                (r"/", self._home),
                (r"/robots\.txt", self._robots),
                (r"/catalog", self._catalog),
                (r"/product/(\d+)(?:;jsessionid=\w+)?", self._product),
                (r"/tracked", lambda *_: Reply(body=content.tracked_page().encode())),
                (r"/articles", lambda *_: Reply(body=content.articles_index(
                    self.options.articles, self.options.editions).encode())),
                (r"/article/(\d+)", self._article),
                (r"/about", lambda *_: Reply(body=content.layout("About", "<p>About us</p>").encode())),
                (r"/private/(.*)", lambda *_: Reply(body=content.layout("Private", "<p>staff</p>").encode())),
                (r"/broken", lambda *_: Reply(body=content.BROKEN_HTML.encode())),
                (r"/charset/(.+)", self._charset),
                (r"/redirect/chain/(\d+)", self._redirect_chain),
                (r"/redirect/loop/(a|b)", self._redirect_loop),
                (r"/redirect/to-product", lambda *_: Reply(status=301, headers=[("Location", "/product/1")])),
                (r"/redirect/to", self._redirect_to),
                (r"/redirect/bad-scheme", lambda *_: Reply(status=302, headers=[("Location", "ftp://x/")])),
                (r"/slow", self._slow),
                (r"/slow-body", self._slow_body),
                (r"/chunked", self._chunked),
                (r"/gzip", self._gzip),
                (r"/deflate", self._deflate),
                (r"/deflate-raw", self._deflate_raw),
                (r"/bomb", self._bomb),
                (r"/reset", lambda *_: Reply(abort=True)),
                (r"/truncated", lambda *_: Reply(body=b"short", declared_length=100, close=True)),
                (r"/lying-keepalive", lambda *_: Reply(body=b"<p>bye</p>", silent_close=True)),
                (r"/tagged/(\w+)", self._tagged),
                (r"/http10", lambda *_: Reply(body=b"<p>old server</p>", http10=True)),
                (r"/status/(\d{3})", self._status),
                (r"/flaky/(\w+)", self._flaky),
                (r"/limited/(\w+)", self._limited),
                (r"/busy/(\w+)", self._busy),
                (r"/big", self._big),
                (r"/sitemap\.xml", self._sitemap_products),
                (r"/sitemap-products\.xml", self._sitemap_products),
                (r"/sitemap-pages\.xml\.gz", self._sitemap_pages),
                (r"/sitemap_index\.xml\.gz", self._sitemap_index),
                (r"/echo", self._echo),
            ]
        ]

    # -- lifecycle -------------------------------------------------------

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, path: str) -> str:
        return self.base_url + path

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self._server = await asyncio.start_server(self._serve, host, port)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        for writer in list(self._writers):
            writer.close()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._server.wait_closed()
        self._server = None

    def paths(self) -> list[str]:
        return [r.path for r in self.requests]

    def revise(self, pid: int, at: float | None = None) -> None:
        """Change product ``pid`` (its price goes up) as of wall time ``at`` (default: now)."""
        self.revisions[pid] = self.revisions.get(pid, 0) + 1
        self.modified[pid] = self.options.wall_clock() if at is None else at

    def product_modified(self, pid: int) -> float:
        return self.modified.get(pid, self.options.epoch)

    def requests_for(self, path: str) -> list[RequestRecord]:
        return [r for r in self.requests if r.path == path]

    @property
    def connections_accepted(self) -> int:
        return self._connections

    # -- protocol --------------------------------------------------------

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._connections += 1
        conn_id = self._connections
        self._writers.add(writer)
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        try:
            while True:
                request = await self._read_request(reader)
                if request is None:
                    break
                method, target, headers = request
                self.requests.append(RequestRecord(method, target, headers, conn_id))
                reply = await self._dispatch(method, target, headers)
                keep = await self._write(writer, reply, method, headers)
                if not keep:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            if task is not None:
                self._tasks.discard(task)
            self._writers.discard(writer)
            if not writer.is_closing():
                writer.close()

    async def _read_request(self, reader: asyncio.StreamReader) -> tuple[str, str, dict[str, str]] | None:
        line = await reader.readline()
        if not line:
            return None
        parts = line.decode("latin-1").split()
        if len(parts) != 3:
            return None
        headers: dict[str, str] = {}
        while True:
            raw = await reader.readline()
            if raw in (b"\r\n", b"\n", b""):
                break
            name, _, value = raw.decode("latin-1").partition(":")
            headers[name.strip().lower()] = value.strip()
        length = int(headers.get("content-length", "0") or 0)
        if length:
            await reader.readexactly(length)
        return parts[0], parts[1], headers

    async def _dispatch(self, method: str, target: str, headers: dict[str, str]) -> Reply:
        split = urlsplit(target)
        query = parse_qs(split.query)
        self.hits[split.path] = self.hits.get(split.path, 0) + 1
        for pattern, handler in self._routes:
            match = pattern.fullmatch(split.path)
            if match:
                result = handler(match.group(1) if match.groups() else "", query, headers)
                if asyncio.iscoroutine(result):
                    result = await result
                return result  # type: ignore[return-value]
        return Reply(status=404, body=b"<h1>not found</h1>")

    async def _write(self, writer: asyncio.StreamWriter, reply: Reply, method: str,
                     req_headers: dict[str, str]) -> bool:
        if self.options.latency:
            await asyncio.sleep(self.options.latency)
        if reply.abort:
            writer.transport.abort()
            return False
        if reply.delay:
            await asyncio.sleep(reply.delay)
        keep = (
            self.options.keep_alive
            and not reply.close
            and not reply.silent_close
            and not reply.http10
            and req_headers.get("connection", "").lower() != "close"
        )
        version = "HTTP/1.0" if reply.http10 else "HTTP/1.1"
        lines = [f"{version} {reply.status} {REASONS.get(reply.status, 'Status')}"]
        if reply.content_type:
            lines.append(f"Content-Type: {reply.content_type}")
        lines.extend(f"{k}: {v}" for k, v in reply.headers)
        if reply.chunked:
            lines.append("Transfer-Encoding: chunked")
        elif not reply.http10:
            length = reply.declared_length if reply.declared_length is not None else len(reply.body)
            lines.append(f"Content-Length: {length}")
        if not keep and not reply.http10 and not reply.silent_close:
            lines.append("Connection: close")
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
        body = b"" if method == "HEAD" else reply.body
        if reply.chunked:
            for start in range(0, len(body), reply.chunk_size):
                piece = body[start : start + reply.chunk_size]
                writer.write(f"{len(piece):x};ext=1\r\n".encode() + piece + b"\r\n")
                if reply.trickle:
                    await writer.drain()
                    await asyncio.sleep(reply.trickle)
            writer.write(b"0\r\nX-Trailer: done\r\n\r\n")
        elif reply.trickle:
            for start in range(0, len(body), 16):
                writer.write(body[start : start + 16])
                await writer.drain()
                await asyncio.sleep(reply.trickle)
        else:
            writer.write(body)
        await writer.drain()
        if reply.silent_close:
            # Close a moment later, after the client has pooled the connection.
            await asyncio.sleep(0.05)
        return keep

    # -- validators ------------------------------------------------------

    def _http_date(self, epoch: float) -> str:
        return formatdate(epoch, usegmt=True)

    def _versioned(self, body: bytes, tag: str, modified: float, headers: dict[str, str]) -> Reply:
        """A 200 with validators, or a 304 when the request's validators still match."""
        mode = self.options.validators
        cache = [("Cache-Control", self.options.cache_control)] if self.options.cache_control else []
        if mode == "none":
            return Reply(body=body, headers=cache)
        etag = f'"{tag}"' if mode in ("both", "etag") else None
        stamp = self._http_date(modified) if mode in ("both", "last-modified") else None
        validator_headers = [("Date", self._http_date(self.options.wall_clock())), *cache]
        if etag is not None:
            validator_headers.append(("ETag", etag))
        if stamp is not None:
            validator_headers.append(("Last-Modified", stamp))
        if self._still_valid(headers, etag, modified):
            self.not_modified += 1
            return Reply(status=304, headers=validator_headers, content_type=None)
        return Reply(body=body, headers=validator_headers)

    def _still_valid(self, headers: dict[str, str], etag: str | None, modified: float) -> bool:
        if "if-none-match" in headers:
            # RFC 9110 13.1.2: weak comparison; If-Modified-Since is ignored when If-None-Match is present.
            if etag is None:
                return False
            offered = [t.strip().removeprefix("W/") for t in headers["if-none-match"].split(",")]
            return "*" in offered or etag.removeprefix("W/") in offered
        if "if-modified-since" in headers and self.options.validators in ("both", "last-modified"):
            try:
                since = parsedate_to_datetime(headers["if-modified-since"]).timestamp()
            except (TypeError, ValueError):
                return False
            return int(modified) <= since
        return False

    # -- routes ----------------------------------------------------------

    def _home(self, _: str, __: dict[str, list[str]], headers: dict[str, str]) -> Reply:
        page = content.home_page(self.options.extra_links).encode()
        return self._versioned(page, "home", self.options.epoch, headers)

    def _article(self, number: str, query: dict[str, list[str]], _: dict[str, str]) -> Reply:
        if not 1 <= int(number) <= self.options.articles:
            return Reply(status=404, body=b"no such story")
        return Reply(body=content.article_page(int(number), query.get("edition", [""])[0]).encode())

    def _robots(self, _: str, __: dict[str, list[str]], headers: dict[str, str]) -> Reply:
        opts = self.options
        if opts.robots_status != 200:
            return Reply(status=opts.robots_status, body=b"robots unavailable", content_type="text/plain")
        text = opts.robots_body if opts.robots_body is not None else content.robots_txt(self.base_url, opts.crawl_delay)
        reply = self._versioned(text.replace("{base}", self.base_url).encode(), "robots", opts.epoch, headers)
        reply.content_type = "text/plain" if reply.status == 200 else None
        return reply

    def _catalog(self, _: str, query: dict[str, list[str]], headers: dict[str, str]) -> Reply:
        try:
            page = int(query.get("page", ["1"])[0])
        except ValueError:
            return Reply(status=404, body=b"bad page")
        if not 1 <= page <= self.options.catalog_pages:
            return Reply(status=404, body=b"no such page")
        per_page = self.options.products_per_page
        pids = range((page - 1) * per_page + 1, page * per_page + 1)
        html = content.catalog_page(page, self.options.catalog_pages, per_page, self.revisions)
        tag = f"catalog{page}-" + ".".join(str(self.revisions.get(pid, 0)) for pid in pids)
        return self._versioned(html.encode(), tag, max(self.product_modified(pid) for pid in pids), headers)

    def _product(self, pid: str, _: dict[str, list[str]], headers: dict[str, str]) -> Reply:
        number = int(pid)
        if number < 1 or number > self.options.catalog_pages * self.options.products_per_page:
            return Reply(status=404, body=b"no such product")
        revision = self.revisions.get(number, 0)
        footer = f"Generated at {self._http_date(self.options.wall_clock())}" if self.options.noisy_footer else ""
        body = content.product_page(number, revision, footer).encode()
        return self._versioned(body, f"p{number}-r{revision}", self.product_modified(number), headers)

    def _charset(self, name: str, *_: object) -> Reply:
        text_fr = "Café crème brûlée"
        if name == "latin1":
            body = content.charset_page("latin1", text_fr).encode("latin-1")
            return Reply(body=body, content_type="text/html; charset=ISO-8859-1")
        if name == "meta-gbk":
            body = content.charset_page("gbk", "商品目录", '<meta charset="gbk">').encode("gbk")
            return Reply(body=body, content_type="text/html")
        if name == "http-equiv-sjis":
            meta = '<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">'
            body = content.charset_page("sjis", "日本語のページ", meta).encode("shift_jis")
            return Reply(body=body, content_type="text/html")
        if name == "bom-utf8":
            body = b"\xef\xbb\xbf" + content.charset_page("bom", text_fr).encode("utf-8")
            return Reply(body=body, content_type="text/html; charset=iso-8859-1")
        if name == "bom-utf16":
            body = content.charset_page("utf16", "Ünïcödé").encode("utf-16")
            return Reply(body=body, content_type="text/html")
        if name == "undeclared-cp1252":
            body = content.charset_page("cp1252", "naïve “quotes”").encode("cp1252")
            return Reply(body=body, content_type="text/html")
        return Reply(status=404)

    def _redirect_chain(self, n: str, *_: object) -> Reply:
        remaining = int(n)
        if remaining == 0:
            return Reply(body=b"<p>end of chain</p>")
        nxt = remaining - 1
        # Rotate through absolute, absolute-path and relative Location forms.
        style = remaining % 3
        if style == 0:
            location = f"{self.base_url}/redirect/chain/{nxt}"
        elif style == 1:
            location = f"/redirect/chain/{nxt}"
        else:
            location = f"../chain/./{nxt}"
        return Reply(status=302 if remaining % 2 else 307, headers=[("Location", location)])

    def _redirect_loop(self, which: str, *_: object) -> Reply:
        target = "b" if which == "a" else "a"
        return Reply(status=302, headers=[("Location", target)])

    def _redirect_to(self, _: str, query: dict[str, list[str]], __: dict[str, str]) -> Reply:
        return Reply(status=302, headers=[("Location", query.get("url", ["/"])[0])])

    def _tagged(self, mode: str, *_: object) -> Reply:
        tag = {"noindex": "noindex", "nofollow": "nofollow", "other": "otherbot: noindex, nofollow",
               "none": "none"}.get(mode, "all")
        return Reply(body=content.product_page(1).encode(), headers=[("X-Robots-Tag", tag)])

    def _slow(self, _: str, query: dict[str, list[str]], __: dict[str, str]) -> Reply:
        delay = float(query.get("delay", ["2"])[0])
        return Reply(body=b"<p>finally</p>", delay=delay)

    def _slow_body(self, _: str, query: dict[str, list[str]], __: dict[str, str]) -> Reply:
        delay = float(query.get("delay", ["0.5"])[0])
        return Reply(body=b"x" * 64, trickle=delay)

    def _chunked(self, _: str, query: dict[str, list[str]], __: dict[str, str]) -> Reply:
        html = content.catalog_page(1, 1, 3).encode()
        if query.get("gzip"):
            return Reply(body=content.gzip_bytes(html), chunked=True, chunk_size=50,
                         headers=[("Content-Encoding", "gzip")])
        return Reply(body=html, chunked=True, chunk_size=37)

    def _gzip(self, *_: object) -> Reply:
        html = content.product_page(2).encode()
        return Reply(body=content.gzip_bytes(html), headers=[("Content-Encoding", "gzip")])

    def _deflate(self, *_: object) -> Reply:
        html = content.product_page(3).encode()
        return Reply(body=zlib.compress(html), headers=[("Content-Encoding", "deflate")])

    def _deflate_raw(self, *_: object) -> Reply:
        html = content.product_page(3).encode()
        comp = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        raw = comp.compress(html) + comp.flush()
        return Reply(body=raw, headers=[("Content-Encoding", "deflate")])

    def _bomb(self, *_: object) -> Reply:
        payload = content.gzip_bytes(b"\0" * (8 * 1024 * 1024))
        return Reply(body=payload, headers=[("Content-Encoding", "gzip")])

    def _status(self, code: str, *_: object) -> Reply:
        return Reply(status=int(code), body=f"<p>status {code}</p>".encode())

    def _flaky(self, name: str, query: dict[str, list[str]], _: dict[str, str]) -> Reply:
        fail = int(query.get("fail", ["2"])[0])
        seen = self.hits[f"/flaky/{name}"]
        if seen <= fail:
            return Reply(status=503, body=b"try later")
        return Reply(body=content.layout("flaky", f'<p id="text">recovered after {fail}</p>').encode())

    def _retry_after_value(self, query: dict[str, list[str]]) -> str:
        seconds = int(query.get("after", ["5"])[0])
        if query.get("form", ["seconds"])[0] == "date":
            return self._http_date(self.options.wall_clock() + seconds)
        return str(seconds)

    def _limited(self, name: str, query: dict[str, list[str]], _: dict[str, str]) -> Reply:
        """429 with ``Retry-After`` for the first ``fail`` requests (``form=seconds|date``, ``after=N``)."""
        fail = int(query.get("fail", ["1"])[0])
        if self.hits[f"/limited/{name}"] <= fail:
            return Reply(status=429, body=b"slow down", headers=[("Retry-After", self._retry_after_value(query))])
        return Reply(body=content.layout("limited", f'<p id="text">served {name}</p>').encode())

    def _busy(self, name: str, query: dict[str, list[str]], _: dict[str, str]) -> Reply:
        """503 with ``Retry-After`` for the first ``fail`` requests."""
        fail = int(query.get("fail", ["1"])[0])
        if self.hits[f"/busy/{name}"] <= fail:
            return Reply(status=503, body=b"maintenance", headers=[("Retry-After", self._retry_after_value(query))])
        return Reply(body=content.layout("busy", f'<p id="text">back {name}</p>').encode())

    def _big(self, _: str, query: dict[str, list[str]], __: dict[str, str]) -> Reply:
        size = int(query.get("size", ["100000"])[0])
        return Reply(body=b"a" * size, chunked=bool(query.get("chunked")), chunk_size=8192,
                     content_type="text/plain")

    def _sitemap_products(self, *_: object) -> Reply:
        total = self.options.catalog_pages * self.options.products_per_page
        paths = [(f"/product/{pid}", self._lastmod(pid)) for pid in range(1, total + 1)]
        return Reply(body=content.urlset(self.base_url, paths), content_type="application/xml")

    def _lastmod(self, pid: int) -> str:
        if pid not in self.modified:
            return self.options.product_lastmod
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.modified[pid]))

    def _sitemap_pages(self, *_: object) -> Reply:
        paths = [("/", "2024-01-01T10:00:00+00:00"), ("/about", None), ("/private/secret", None)]
        return Reply(body=content.gzip_bytes(content.urlset(self.base_url, paths)),
                     content_type="application/x-gzip")

    def _sitemap_index(self, *_: object) -> Reply:
        children = [("/sitemap-products.xml", "2024-05-01"), ("/sitemap-pages.xml.gz", "2024-01-01")]
        return Reply(body=content.gzip_bytes(content.sitemap_index(self.base_url, children)),
                     content_type="application/gzip")

    def _echo(self, _: str, __: dict[str, list[str]], headers: dict[str, str]) -> Reply:
        lines = "\n".join(f"{k}: {v}" for k, v in sorted(headers.items()))
        return Reply(body=lines.encode(), content_type="text/plain")


@contextlib.contextmanager
def running_site(options: SiteOptions | None = None) -> Iterator[FixtureSite]:
    """Run a :class:`FixtureSite` on its own event loop in a background thread."""
    site = FixtureSite(options)
    loop = asyncio.new_event_loop()
    ready = threading.Event()
    failure: list[BaseException] = []

    def runner() -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(site.start())
        except BaseException as exc:  # pragma: no cover - surfaced to the test
            failure.append(exc)
            ready.set()
            return
        ready.set()
        loop.run_forever()
        loop.run_until_complete(site.stop())
        loop.close()

    thread = threading.Thread(target=runner, name="fixture-site", daemon=True)
    thread.start()
    ready.wait(10)
    if failure:
        raise failure[0]
    try:
        yield site
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(10)
