"""An HTTP server replaying archived captures, for audits and spot checks.

Routes::

    GET /web/<timestamp>/<url>   capture of <url> closest to the 14-digit <timestamp>
    GET /web/latest/<url>        newest capture of <url>
    GET /cdx?url=<url>[&matchType=exact|prefix|host|domain][&from=TS][&to=TS][&filter=status:200]
            [&limit=N][&output=json]   captures, as CDX lines or JSON objects
    GET /timemap/link/<url>      Memento TimeMap (RFC 7089, application/link-format)

HTML pages are served with their links rewritten into the archive (see
:mod:`skein.warc.rewrite`) unless ``rewrite`` is off.

A replayed response carries the archived status and header fields; since
the body is served decoded, ``Content-Encoding`` and the original
``Content-Length`` are replaced. ``Memento-Datetime`` (RFC 7089) gives the
capture time and ``X-Archive-Src`` the WARC file and offset it came from.
"""

from __future__ import annotations

import codecs
import json
from datetime import datetime, timezone
from email.utils import formatdate
from urllib.parse import parse_qs, unquote, urlsplit

from skein.net.server import Reply, SimpleServer
from skein.warc.cdx import CdxIndex, timestamp14
from skein.warc.record import WarcFormatError, parse_warc_date
from skein.warc.replay import ReplayError, Replayer
from skein.warc.rewrite import rewrite_html

_DROPPED = frozenset({"content-encoding", "content-length", "transfer-encoding", "connection", "keep-alive"})


class ReplayServer(SimpleServer):
    """Serves ``replayer``'s captures on ``host:port`` (port 0 picks a free port)."""

    def __init__(self, replayer: Replayer, *, host: str = "127.0.0.1", port: int = 0, rewrite: bool = True) -> None:
        super().__init__(self.answer, host=host, port=port)
        self.replayer = replayer
        self.rewrite = rewrite
        self.served = 0

    @property
    def index(self) -> CdxIndex:
        return self.replayer.index

    def answer(self, method: str, target: str) -> Reply:
        """The reply to one request (no I/O besides reading WARC files)."""
        if method not in ("GET", "HEAD"):
            return Reply.text(405, "Method Not Allowed", "method not allowed\n")
        split = urlsplit(target)
        if split.path == "/cdx":
            return self._cdx(parse_qs(split.query))
        if split.path.startswith("/timemap/link/"):
            return self._timemap(target[len("/timemap/link/"):])
        if not split.path.startswith("/web/"):
            return Reply.text(404, "Not Found", "not found\n")
        stamp, _, url = target[len("/web/"):].partition("/")
        url = unquote(url) if "://" not in url else url
        if not url or not (stamp == "latest" or (stamp.isdigit() and len(stamp) <= 14)):
            return Reply.text(400, "Bad Request", "use /web/<timestamp>/<url>\n")
        try:
            replayed = self.replayer.replay(url, None if stamp == "latest" else stamp)
        except ReplayError as exc:
            return Reply.text(404, "Not Found", f"{exc}\n")
        except (WarcFormatError, OSError) as exc:
            return Reply.text(502, "Bad Gateway", f"archive damaged: {exc}\n")
        headers = [(name, value) for name, value in replayed.headers if name.lower() not in _DROPPED]
        headers.append(("Memento-Datetime", formatdate(parse_warc_date(replayed.date), usegmt=True)))
        headers.append(("X-Archive-Src", f"{replayed.filename}#{replayed.offset}"))
        body = replayed.body
        content_type = (replayed.header("Content-Type") or "").lower()
        if self.rewrite and content_type.startswith("text/html"):
            charset = content_type.partition("charset=")[2].split(";")[0].strip() or "utf-8"
            try:
                codecs.lookup(charset)
            except LookupError:
                charset = "utf-8"
            text, rewritten = rewrite_html(body.decode(charset, "replace"), url, timestamp14(replayed.date))
            body = text.encode(charset, "replace")
            headers.append(("X-Archive-Rewritten-Links", str(rewritten)))
        self.served += 1
        return Reply(replayed.status, replayed.reason or "OK", body, headers)

    def _timemap(self, url: str) -> Reply:
        """Every capture of ``url`` as Memento link-format entries, oldest first."""
        url = unquote(url) if "://" not in url else url
        captures = self.index.captures(url)
        if not captures:
            return Reply.text(404, "Not Found", f"no capture of {url}\n")
        links = [f'<{url}>; rel="original"', f'</timemap/link/{url}>; rel="self"; type="application/link-format"']
        for position, line in enumerate(captures):
            moment = formatdate(datetime.strptime(line.timestamp, "%Y%m%d%H%M%S")
                                .replace(tzinfo=timezone.utc).timestamp(), usegmt=True)
            relation = "memento"
            if position == 0:
                relation = "first memento"
            if position == len(captures) - 1:
                relation = "last memento" if position else "first last memento"
            links.append(f'</web/{line.timestamp}/{url}>; rel="{relation}"; datetime="{moment}"')
        return Reply(200, "OK", (",\n".join(links) + "\n").encode("utf-8"),
                     [("Content-Type", "application/link-format")])

    def _cdx(self, params: dict[str, list[str]]) -> Reply:
        def first(name: str) -> str | None:
            values = params.get(name)
            return values[0] if values else None

        url = first("url") or ""
        status = None
        for condition in params.get("filter", []):
            field_name, _, value = condition.partition(":")
            if field_name in ("status", "statuscode"):
                status = value
        try:
            limit = int(first("limit") or 0) or None
            lines = self.index.query(url, match=first("matchType") or "exact", since=first("from"),
                                     until=first("to"), status=status, limit=limit)
        except ValueError as exc:
            return Reply.text(400, "Bad Request", f"{exc}\n")
        if first("output") == "json":
            rows = [{"urlkey": l.urlkey, "timestamp": l.timestamp, "url": l.original, "mime": l.mimetype,
                     "status": l.status, "digest": l.digest, "length": l.length, "offset": l.offset,
                     "filename": l.filename} for l in lines]
            return Reply(200, "OK", "".join(json.dumps(row) + "\n" for row in rows).encode("utf-8"),
                         [("Content-Type", "application/x-ndjson")])
        return Reply.text(200, "OK", "".join(line.to_line() + "\n" for line in lines))
