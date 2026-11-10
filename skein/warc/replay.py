"""Replaying archived responses: find a capture through the CDX index and rebuild the HTTP response.

A revisit record carries only headers; its content is the payload of the
capture it refers to, found by ``WARC-Refers-To-Target-URI`` and
``WARC-Refers-To-Date`` (or, failing that, by the payload digest). The
replayed body is content-decoded (gzip/deflate) like a live response.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from skein.net.content import decode_content
from skein.warc.cdx import CdxIndex, CdxLine, timestamp14
from skein.warc.http import ORIG_PREFIX, parse_http_head
from skein.warc.reader import read_record
from skein.warc.record import WarcFormatError, WarcRecord

MAX_REPLAY_BYTES = 64 * 1024 * 1024
MAX_REVISIT_HOPS = 8


class ReplayError(LookupError):
    """No usable capture for the requested URL."""


@dataclass
class Replayed:
    """A response rebuilt from the archive."""

    url: str
    status: int
    reason: str
    headers: list[tuple[str, str]]
    body: bytes
    record_id: str
    date: str
    filename: str
    offset: int
    revisit_chain: list[str] = field(default_factory=list)

    def header(self, name: str) -> str | None:
        lowered = name.lower()
        return next((value for key, value in self.headers if key.lower() == lowered), None)


def _status(start_line: str) -> tuple[int, str]:
    parts = start_line.split(" ", 2)
    try:
        return int(parts[1]), parts[2] if len(parts) > 2 else ""
    except (IndexError, ValueError) as exc:
        raise WarcFormatError(f"bad HTTP status line {start_line!r}") from exc


def _decoded(headers: list[tuple[str, str]], payload: bytes) -> bytes:
    codings = [token.strip().lower() for name, value in headers if name.lower() == "content-encoding"
               for token in value.split(",") if token.strip()]
    return decode_content(payload, codings, MAX_REPLAY_BYTES) if codings else payload


class Replayer:
    """Rebuilds responses from WARC files in ``directory`` using ``index``."""

    def __init__(self, index: CdxIndex, directory: Path) -> None:
        self.index = index
        self.directory = Path(directory)

    def _record(self, line: CdxLine) -> WarcRecord:
        return read_record(self.directory / line.filename, line.offset)

    def _original(self, record: WarcRecord) -> tuple[CdxLine, WarcRecord]:
        target = record.get("WARC-Refers-To-Target-URI") or record.target_uri or ""
        refers_date = record.get("WARC-Refers-To-Date")
        line: CdxLine | None = None
        if refers_date:
            stamp = timestamp14(refers_date)
            line = next((c for c in self.index.captures(target)
                         if c.timestamp == stamp and c.mimetype != "warc/revisit"), None)
        if line is None:
            digest = (record.get("WARC-Payload-Digest") or "").split(":", 1)[-1]
            line = self.index.by_digest(target, digest)
        if line is None:
            raise ReplayError(f"revisit of {target} refers to a capture that is not in the index")
        return line, self._record(line)

    def replay(self, url: str, timestamp: str | None = None) -> Replayed:
        line = self.index.closest(url, timestamp)
        if line is None:
            raise ReplayError(f"no capture of {url} in the index")
        record = self._record(line)
        head, _ = record.http_split()
        start, headers = parse_http_head(head)
        status, reason = _status(start)
        chain: list[str] = []
        content = record
        while content.type == "revisit":
            if len(chain) >= MAX_REVISIT_HOPS:
                raise ReplayError(f"revisit chain of {url} is too long")
            chain.append(content.record_id)
            _, content = self._original(content)
        _, payload = content.http_split()
        if status == 304:
            # Server-not-modified: present the archived content with the original status.
            original_head, _ = content.http_split()
            status, reason = _status(parse_http_head(original_head)[0])
            headers = parse_http_head(original_head)[1]
        restored = [(name[len(ORIG_PREFIX):] if name.startswith(ORIG_PREFIX) else name, value)
                    for name, value in headers]
        visible = [(name, value) for name, value in restored if name.lower() != "transfer-encoding"]
        return Replayed(url, status, reason, visible, _decoded(headers, payload), record.record_id, record.date,
                        line.filename, line.offset, chain)
