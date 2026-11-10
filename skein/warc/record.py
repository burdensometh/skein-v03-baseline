"""WARC 1.1 records: named header fields plus a content block (ISO 28500:2017).

A record is serialised as::

    WARC/1.1\\r\\n
    Name: value\\r\\n          (one line per field, order preserved)
    \\r\\n
    <Content-Length bytes of block>
    \\r\\n\\r\\n

``WARC-Block-Digest`` covers the whole block; ``WARC-Payload-Digest`` covers
the payload, which for ``application/http`` blocks is the entity body after
the HTTP header (with any transfer coding removed). Digests are written as
``sha1:<base32>``, the form the WARC tools ecosystem expects.
"""

from __future__ import annotations

import base64
import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

WARC_VERSION = "WARC/1.1"
ACCEPTED_VERSIONS = ("WARC/1.1", "WARC/1.0")
RECORD_TYPES = ("warcinfo", "response", "resource", "request", "metadata", "revisit", "conversion", "continuation")
HTTP_RESPONSE_TYPE = "application/http;msgtype=response"
HTTP_REQUEST_TYPE = "application/http;msgtype=request"
REVISIT_NOT_MODIFIED = "http://netpreserve.org/warc/1.1/revisit/server-not-modified"
REVISIT_IDENTICAL = "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest"
MAX_HEADER_BYTES = 64 * 1024


class WarcFormatError(ValueError):
    """Bytes that do not form a valid WARC record."""


def new_record_id() -> str:
    return f"<urn:uuid:{uuid.uuid4()}>"


def warc_date(epoch: float) -> str:
    """WARC 1.1 date with microseconds, e.g. ``2024-05-01T12:00:00.250000Z``."""
    moment = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_warc_date(text: str) -> float:
    """Epoch seconds of a WARC-Date (with or without fractional seconds)."""
    cleaned = text.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned).timestamp()
    except ValueError as exc:
        raise WarcFormatError(f"invalid WARC-Date {text!r}") from exc


def sha1_digest(data: bytes) -> str:
    return "sha1:" + base64.b32encode(hashlib.sha1(data).digest()).decode("ascii")


def digest_matches(label: str, data: bytes) -> bool:
    """Check a ``algorithm:value`` digest label (sha1 base32/hex, sha256 hex) against ``data``."""
    algorithm, _, value = label.partition(":")
    algorithm = algorithm.lower()
    if algorithm not in ("sha1", "sha256"):
        return False
    raw = hashlib.new(algorithm, data).digest()
    return value.upper() in (base64.b32encode(raw).decode("ascii"), raw.hex().upper())


@dataclass
class WarcRecord:
    """Header fields (in order) and the content block."""

    headers: list[tuple[str, str]] = field(default_factory=list)
    block: bytes = b""
    version: str = WARC_VERSION

    def get(self, name: str, default: str | None = None) -> str | None:
        lowered = name.lower()
        for key, value in self.headers:
            if key.lower() == lowered:
                return value
        return default

    @property
    def type(self) -> str:
        return self.get("WARC-Type") or ""

    @property
    def record_id(self) -> str:
        return self.get("WARC-Record-ID") or ""

    @property
    def target_uri(self) -> str | None:
        return self.get("WARC-Target-URI")

    @property
    def date(self) -> str:
        return self.get("WARC-Date") or ""

    @property
    def is_http(self) -> bool:
        return (self.get("Content-Type") or "").replace(" ", "").lower().startswith("application/http")

    def http_split(self) -> tuple[bytes, bytes]:
        """``(http_head, payload)`` of an ``application/http`` block (head includes the blank line)."""
        end = self.block.find(b"\r\n\r\n")
        if end < 0:
            return self.block, b""
        return self.block[: end + 4], self.block[end + 4 :]

    @property
    def payload(self) -> bytes:
        return self.http_split()[1] if self.is_http else self.block

    def verify(self) -> list[str]:
        """Problems with the stored digests (empty when they match or are absent)."""
        problems = []
        block_digest = self.get("WARC-Block-Digest")
        if block_digest and not digest_matches(block_digest, self.block):
            problems.append("WARC-Block-Digest mismatch")
        payload_digest = self.get("WARC-Payload-Digest")
        if payload_digest and self.type != "revisit" and not digest_matches(payload_digest, self.payload):
            problems.append("WARC-Payload-Digest mismatch")
        return problems

    def serialize(self) -> bytes:
        lines = [self.version]
        lines.extend(f"{name}: {value}" for name, value in self.headers if name.lower() != "content-length")
        lines.append(f"Content-Length: {len(self.block)}")
        head = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8")
        return head + self.block + b"\r\n\r\n"

    @classmethod
    def parse(cls, data: bytes) -> "WarcRecord":
        """Parse one serialised record (trailing bytes after the record end are rejected)."""
        record, end = parse_record(data, 0)
        if data[end:].strip(b"\r\n"):
            raise WarcFormatError(f"{len(data) - end} unexpected byte(s) after the record")
        return record


def parse_record(data: bytes, start: int) -> tuple[WarcRecord, int]:
    """Parse the record beginning at ``start``; returns it and the offset just past its trailer."""
    head_end = data.find(b"\r\n\r\n", start, start + MAX_HEADER_BYTES)
    if head_end < 0:
        raise WarcFormatError("record header not terminated")
    lines = data[start:head_end].decode("utf-8", "replace").split("\r\n")
    version = lines[0].strip()
    if version not in ACCEPTED_VERSIONS:
        raise WarcFormatError(f"unsupported WARC version line {version!r}")
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if line[:1] in (" ", "\t") and headers:
            name, value = headers[-1]
            headers[-1] = (name, value + " " + line.strip())
            continue
        name, sep, value = line.partition(":")
        if not sep or not name.strip():
            raise WarcFormatError(f"malformed header line {line[:80]!r}")
        headers.append((name.strip(), value.strip()))
    record = WarcRecord(headers, b"", version)
    try:
        length = int(record.get("Content-Length") or "")
    except ValueError as exc:
        raise WarcFormatError("missing or invalid Content-Length") from exc
    body_start = head_end + 4
    if body_start + length > len(data):
        raise WarcFormatError(f"block truncated: {len(data) - body_start} of {length} byte(s)")
    record.block = data[body_start : body_start + length]
    if record.type not in RECORD_TYPES:
        raise WarcFormatError(f"unknown WARC-Type {record.type!r}")
    end = body_start + length
    if data[end : end + 4] == b"\r\n\r\n":
        end += 4
    return record, end


def make_record(
    record_type: str,
    block: bytes,
    *,
    date: str,
    content_type: str | None = None,
    target_uri: str | None = None,
    payload_digest: str | None = None,
    record_id: str | None = None,
    extra: list[tuple[str, str]] | None = None,
) -> WarcRecord:
    """A record with the mandatory fields, ``WARC-Block-Digest`` and the given optional ones."""
    if record_type not in RECORD_TYPES:
        raise ValueError(f"unknown WARC record type {record_type!r}")
    headers = [("WARC-Type", record_type), ("WARC-Record-ID", record_id or new_record_id()), ("WARC-Date", date)]
    if target_uri is not None:
        headers.append(("WARC-Target-URI", target_uri))
    headers.extend(extra or [])
    if content_type is not None:
        headers.append(("Content-Type", content_type))
    headers.append(("WARC-Block-Digest", sha1_digest(block)))
    if payload_digest is not None:
        headers.append(("WARC-Payload-Digest", payload_digest))
    return WarcRecord(headers, block)
