"""HTTP messages as WARC blocks, and the records one fetch produces.

A response block holds the status line and header fields as received
followed by the entity body with its content coding intact (gzip stays
gzip) but any transfer coding removed. Because the body is no longer
chunked, a received ``Transfer-Encoding`` field is kept under the name
``X-Archive-Orig-Transfer-Encoding`` so that replay tools do not try to
de-chunk it again.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from skein.warc.record import (
    HTTP_REQUEST_TYPE,
    HTTP_RESPONSE_TYPE,
    REVISIT_IDENTICAL,
    REVISIT_NOT_MODIFIED,
    WarcRecord,
    make_record,
    new_record_id,
    sha1_digest,
    warc_date,
)

ORIG_PREFIX = "X-Archive-Orig-"
_HOP_BY_HOP_RENAMED = frozenset({"transfer-encoding"})


def request_block(method: str, target: str, headers: list[tuple[str, str]]) -> bytes:
    lines = [f"{method} {target} HTTP/1.1", *(f"{name}: {value}" for name, value in headers)]
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1", "replace")


def response_head(version: str, status: int, reason: str, headers: list[tuple[str, str]]) -> bytes:
    lines = [f"{version} {status} {reason}".rstrip()]
    for name, value in headers:
        if name.lower() in _HOP_BY_HOP_RENAMED:
            name = ORIG_PREFIX + name
        lines.append(f"{name}: {value}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1", "replace")


def parse_http_head(head: bytes) -> tuple[str, list[tuple[str, str]]]:
    """Start line and header fields of an archived HTTP message head."""
    lines = head.decode("latin-1").split("\r\n")
    fields: list[tuple[str, str]] = []
    for line in lines[1:]:
        if not line:
            continue
        name, _, value = line.partition(":")
        fields.append((name.strip(), value.strip()))
    return lines[0], fields


@dataclass
class Exchange:
    """Everything about one HTTP fetch that goes into the archive."""

    url: str
    fetched_at: float
    method: str
    request_target: str
    request_headers: list[tuple[str, str]]
    http_version: str
    status: int
    reason: str
    response_headers: list[tuple[str, str]]
    payload: bytes
    ip_address: str | None = None
    metadata: list[tuple[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class RevisitOf:
    """The earlier capture a revisit record refers to."""

    profile: str
    record_id: str | None
    target_uri: str
    date: str | None


def exchange_records(exchange: Exchange, *, revisit: RevisitOf | None, software: str) -> list[WarcRecord]:
    """The request, response (or revisit) and optional metadata records of one exchange.

    A revisit record keeps only the HTTP head; its payload digest is the
    one of the original capture (identical-payload) or of the empty 304
    body (server-not-modified).
    """
    date = warc_date(exchange.fetched_at)
    response_id = new_record_id()
    head = response_head(exchange.http_version, exchange.status, exchange.reason, exchange.response_headers)
    payload_digest = sha1_digest(exchange.payload)
    concurrent = [("WARC-IP-Address", exchange.ip_address)] if exchange.ip_address else []
    if revisit is None:
        main = make_record("response", head + exchange.payload, date=date, content_type=HTTP_RESPONSE_TYPE,
                           target_uri=exchange.url, payload_digest=payload_digest, record_id=response_id,
                           extra=concurrent)
    else:
        extra = [*concurrent, ("WARC-Profile", revisit.profile), ("WARC-Refers-To-Target-URI", revisit.target_uri)]
        if revisit.record_id:
            extra.append(("WARC-Refers-To", revisit.record_id))
        if revisit.date:
            extra.append(("WARC-Refers-To-Date", revisit.date))
        main = make_record("revisit", head, date=date, content_type=HTTP_RESPONSE_TYPE, target_uri=exchange.url,
                           payload_digest=payload_digest, record_id=response_id, extra=extra)
    request = make_record(
        "request", request_block(exchange.method, exchange.request_target, exchange.request_headers),
        date=date, content_type=HTTP_REQUEST_TYPE, target_uri=exchange.url,
        extra=[("WARC-Concurrent-To", response_id)],
    )
    records = [request, main]
    if exchange.metadata:
        body = "".join(f"{name}: {value}\r\n" for name, value in [("software", software), *exchange.metadata])
        records.append(make_record("metadata", body.encode("utf-8"), date=date,
                                   content_type="application/warc-fields", target_uri=exchange.url,
                                   extra=[("WARC-Refers-To", response_id)]))
    return records


def revisit_profile(status: int, payload_digest: str, previous_digest: str | None) -> str | None:
    """Which revisit profile applies: 304 -> server-not-modified, same payload -> identical-payload-digest."""
    if status == 304:
        return REVISIT_NOT_MODIFIED
    if previous_digest is not None and previous_digest == payload_digest and 200 <= status < 300:
        return REVISIT_IDENTICAL
    return None
