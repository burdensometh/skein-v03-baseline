"""The asyncio HTTP/1.1 client against the real fixture server and in-memory streams."""

from __future__ import annotations

import asyncio
import gzip
import zlib

import pytest

from skein.clock import FakeClock
from skein.net import (
    BodyTooLarge,
    ClientConfig,
    ConnectError,
    ConnectionClosed,
    DecodingError,
    Headers,
    HttpClient,
    InvalidURL,
    ProtocolError,
    RedirectError,
    RedirectLoop,
    TimeoutError,
    TooManyRedirects,
)
from skein.net.charset import detect_charset, normalize_charset, prescan_meta
from skein.net.content import decode_content
from skein.net.headers import parse_header_params
from skein.net.http11 import Connection, ResponseHead, content_length, parse_status_line
from tests.fixtures_site import FixtureSite


def fetch(site: FixtureSite, path: str, **config: object):  # noqa: ANN201
    async def go():  # noqa: ANN202
        async with HttpClient(ClientConfig(**config)) as client:  # type: ignore[arg-type]
            return await client.request("GET", site.url(path))

    return asyncio.run(go())


# -- wire-level parsing -------------------------------------------------------


class _Writer:
    def __init__(self) -> None:
        self.transport = None

    def is_closing(self) -> bool:
        return False


def _connection(raw: bytes) -> Connection:
    reader = asyncio.StreamReader()
    reader.feed_data(raw)
    reader.feed_eof()
    return Connection(reader, _Writer(), "http://t:80", FakeClock())  # type: ignore[arg-type]


def test_status_line_parsing() -> None:
    assert parse_status_line(b"HTTP/1.1 404 Not Found\r\n") == ("HTTP/1.1", 404, "Not Found")
    assert parse_status_line(b"HTTP/1.0 200\r\n") == ("HTTP/1.0", 200, "")
    for bad in (b"ICY 200 OK\r\n", b"HTTP/2 200 OK\r\n", b"HTTP/1.1 20 OK\r\n"):
        with pytest.raises(ProtocolError):
            parse_status_line(bad)


def test_content_length_validation() -> None:
    assert content_length(Headers([("Content-Length", "10"), ("Content-Length", "10")])) == 10
    with pytest.raises(ProtocolError):
        content_length(Headers([("Content-Length", "10"), ("Content-Length", "11")]))
    with pytest.raises(ProtocolError):
        content_length(Headers([("Content-Length", "-1")]))


def test_chunked_body_with_extensions_and_trailers() -> None:
    raw = (
        b"HTTP/1.1 100 Continue\r\n\r\n"
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX-Folded: a\r\n  b\r\n\r\n"
        b"5;name=val\r\nhello\r\n6\r\n world\r\n0\r\nX-Checksum: 42\r\n\r\n"
    )

    async def go():  # noqa: ANN202
        conn = _connection(raw)
        head = await conn.read_head(1.0)
        body = await conn.read_body(head, "GET", 1000, 1.0)
        return head, body

    head, body = asyncio.run(go())
    assert head.status == 200
    assert head.headers["X-Folded"] == "a b"
    assert body.data == b"hello world"
    assert body.trailers["X-Checksum"] == "42"
    assert body.reusable


def test_bad_chunk_size_is_protocol_error() -> None:
    raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nhello\r\n0\r\n\r\n"

    async def go() -> None:
        conn = _connection(raw)
        head = await conn.read_head(1.0)
        await conn.read_body(head, "GET", 1000, 1.0)

    with pytest.raises(ProtocolError):
        asyncio.run(go())


def test_body_framing_rules() -> None:
    async def go(raw: bytes, method: str = "GET") -> tuple[ResponseHead, bytes, bool]:
        conn = _connection(raw)
        head = await conn.read_head(1.0)
        body = await conn.read_body(head, method, 1000, 1.0)
        return head, body.data, body.reusable

    _, data, reusable = asyncio.run(go(b"HTTP/1.1 200 OK\r\n\r\nuntil close"))
    assert (data, reusable) == (b"until close", False)
    _, data, reusable = asyncio.run(go(b"HTTP/1.1 304 Not Modified\r\nContent-Length: 50\r\n\r\n"))
    assert (data, reusable) == (b"", True)
    _, data, _ = asyncio.run(go(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello", "HEAD"))
    assert data == b""
    head, _, reusable = asyncio.run(go(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok"))
    assert not head.keep_alive and not reusable


def test_header_param_parsing() -> None:
    assert parse_header_params('text/html; charset="utf-8"; q=0.5') == ("text/html", {"charset": "utf-8", "q": "0.5"})
    assert parse_header_params('attachment; filename="a;b.txt"')[1]["filename"] == "a;b.txt"
    assert parse_header_params("TEXT/PLAIN;;junk")[0] == "text/plain"


def test_content_decoding_helpers() -> None:
    payload = b"spam " * 100
    assert decode_content(gzip.compress(payload), ["gzip"], 10_000) == payload
    assert decode_content(gzip.compress(b"a") + gzip.compress(b"b"), ["gzip"], 100) == b"ab"
    assert decode_content(zlib.compress(payload), ["deflate"], 10_000) == payload
    assert decode_content(gzip.compress(zlib.compress(payload)), ["deflate", "gzip"], 10_000) == payload
    with pytest.raises(BodyTooLarge):
        decode_content(gzip.compress(b"\0" * 100_000), ["gzip"], 1000)
    with pytest.raises(DecodingError):
        decode_content(b"not gzip at all", ["gzip"], 1000)
    with pytest.raises(DecodingError):
        decode_content(b"x", ["br"], 1000)


def test_charset_detection_precedence() -> None:
    assert detect_charset(b"\xef\xbb\xbf<p>", "text/html; charset=latin1").source == "bom"
    assert detect_charset(b"<p>", "text/html; charset=ISO-8859-1").encoding == "cp1252"
    assert detect_charset(b"<meta charset='gb2312'>", "text/html").encoding == "gb18030"
    assert detect_charset(b'<?xml version="1.0" encoding="Shift_JIS"?><a/>', "text/xml").source == "xml"
    assert detect_charset("naïve".encode("utf-8")).encoding == "utf-8"
    assert detect_charset("naïve".encode("cp1252")).source == "fallback"
    assert prescan_meta(b"<!-- <meta charset=koi8-r> --><meta charset=utf-16>") == "utf-8"
    assert normalize_charset("no-such-charset") is None


# -- against the fixture site --------------------------------------------------


def test_keep_alive_connection_is_reused(site: FixtureSite) -> None:
    async def go() -> HttpClient:
        client = HttpClient()
        for path in ("/", "/about", "/catalog?page=1", "/chunked"):
            response = await client.request("GET", site.url(path))
            assert response.status == 200
        await client.close()
        return client

    client = asyncio.run(go())
    assert site.connections_accepted == 1
    assert client.pool.stats.connections_reused == 3


def test_connection_close_prevents_reuse(make_site) -> None:  # noqa: ANN001
    site = make_site(keep_alive=False)

    async def go() -> None:
        async with HttpClient() as client:
            for _ in range(3):
                assert (await client.request("GET", site.url("/"))).ok

    asyncio.run(go())
    assert site.connections_accepted == 3


def test_request_headers_sent(site: FixtureSite) -> None:
    response = fetch(site, "/echo", user_agent="probe/1.0", default_headers={"X-Team": "data"})
    text = response.text
    assert f"host: 127.0.0.1:{site.port}" in text
    assert "user-agent: probe/1.0" in text
    assert "accept-encoding: gzip, deflate" in text
    assert "x-team: data" in text


@pytest.mark.parametrize("path", ["/chunked", "/chunked?gzip=1", "/gzip", "/deflate", "/deflate-raw"])
def test_transfer_and_content_codings(site: FixtureSite, path: str) -> None:
    response = fetch(site, path)
    assert response.status == 200
    assert response.text.startswith("<!DOCTYPE html>")
    assert response.text.rstrip().endswith("</html>")


def test_chunked_trailers_exposed(site: FixtureSite) -> None:
    assert fetch(site, "/chunked").trailers["X-Trailer"] == "done"


@pytest.mark.parametrize(
    "path,needle,source",
    [
        ("/charset/latin1", "Café crème brûlée", "header"),
        ("/charset/meta-gbk", "商品目录", "meta"),
        ("/charset/http-equiv-sjis", "日本語のページ", "meta"),
        ("/charset/bom-utf8", "Café crème brûlée", "bom"),
        ("/charset/bom-utf16", "Ünïcödé", "bom"),
        ("/charset/undeclared-cp1252", "naïve “quotes”", "fallback"),
    ],
)
def test_charset_handling(site: FixtureSite, path: str, needle: str, source: str) -> None:
    response = fetch(site, path)
    assert needle in response.text
    assert response.charset.source == source
    assert "﻿" not in response.text


def test_redirect_chain_with_relative_locations(site: FixtureSite) -> None:
    response = fetch(site, "/redirect/chain/5")
    assert response.status == 200
    assert response.url == site.url("/redirect/chain/0")
    assert [hop.status for hop in response.history] == [302, 307, 302, 307, 302]
    assert response.request_url == site.url("/redirect/chain/5")
    assert response.history[0].location == "../chain/./4"
    assert response.history[1].location == "/redirect/chain/3"


def test_redirect_limit(site: FixtureSite) -> None:
    with pytest.raises(TooManyRedirects):
        fetch(site, "/redirect/chain/5", max_redirects=3)


def test_redirect_loop_detected(site: FixtureSite) -> None:
    with pytest.raises(RedirectLoop) as info:
        fetch(site, "/redirect/loop/a")
    assert "/redirect/loop/b" in str(info.value)


def test_redirect_to_unsupported_scheme(site: FixtureSite) -> None:
    with pytest.raises(RedirectError):
        fetch(site, "/redirect/bad-scheme")


def test_redirect_filter_stops_following(site: FixtureSite) -> None:
    async def go():  # noqa: ANN202
        async with HttpClient() as client:
            return await client.request("GET", site.url("/redirect/chain/3"), redirect_filter=lambda url: False)

    response = asyncio.run(go())
    assert response.status == 302 and response.is_redirect
    assert response.history == []


def test_read_timeout(site: FixtureSite) -> None:
    with pytest.raises(TimeoutError) as info:
        fetch(site, "/slow?delay=3", read_timeout=0.2)
    assert info.value.phase == "read"


def test_total_timeout_on_trickling_body(site: FixtureSite) -> None:
    with pytest.raises(TimeoutError) as info:
        fetch(site, "/slow-body?delay=0.1", read_timeout=1.0, total_timeout=0.3)
    assert info.value.phase == "total"


def test_connection_reset(site: FixtureSite) -> None:
    with pytest.raises(ConnectionClosed):
        fetch(site, "/reset")


def test_truncated_body(site: FixtureSite) -> None:
    with pytest.raises(ConnectionClosed, match="5 of 100"):
        fetch(site, "/truncated")


def test_body_limit_on_declared_length_and_chunked(site: FixtureSite) -> None:
    with pytest.raises(BodyTooLarge):
        fetch(site, "/big?size=200000", max_body_bytes=100_000)
    with pytest.raises(BodyTooLarge):
        fetch(site, "/big?size=200000&chunked=1", max_body_bytes=100_000)
    assert len(fetch(site, "/big?size=50000&chunked=1", max_body_bytes=100_000).body) == 50_000


def test_decompression_bomb_is_capped(site: FixtureSite) -> None:
    with pytest.raises(BodyTooLarge):
        fetch(site, "/bomb", max_body_bytes=1024 * 1024)


def test_http10_read_until_close(site: FixtureSite) -> None:
    response = fetch(site, "/http10")
    assert response.http_version == "HTTP/1.0"
    assert response.body == b"<p>old server</p>"


def test_error_statuses_are_responses_not_exceptions(site: FixtureSite) -> None:
    response = fetch(site, "/status/503")
    assert response.status == 503 and not response.ok


def test_connection_after_failure_is_not_reused(site: FixtureSite) -> None:
    async def go() -> None:
        async with HttpClient() as client:
            with pytest.raises(ConnectionClosed):
                await client.request("GET", site.url("/reset"))
            assert (await client.request("GET", site.url("/"))).ok

    asyncio.run(go())
    assert site.connections_accepted == 2


def test_stale_keep_alive_is_retried_transparently(site: FixtureSite) -> None:
    async def go() -> HttpClient:
        client = HttpClient()
        assert (await client.request("GET", site.url("/lying-keepalive"))).ok
        assert client.pool.idle_count() == 1
        await asyncio.sleep(0.2)
        # The server closed the connection without saying so. Pretend the
        # liveness probe missed it, as happens when the FIN is still in flight.
        for slot in client.pool._slots.values():
            for conn in slot.idle:
                conn.looks_alive = lambda: True  # type: ignore[method-assign]
        response = await client.request("GET", site.url("/about"))
        await client.close()
        assert response.ok
        return client

    client = asyncio.run(go())
    assert client.pool.stats.connections_reused == 1
    assert client.pool.stats.connections_opened == 2
    assert site.paths().count("/about") == 1


def test_connect_refused() -> None:
    async def go() -> None:
        async with HttpClient(ClientConfig(connect_timeout=1.0)) as client:
            await client.request("GET", "http://127.0.0.1:1/")

    with pytest.raises(ConnectError):
        asyncio.run(go())


@pytest.mark.parametrize("url", ["ftp://example.com/", "http:///nohost", "mailto:x@y"])
def test_invalid_urls(url: str) -> None:
    async def go() -> None:
        async with HttpClient() as client:
            await client.request("GET", url)

    with pytest.raises(InvalidURL):
        asyncio.run(go())


def test_concurrent_requests_respect_pool_limit(site: FixtureSite) -> None:
    async def go() -> None:
        async with HttpClient(ClientConfig(max_connections_per_origin=2)) as client:
            results = await asyncio.gather(*(client.request("GET", site.url(f"/product/{i}")) for i in range(1, 9)))
            assert all(r.ok for r in results)

    asyncio.run(go())
    assert site.connections_accepted <= 2


def test_closed_client_refuses_requests(site: FixtureSite) -> None:
    async def go() -> None:
        client = HttpClient()
        assert (await client.request("GET", site.url("/"))).ok
        await client.close()
        await client.close()
        await client.request("GET", site.url("/"))

    with pytest.raises(ConnectError, match="client is closed"):
        asyncio.run(go())
