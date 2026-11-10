"""Content-coding decoders (gzip, deflate) with decompression-bomb protection."""

from __future__ import annotations

import zlib

from skein.net.errors import BodyTooLarge, DecodingError

SUPPORTED_CODINGS = ("gzip", "deflate")
_DECODE_STEP = 256 * 1024


def _inflate(data: bytes, wbits: int, limit: int, multi_member: bool) -> bytes:
    out = bytearray()
    remaining = data
    while remaining:
        decoder = zlib.decompressobj(wbits)
        pending = remaining
        while pending:
            budget = limit - len(out) + 1
            chunk = decoder.decompress(pending, min(_DECODE_STEP, budget))
            out.extend(chunk)
            if len(out) > limit:
                raise BodyTooLarge(limit)
            pending = decoder.unconsumed_tail
            if decoder.eof:
                break
        if not decoder.eof:
            tail = decoder.flush()
            out.extend(tail)
            if len(out) > limit:
                raise BodyTooLarge(limit)
            if not decoder.eof:
                raise DecodingError("truncated compressed stream")
        remaining = decoder.unused_data
        if not multi_member:
            break
        remaining = remaining.lstrip(b"\x00")
    return bytes(out)


def gunzip(data: bytes, limit: int) -> bytes:
    """Decompress gzip data, including concatenated multi-member streams."""
    try:
        return _inflate(data, 16 + zlib.MAX_WBITS, limit, multi_member=True)
    except zlib.error as exc:
        raise DecodingError(f"invalid gzip data: {exc}") from exc


def inflate(data: bytes, limit: int) -> bytes:
    """Decompress ``deflate`` data.

    RFC 9110 means zlib-wrapped deflate, but many servers send a raw deflate
    stream; both are accepted, told apart by the zlib header.
    """
    # A zlib header has CM=8 in the low nibble and a big-endian check value divisible by 31.
    wrapped = len(data) >= 2 and (data[0] & 0x0F) == 8 and int.from_bytes(data[:2], "big") % 31 == 0
    try:
        return _inflate(data, zlib.MAX_WBITS if wrapped else -zlib.MAX_WBITS, limit, multi_member=False)
    except zlib.error as exc:
        raise DecodingError(f"invalid deflate data: {exc}") from exc


def decode_content(data: bytes, codings: list[str], limit: int) -> bytes:
    """Undo the ``Content-Encoding`` list (applied in order, so decoded in reverse)."""
    for coding in reversed(codings):
        coding = coding.strip().lower()
        if coding in ("", "identity"):
            continue
        if coding in ("gzip", "x-gzip"):
            data = gunzip(data, limit) if data else b""
        elif coding == "deflate":
            data = inflate(data, limit) if data else b""
        else:
            raise DecodingError(f"unsupported content coding {coding!r}")
    return data


def looks_gzipped(data: bytes) -> bool:
    """True when ``data`` starts with the gzip magic number."""
    return data[:2] == b"\x1f\x8b"
