"""Percent-encoding normalisation (RFC 3986 sections 2.1-2.4 and 6.2.2)."""

from __future__ import annotations

import string

UNRESERVED = frozenset(string.ascii_letters + string.digits + "-._~")
SUB_DELIMS = frozenset("!$&'()*+,;=")
_HEX = frozenset(string.hexdigits)

PATH_SAFE = UNRESERVED | SUB_DELIMS | frozenset(":@/")
QUERY_SAFE = UNRESERVED | SUB_DELIMS | frozenset(":@/?")
FRAGMENT_SAFE = QUERY_SAFE
USERINFO_SAFE = UNRESERVED | SUB_DELIMS | frozenset(":")
# Inside a single query key or value '&', '=' and '+' keep their delimiter meaning.
QUERY_COMPONENT_SAFE = QUERY_SAFE - frozenset("&=+")


def normalize_percent(text: str, safe: frozenset[str]) -> str:
    """Normalise percent-encoding in one URL component.

    * escapes of unreserved characters are decoded (``%7E`` -> ``~``);
    * remaining escapes get upper-case hex digits (``%3a`` -> ``%3A``);
    * characters outside ``safe`` (spaces, non-ASCII) are UTF-8 encoded;
    * a stray ``%`` not followed by two hex digits becomes ``%25``.
    """
    out: list[str] = []
    i = 0
    length = len(text)
    while i < length:
        ch = text[i]
        if ch == "%":
            pair = text[i + 1 : i + 3]
            if len(pair) == 2 and pair[0] in _HEX and pair[1] in _HEX:
                decoded = chr(int(pair, 16))
                out.append(decoded if decoded in UNRESERVED else "%" + pair.upper())
                i += 3
                continue
            out.append("%25")
        elif ch in safe:
            out.append(ch)
        else:
            out.extend(f"%{byte:02X}" for byte in ch.encode("utf-8", "surrogatepass"))
        i += 1
    return "".join(out)


def percent_decode(text: str, encoding: str = "utf-8", plus_as_space: bool = False) -> str:
    """Decode all percent escapes; invalid byte sequences are replaced."""
    if plus_as_space:
        text = text.replace("+", " ")
    if "%" not in text:
        return text
    buf = bytearray()
    i = 0
    while i < len(text):
        ch = text[i]
        pair = text[i + 1 : i + 3]
        if ch == "%" and len(pair) == 2 and pair[0] in _HEX and pair[1] in _HEX:
            buf.append(int(pair, 16))
            i += 3
        else:
            buf.extend(ch.encode("utf-8"))
            i += 1
    return buf.decode(encoding, "replace")
