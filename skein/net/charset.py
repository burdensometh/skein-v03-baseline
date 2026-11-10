"""Character-set detection for fetched documents.

Precedence follows the HTML encoding sniffing algorithm in simplified form:
byte order mark, then the ``Content-Type`` charset parameter, then a
``<meta>`` prescan (or the XML declaration), then UTF-8 if the bytes are
valid UTF-8, and finally windows-1252.
"""

from __future__ import annotations

import codecs
import re
from dataclasses import dataclass

from skein.net.headers import parse_header_params

PRESCAN_BYTES = 4096

_BOMS = (
    (codecs.BOM_UTF8, "utf-8"),
    (codecs.BOM_UTF32_LE, "utf-32-le"),
    (codecs.BOM_UTF32_BE, "utf-32-be"),
    (codecs.BOM_UTF16_LE, "utf-16-le"),
    (codecs.BOM_UTF16_BE, "utf-16-be"),
)

# WHATWG Encoding Standard label folding for the labels seen in practice.
_LABEL_ALIASES = {
    "latin1": "cp1252",
    "latin-1": "cp1252",
    "iso-8859-1": "cp1252",
    "iso8859-1": "cp1252",
    "iso_8859-1": "cp1252",
    "l1": "cp1252",
    "us-ascii": "cp1252",
    "ascii": "cp1252",
    "windows-1252": "cp1252",
    "x-cp1252": "cp1252",
    "gb2312": "gb18030",
    "gbk": "gb18030",
    "x-gbk": "gb18030",
    "gb_2312-80": "gb18030",
    "chinese": "gb18030",
    "csgb2312": "gb18030",
    "big5-hkscs": "big5hkscs",
    "x-sjis": "shift_jis",
    "sjis": "shift_jis",
    "ms_kanji": "shift_jis",
    "windows-31j": "cp932",
    "euc-kr": "cp949",
    "ks_c_5601-1987": "cp949",
    "iso-8859-9": "cp1254",
    "tis-620": "cp874",
    "unicode-1-1-utf-8": "utf-8",
    "utf8": "utf-8",
    "x-unicode20utf8": "utf-8",
    "unicode": "utf-16-le",
    "utf-16": "utf-16-le",
}

_META_RE = re.compile(rb"<meta\b([^>]*)>", re.IGNORECASE)
_ATTR_RE = re.compile(rb"""([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*(?:=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+)))?""")
_COMMENT_RE = re.compile(rb"<!--.*?(?:-->|$)", re.DOTALL)
_XML_DECL_RE = re.compile(rb"""^<\?xml[^>]*\bencoding\s*=\s*["']([A-Za-z0-9._:\-]+)["']""")
_CHARSET_IN_CONTENT_RE = re.compile(rb"""charset\s*=\s*["']?([A-Za-z0-9._:\-]+)""", re.IGNORECASE)


@dataclass(frozen=True)
class Detected:
    """Result of charset detection: Python codec name, where it came from, BOM length."""

    encoding: str
    source: str
    bom_length: int = 0


def normalize_charset(label: str | None) -> str | None:
    """Map a charset label to a Python codec name, or ``None`` if unknown."""
    if not label:
        return None
    cleaned = label.strip().strip("\"'").lower()
    cleaned = _LABEL_ALIASES.get(cleaned, cleaned)
    try:
        return codecs.lookup(cleaned).name
    except LookupError:
        return None


def sniff_bom(data: bytes) -> tuple[str, int] | None:
    for bom, name in _BOMS:
        if data.startswith(bom):
            if name == "utf-16-le" and data.startswith(codecs.BOM_UTF32_LE):
                continue
            return name, len(bom)
    return None


def charset_from_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    _, params = parse_header_params(content_type)
    return normalize_charset(params.get("charset"))


def prescan_meta(data: bytes) -> str | None:
    """Find a charset declared by ``<meta charset>`` or ``<meta http-equiv>``."""
    window = _COMMENT_RE.sub(b"", data[:PRESCAN_BYTES])
    for match in _META_RE.finditer(window):
        attrs: dict[bytes, bytes] = {}
        for attr in _ATTR_RE.finditer(match.group(1)):
            name = attr.group(1).lower()
            value = attr.group(2) or attr.group(3) or attr.group(4) or b""
            attrs.setdefault(name, value)
        label: bytes | None = None
        if b"charset" in attrs:
            label = attrs[b"charset"]
        elif attrs.get(b"http-equiv", b"").lower() == b"content-type" and b"content" in attrs:
            found = _CHARSET_IN_CONTENT_RE.search(attrs[b"content"])
            label = found.group(1) if found else None
        if label:
            name = normalize_charset(label.decode("ascii", "ignore"))
            if name:
                # A document cannot declare a UTF-16 encoding from inside itself.
                return "utf-8" if name.startswith("utf-16") else name
    return None


def xml_declared_encoding(data: bytes) -> str | None:
    match = _XML_DECL_RE.match(data[:200])
    return normalize_charset(match.group(1).decode("ascii")) if match else None


def _is_utf8(data: bytes) -> bool:
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def detect_charset(data: bytes, content_type: str | None = None) -> Detected:
    """Decide which encoding to use for ``data``."""
    bom = sniff_bom(data)
    if bom:
        return Detected(bom[0], "bom", bom[1])
    header = charset_from_content_type(content_type)
    if header:
        return Detected(header, "header")
    media = parse_header_params(content_type)[0] if content_type else ""
    if "xml" in media or data.lstrip()[:5] == b"<?xml":
        declared = xml_declared_encoding(data.lstrip())
        if declared:
            return Detected(declared, "xml")
    meta = prescan_meta(data)
    if meta:
        return Detected(meta, "meta")
    if _is_utf8(data):
        return Detected("utf-8", "default")
    return Detected("cp1252", "fallback")


def decode_body(data: bytes, content_type: str | None = None) -> tuple[str, Detected]:
    """Decode ``data`` to text with the detected encoding; undecodable bytes are replaced."""
    detected = detect_charset(data, content_type)
    text = data[detected.bom_length :].decode(detected.encoding, "replace")
    return text, detected
