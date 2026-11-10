"""Content fingerprints that decide whether a refetched document really changed.

Byte-for-byte comparison would report a change whenever a page embeds a
timestamp, a session token or re-indented markup. For HTML the
fingerprint therefore covers what a reader (and the extractor) sees: the
title, the visible text with whitespace collapsed (scripts, styles and
comments excluded) and the link targets. Configured ``ignore`` patterns
are removed from that text first, which takes care of "Generated at ..."
footers and similar noise. Other content types are hashed as bytes.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from skein.html.dom import Document, collapse_whitespace

FINGERPRINT_SCHEME = "v1"


@dataclass(frozen=True)
class Fingerprinter:
    """Computes ``v1:<sha256>`` fingerprints; ``ignore`` holds compiled noise patterns."""

    ignore: tuple[re.Pattern[str], ...] = ()

    def _strip_noise(self, text: str) -> str:
        for pattern in self.ignore:
            text = pattern.sub("", text)
        return collapse_whitespace(text)

    def visible_text(self, document: Document) -> str:
        """The page text a reader sees, noise patterns removed (also what SimHashes are computed from)."""
        return self._strip_noise(document.text())

    def of_document(self, document: Document, text: str | None = None) -> str:
        """Fingerprint of ``document``; ``text`` may pass in an already computed :meth:`visible_text`."""
        digest = hashlib.sha256()
        digest.update(self._strip_noise(document.title or "").encode("utf-8"))
        digest.update(b"\x00")
        digest.update((self.visible_text(document) if text is None else text).encode("utf-8"))
        digest.update(b"\x00")
        hrefs = sorted({element.get("href") or "" for element in document.select("a[href], area[href]")})
        digest.update("\n".join(hrefs).encode("utf-8"))
        return f"{FINGERPRINT_SCHEME}:{digest.hexdigest()}"

    def of_bytes(self, body: bytes) -> str:
        return f"{FINGERPRINT_SCHEME}:{hashlib.sha256(body).hexdigest()}"
