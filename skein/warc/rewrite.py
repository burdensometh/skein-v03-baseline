"""Rewriting links in replayed HTML so that browsing stays inside the archive.

Every ``href``, ``src`` and ``action`` attribute value that resolves to an
``http(s)`` URL is replaced by ``/web/<timestamp>/<absolute URL>``: the
replay server then answers with the capture closest to the page's own
capture time. ``data:``, ``javascript:``, ``mailto:`` and fragment-only
references are left alone, as is everything outside those attributes.
"""

from __future__ import annotations

import html
import re

from skein.urls.parse import URLError, resolve

_ATTRIBUTE_RE = re.compile(r"""(?P<lead>\b(?:href|src|action)\s*=\s*)(?P<quote>["']?)(?P<value>[^"'\s>]*)(?P=quote)""",
                           re.IGNORECASE)
_SKIPPED = ("data:", "javascript:", "mailto:", "tel:", "#", "about:")


def archived_link(base_url: str, reference: str, timestamp: str, prefix: str = "/web") -> str | None:
    """The replay URL for ``reference`` found on ``base_url``; ``None`` if it should stay as it is."""
    target = html.unescape(reference).strip()
    if not target or target.lower().startswith(_SKIPPED):
        return None
    try:
        absolute = str(resolve(base_url, target))
    except URLError:
        return None
    if not absolute.startswith(("http://", "https://")):
        return None
    return f"{prefix}/{timestamp}/{absolute}"


def rewrite_html(text: str, base_url: str, timestamp: str, prefix: str = "/web") -> tuple[str, int]:
    """``text`` with archive-relative links, and how many were rewritten."""
    count = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal count
        replacement = archived_link(base_url, match.group("value"), timestamp, prefix)
        if replacement is None:
            return match.group(0)
        count += 1
        quote = match.group("quote") or '"'
        return f"{match.group('lead')}{quote}{html.escape(replacement, quote=True)}{quote}"

    return _ATTRIBUTE_RE.sub(replace, text), count
