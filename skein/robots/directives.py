"""Page-level robots directives from ``<meta name="robots">`` and ``X-Robots-Tag``."""

from __future__ import annotations

from typing import Iterable

from skein.robots.parser import agent_token

KNOWN_DIRECTIVES = frozenset(
    {"all", "none", "noindex", "nofollow", "noarchive", "nosnippet", "noimageindex", "notranslate", "indexifembedded"}
)


def parse_x_robots_tag(values: Iterable[str], user_agent: str) -> set[str]:
    """Directives from ``X-Robots-Tag`` header values that apply to ``user_agent``.

    A value may be prefixed by a user-agent token (``otherbot: noindex``), in
    which case it only applies to that crawler. ``none`` expands to
    ``noindex`` and ``nofollow``; ``unavailable_after`` and unknown tokens are
    ignored.
    """
    token = agent_token(user_agent)
    directives: set[str] = set()
    for value in values:
        scope, sep, rest = value.partition(":")
        if sep and scope.strip().lower() not in KNOWN_DIRECTIVES and " " not in scope.strip():
            if scope.strip().lower() != token:
                continue
            value = rest
        for raw in value.split(","):
            directive = raw.strip().lower()
            if directive in KNOWN_DIRECTIVES:
                directives.add(directive)
    if "none" in directives:
        directives.update({"noindex", "nofollow"})
    return directives


def merge_directives(meta: set[str], header: set[str]) -> set[str]:
    """Union of meta-tag and header directives with ``none`` expanded."""
    merged = {d for d in meta | header if d}
    if "none" in merged:
        merged.update({"noindex", "nofollow"})
    return merged
