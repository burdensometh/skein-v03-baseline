"""URL parsing, resolution and canonicalisation."""

from skein.urls.canonical import Canonicalizer, QueryPolicy, normalize_host, parse_ipv4
from skein.urls.parse import URL, URLError, parse_url, remove_dot_segments, resolve

__all__ = [
    "URL",
    "URLError",
    "Canonicalizer",
    "QueryPolicy",
    "normalize_host",
    "parse_ipv4",
    "parse_url",
    "remove_dot_segments",
    "resolve",
]
