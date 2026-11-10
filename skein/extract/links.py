"""Link discovery: resolving, canonicalising, scoping and filtering hrefs."""

from __future__ import annotations

from dataclasses import dataclass

from skein.extract.rules import LinkRule
from skein.html.dom import Document
from skein.urls.canonical import Canonicalizer
from skein.urls.parse import URLError, parse_url, resolve

_SKIPPED_SCHEMES = frozenset({"mailto", "javascript", "tel", "data", "ftp", "file", "about", "sms"})

# File types that never contain HTML worth parsing; links to them are not followed.
DEFAULT_DENY_EXTENSIONS = frozenset(
    """7z apk avi bmp bz2 css deb dmg doc docx eot exe flac flv gif gz ico iso jar jpeg jpg js
    m4a m4v mkv mov mp3 mp4 mpeg mpg msi odp ods odt ogg otf pdf png ppt pptx rar rpm svg
    tar tgz tif tiff ttf wav webm webp wma wmv woff woff2 xls xlsx xz zip""".split()
)


def url_extension(url: str) -> str:
    """Lower-cased extension of the last path segment (``""`` when there is none)."""
    try:
        path = parse_url(url).path
    except URLError:
        return ""
    segment = path.rsplit("/", 1)[-1]
    if "." not in segment:
        return ""
    return segment.rsplit(".", 1)[1].lower()


@dataclass(frozen=True)
class DiscoveredLink:
    """A link that passed scope and rule checks."""

    url: str
    depth: int
    priority: int
    kind: str
    anchor_text: str


class DomainScope:
    """Restricts a crawl to a set of registrable host names and their subdomains."""

    def __init__(self, domains: list[str] | tuple[str, ...]) -> None:
        self.domains = tuple(d.lower().strip(".") for d in domains if d.strip("."))

    def allows(self, url: str) -> bool:
        if not self.domains:
            return True
        try:
            host = (parse_url(url).host or "").lower()
        except URLError:
            return False
        return any(host == d or host.endswith("." + d) for d in self.domains)


class LinkExtractor:
    """Finds followable links in a document according to :class:`LinkRule` objects.

    Links are resolved against ``<base href>`` (or the page URL), anything
    that is not http(s) is dropped, URLs are canonicalised, out-of-scope
    hosts are dropped and the first matching rule decides depth limit,
    priority and kind. ``rel="nofollow"`` links and pages carrying a
    ``nofollow`` robots meta tag are skipped when ``obey_nofollow`` is set.
    Links whose path ends in one of ``deny_extensions`` are dropped unless a
    rule explicitly targets sitemaps.
    """

    def __init__(
        self,
        rules: tuple[LinkRule, ...] | list[LinkRule],
        canonicalizer: Canonicalizer,
        scope: DomainScope,
        *,
        obey_nofollow: bool = True,
        deny_extensions: frozenset[str] = DEFAULT_DENY_EXTENSIONS,
    ) -> None:
        self.rules = tuple(rules)
        self.canonicalizer = canonicalizer
        self.scope = scope
        self.obey_nofollow = obey_nofollow
        self.deny_extensions = frozenset(e.lower().lstrip(".") for e in deny_extensions)

    def base_for(self, document: Document, page_url: str) -> str:
        href = document.base_href
        if not href:
            return page_url
        try:
            return str(resolve(page_url, href))
        except URLError:
            return page_url

    def extract(
        self, document: Document, page_url: str, depth: int, directives: set[str] | None = None
    ) -> list[DiscoveredLink]:
        """Followable links of ``document``.

        ``directives`` are the page's robots directives; when omitted they are
        read from the document's robots meta tags.
        """
        page_directives = document.meta_robots() if directives is None else directives
        if self.obey_nofollow and "nofollow" in page_directives:
            return []
        base = self.base_for(document, page_url)
        found: dict[str, DiscoveredLink] = {}
        for rule in self.rules:
            if rule.source_pattern is not None and not rule.source_pattern.search(page_url):
                continue
            child_depth = depth + 1
            if rule.max_depth is not None and child_depth > rule.max_depth:
                continue
            for element in document.select(rule.selector):
                if self.obey_nofollow and "nofollow" in (element.get("rel") or "").lower().split():
                    continue
                href = (element.get(rule.attr) or "").strip()
                url = self.normalize(base, href)
                if url is None or url in found or not self.scope.allows(url):
                    continue
                if not rule.accepts(url):
                    continue
                if rule.kind != "sitemap" and url_extension(url) in self.deny_extensions:
                    continue
                found[url] = DiscoveredLink(url, child_depth, rule.priority, rule.kind, element.text()[:200])
        return list(found.values())

    def normalize(self, base: str, href: str) -> str | None:
        """Resolve and canonicalise ``href``; ``None`` for unusable links."""
        if not href or href.startswith("#"):
            return None
        try:
            ref = parse_url(href)
            if ref.scheme in _SKIPPED_SCHEMES:
                return None
            absolute = resolve(base, ref)
            if absolute.scheme not in ("http", "https") or not absolute.host:
                return None
            return self.canonicalizer.key(absolute)
        except URLError:
            return None
