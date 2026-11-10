"""Turning a fetched page or sitemap into items, links and change information (no I/O)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from skein.dedup.simhash import SimHasher
from skein.extract.extractor import ExtractedItem, Extractor
from skein.extract.links import DiscoveredLink, LinkExtractor
from skein.html.builder import parse_html
from skein.incremental.fingerprint import Fingerprinter
from skein.net.response import Response
from skein.robots.directives import merge_directives, parse_x_robots_tag
from skein.sitemaps.parser import Sitemap, SitemapError, parse_sitemap

log = logging.getLogger(__name__)


@dataclass
class PageAnalysis:
    """What a 2xx page response yielded."""

    fingerprint: str
    html: bool
    items: list[ExtractedItem] = field(default_factory=list)
    links: list[DiscoveredLink] = field(default_factory=list)
    dropped: int = 0
    extraction_errors: int = 0
    noindex: bool = False
    item_types: tuple[str, ...] = ()
    simhash: int | None = None


def analyse_page(
    response: Response,
    final_url: str,
    depth: int,
    *,
    extractor: Extractor,
    links: LinkExtractor,
    fingerprinter: Fingerprinter,
    user_agent: str,
    obey_noindex: bool,
    simhasher: SimHasher | None = None,
) -> PageAnalysis:
    """Extract items and links from an HTML page and fingerprint any response body.

    ``noindex`` (meta tag or ``X-Robots-Tag`` for our user agent)
    suppresses item extraction; ``nofollow`` is applied by the link
    extractor. ``item_types`` lists the item rules that ran over the page,
    i.e. the types whose complete set of items on this page is known.
    Partial fragment rules are left out: their items are reconciled across
    the whole run instead of against a single page.
    With a ``simhasher`` the page's visible text also gets a SimHash.
    """
    if not response.is_html:
        return PageAnalysis(fingerprinter.of_bytes(response.body), html=False)
    document = parse_html(response.text)
    directives = merge_directives(
        document.meta_robots(), parse_x_robots_tag(response.headers.get_all("X-Robots-Tag"), user_agent)
    )
    text = fingerprinter.visible_text(document)
    analysis = PageAnalysis(fingerprinter.of_document(document, text), html=True)
    if simhasher is not None:
        analysis.simhash = simhasher.of_text(text)
    analysis.noindex = obey_noindex and "noindex" in directives
    if extractor.rules and not analysis.noindex:
        analysis.item_types = tuple(
            rule.name for rule in extractor.rules
            if rule.applies_to(final_url) and not rule.partial
        )
        result = extractor.extract(document, final_url)
        analysis.items = result.items
        analysis.dropped = result.dropped
        analysis.extraction_errors = len(result.errors)
        for message in result.errors[:5]:
            log.debug("extraction: %s", message)
    analysis.links = links.extract(document, final_url, depth, directives)
    return analysis


@dataclass
class SitemapAnalysis:
    sitemap: Sitemap | None
    fingerprint: str
    error: str | None = None


def analyse_sitemap(response: Response, fingerprinter: Fingerprinter) -> SitemapAnalysis:
    fingerprint = fingerprinter.of_bytes(response.body)
    try:
        return SitemapAnalysis(parse_sitemap(response.body, base_url=response.url), fingerprint)
    except SitemapError as exc:
        return SitemapAnalysis(None, fingerprint, f"SitemapError: {exc}")
