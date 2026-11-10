"""XML sitemap parsing: urlset, sitemapindex, gzip and plain-text variants."""

from skein.sitemaps.dates import parse_w3c_datetime
from skein.sitemaps.parser import Sitemap, SitemapEntry, SitemapError, parse_sitemap

__all__ = ["Sitemap", "SitemapEntry", "SitemapError", "parse_sitemap", "parse_w3c_datetime"]
