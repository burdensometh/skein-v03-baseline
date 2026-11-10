"""Page bodies served by the fixture site."""

from __future__ import annotations

import gzip
import random
from html import escape

PRODUCT_TAGS = ["red", "green", "blue", "large", "small", "sale"]


def product_name(pid: int) -> str:
    return f"Widget {pid}"


def product_price(pid: int, revision: int = 0) -> str:
    cents = 999 + pid * 1250 + revision * 100
    whole, frac = divmod(cents, 100)
    return f"${whole:,}.{frac:02d}"


def product_tags(pid: int) -> list[str]:
    return [PRODUCT_TAGS[(pid + i) % len(PRODUCT_TAGS)] for i in range(1 + pid % 3)]


def in_stock(pid: int) -> bool:
    return pid % 4 != 0


def layout(title: str, body: str, head_extra: str = "") -> str:
    return (
        "<!DOCTYPE html>\n<html><head>"
        f"<title>{escape(title)}</title>{head_extra}</head>\n"
        f"<body>\n{body}\n</body></html>\n"
    )


def home_page(extra_links: tuple[str, ...] = ()) -> str:
    links = [
        ("/catalog?page=1", "Catalog"),
        ("/about", "About us"),
        ("/private/secret", "Staff only"),
        ("/private/open", "Open staff notes"),
        ("/redirect/to-product", "Featured product"),
        ("mailto:sales@example.invalid", "Mail us"),
        ("javascript:void(0)", "Menu"),
        *((url, f"Partner {n}") for n, url in enumerate(extra_links)),
    ]
    items = "\n".join(f'<li><a href="{href}">{escape(text)}</a></li>' for href, text in links)
    return layout("Fixture shop", f"<h1>Fixture shop</h1>\n<ul id=\"nav\">\n{items}\n</ul>")


def catalog_page(page: int, pages: int, per_page: int, revisions: dict[int, int] | None = None) -> str:
    revisions = revisions or {}
    cards = []
    first = (page - 1) * per_page + 1
    for pid in range(first, first + per_page):
        tags = "".join(f"<li>{t}</li>" for t in product_tags(pid))
        stock = "In stock" if in_stock(pid) else "Sold out"
        # Every other card links with a tracking parameter and a fragment so the
        # crawler's canonicalisation has duplicates to collapse.
        href = f"/product/{pid}" if pid % 2 else f"/product/{pid}?utm_source=catalog#reviews"
        cards.append(
            f'<div class="product card" data-sku="SKU-{pid:04d}">\n'
            f'  <a class="title" href="{href}">{product_name(pid)}</a>\n'
            f'  <span class="price">{product_price(pid, revisions.get(pid, 0))}</span>\n'
            f'  <span class="stock">{stock}</span>\n'
            f'  <ul class="tags">{tags}</ul>\n'
            "</div>"
        )
    nav = ['<div class="pagination">']
    if page > 1:
        nav.append(f'<a rel="prev" href="?page={page - 1}">Previous</a>')
    if page < pages:
        nav.append(f'<a rel="next" href="?page={page + 1}">Next</a>')
    nav.append("</div>")
    body = (
        f'<h1>Catalog page {page}</h1>\n<div id="listing">\n' + "\n".join(cards) + "\n</div>\n"
        + "\n".join(nav)
        + '\n<a href="/">Home</a>'
    )
    return layout(f"Catalog {page}", body)


def product_page(pid: int, revision: int = 0, footer: str = "") -> str:
    tags = "".join(f'<li class="tag">{t}</li>' for t in product_tags(pid))
    stock = "In stock" if in_stock(pid) else "Sold out"
    body = (
        f'<div class="product-detail" id="p{pid}">\n'
        f'<h1 itemprop="name">  {product_name(pid)}  </h1>\n'
        f'<p>SKU: <span id="sku">SKU-{pid:04d}</span></p>\n'
        f'<p class="price">Price: <span>{product_price(pid, revision)}</span></p>\n'
        f'<p class="stock">{stock}</p>\n'
        f'<ul class="tags">{tags}</ul>\n'
        f'<img src="/img/{pid}.png" alt="photo">\n'
        f'<a class="related" href="/product/{pid % 12 + 1}">Related</a>\n'
        '<a href="/catalog?page=1">Back to catalog</a>\n'
        "</div>"
        + (f'\n<footer class="generated">{escape(footer)}</footer>' if footer else "")
    )
    return layout(product_name(pid), body)


BROKEN_HTML = """<html><head><title>Broken & proud</title>
<body>
<div class=product data-sku=SKU-9001>
  <a class=title href=/product/1>Mangled <b>widget</a></b>
  <span class="price">$5.00
  <p>first paragraph<p>second paragraph
  <ul class=tags><li>alpha<li>beta<li>gamma</ul>
</span>
</div></div></span>
<table><tr><td>cell one<td>cell two<tr><td>row two
</table>
<img src=/a.png><br/><input type=checkbox checked>
<script>if (a < b && c > d) { document.write("<div class='fake'>") }</script>
<p>unterminated <i>italic
<a href="/catalog?page=2">deep link
"""


def charset_page(title: str, text: str, meta: str = "") -> str:
    return layout(title, f'<p id="text">{text}</p>', head_extra=meta)


def robots_txt(base: str, crawl_delay: float | None) -> str:
    delay = f"Crawl-delay: {crawl_delay}\n" if crawl_delay is not None else ""
    return (
        "# fixture robots\n"
        "User-agent: otherbot\n"
        "Disallow: /\n"
        "\n"
        "User-agent: *\n"
        "Disallow: /private/\n"
        "Allow: /private/open\n"
        "Disallow: /*.pdf$\n"
        f"{delay}"
        "\n"
        f"Sitemap: {base}/sitemap_index.xml.gz\n"
    )


def urlset(base: str, paths: list[tuple[str, str | None]]) -> bytes:
    entries = []
    for path, lastmod in paths:
        mod = f"<lastmod>{lastmod}</lastmod>" if lastmod else ""
        entries.append(f"  <url><loc>{escape(base + path)}</loc>{mod}<changefreq>daily</changefreq></url>")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(entries)
        + "\n</urlset>\n"
    ).encode("utf-8")


def sitemap_index(base: str, children: list[tuple[str, str]]) -> bytes:
    entries = "\n".join(
        f"  <sitemap><loc>{base}{path}</loc><lastmod>{mod}</lastmod></sitemap>" for path, mod in children
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{entries}\n</sitemapindex>\n"
    ).encode("utf-8")


def gzip_bytes(data: bytes) -> bytes:
    return gzip.compress(data, mtime=0)


ARTICLE_WORDS = (
    "harbour council budget meeting river bridge repairs school library opening market traffic winter "
    "festival museum exhibition railway station timetable volunteers garden park cycling route weather "
    "storm warning hospital clinic hours election candidates debate housing plan tram extension"
).split()


def article_text(number: int) -> str:
    """A deterministic news story of about 1500 words; the same ``number`` always gives the same text."""
    chooser = random.Random(number)
    words = []
    for index in range(1500):
        words.append(chooser.choice(ARTICLE_WORDS))
        if index % 15 == 14:
            words[-1] += "."
    return " ".join(words)


def article_page(number: int, edition: str = "") -> str:
    """Article ``number``; ``edition`` adds a one-line variation, giving a near duplicate of the plain page."""
    note = f"<p class=\"edition\">{escape(edition)} edition</p>\n" if edition else ""
    body = (f"<article id=\"a{number}\">\n<h1>Story {number}</h1>\n{note}<p>{article_text(number)}</p>\n"
            "</article>\n<a href=\"/articles\">All stories</a>")
    return layout(f"Story {number}", body)


def articles_index(count: int, editions: tuple[str, ...]) -> str:
    links = []
    for number in range(1, count + 1):
        links.append(f'<li><a href="/article/{number}">Story {number}</a></li>')
        links.extend(f'<li><a href="/article/{number}?edition={escape(e)}">Story {number} ({escape(e)})</a></li>'
                     for e in editions)
    return layout("Stories", "<ul id=\"stories\">\n" + "\n".join(links) + "\n</ul>")


def tracked_page() -> str:
    """Links that differ only in tracking parameters, parameter order and path session ids."""
    links = [
        "/product/1?utm_source=newsletter&utm_medium=email",
        "/product/1?fbclid=IwAR0abc",
        "/product/2;jsessionid=0A1B2C3D",
        "/product/2?gclid=xyz&mc_eid=42",
        "/catalog?page=2&sort=price",
        "/catalog?sort=price&page=2",
        "/catalog?sort=price&page=2&utm_campaign=spring",
    ]
    items = "\n".join(f'<li><a href="{href}">variant {n}</a></li>' for n, href in enumerate(links))
    return layout("Tracked links", f"<ul id=\"tracked\">\n{items}\n</ul>")
