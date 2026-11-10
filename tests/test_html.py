"""Tolerant HTML tree building and the CSS selector engine."""

from __future__ import annotations

import pytest

from skein.html import SelectorError, matches, parse_html, parse_selector
from skein.html.selector import parse_nth
from tests.fixtures_site.content import BROKEN_HTML, catalog_page

PAGE = """<!DOCTYPE html>
<html lang="en"><head><title> Demo &amp; Co </title><base href="/shop/"></head>
<body class="home page">
  <div id="main" class="content">
    <ul class="menu">
      <li class="item first"><a href="/a" data-id="1">A</a></li>
      <li class="item"><a href="/b" rel="nofollow" data-id="2">B</a></li>
      <li class="item special"><a href="https://ext.example/c" data-id="3">C</a></li>
      <li class="item last"><a href="/d" data-id="4" lang="en-US">D</a></li>
    </ul>
    <p>Intro <b>bold</b> text<!-- hidden --> <script>var x = "<p>";</script></p>
    <h2>One</h2><p class="lead">after one</p><p>second after one</p>
    <section><div></div><div> </div><span>x</span></section>
  </div>
</body></html>"""


@pytest.fixture(scope="module")
def doc():  # noqa: ANN201
    return parse_html(PAGE)


def texts(elements) -> list[str]:  # noqa: ANN001
    return [e.text() for e in elements]


def test_document_basics(doc) -> None:  # noqa: ANN001
    assert doc.title == "Demo & Co"
    assert doc.base_href == "/shop/"
    body = doc.select_one("body")
    assert body is not None and body.classes == ["home", "page"]


def test_text_excludes_comments_and_scripts(doc) -> None:  # noqa: ANN001
    paragraph = doc.select_one("#main > p")
    assert paragraph is not None
    assert paragraph.text() == "Intro bold text"
    assert paragraph.own_text() == "Intro text"


def test_block_elements_separate_words(doc) -> None:  # noqa: ANN001
    menu = doc.select_one("ul.menu")
    assert menu is not None and menu.text() == "A B C D"


@pytest.mark.parametrize(
    "selector,expected",
    [
        ("li", ["A", "B", "C", "D"]),
        ("li.item.special", ["C"]),
        ("#main .menu a", ["A", "B", "C", "D"]),
        ("ul > li > a[rel]", ["B"]),
        ("a[data-id='3']", ["C"]),
        ('a[data-id="3"]', ["C"]),
        ("a[href^=https]", ["C"]),
        ("a[href$='/d']", ["D"]),
        ("a[href*=ext]", ["C"]),
        ("li[class~=last] a", ["D"]),
        ("a[lang|=en]", ["D"]),
        ("a[DATA-ID=\"2\" i]", ["B"]),
        ("li:nth-child(2)", ["B"]),
        ("li:nth-child(odd)", ["A", "C"]),
        ("li:nth-child(even)", ["B", "D"]),
        ("li:nth-child(2n+3)", ["C"]),
        ("li:nth-child(-n+2)", ["A", "B"]),
        ("li:nth-last-child(1)", ["D"]),
        ("li:first-child, li:last-child", ["A", "D"]),
        ("li:not(.item)", []),
        ("li:not(.first):not(.last)", ["B", "C"]),
        ("h2 + p", ["after one"]),
        ("h2 ~ p", ["after one", "second after one"]),
        ("p:nth-of-type(2)", ["after one"]),
        ("section > :empty", [""]),
        ("section span:only-child", []),
        ("*.lead", ["after one"]),
    ],
)
def test_selectors(doc, selector: str, expected: list[str]) -> None:  # noqa: ANN001
    assert texts(doc.select(selector)) == expected


def test_results_in_document_order_without_duplicates(doc) -> None:  # noqa: ANN001
    found = doc.select("a, li.item a, #main a")
    assert texts(found) == ["A", "B", "C", "D"]


def test_scoped_select_and_matches(doc) -> None:  # noqa: ANN001
    menu = doc.select_one("ul")
    assert menu is not None
    assert texts(menu.select("#main a.missing, li:nth-child(1) a")) == ["A"]
    link = doc.select_one("a[data-id='2']")
    assert link is not None and matches(link, "div#main li > a")


@pytest.mark.parametrize(
    "bad", ["", "   ", "div >", "a[href", "a[href=]", ":hover", "li:nth-child(x)", "a,,b", "a!", ":not(:not(a))", "[x='open]"]
)
def test_selector_errors(bad: str) -> None:
    with pytest.raises(SelectorError):
        parse_selector(bad)


def test_nth_parsing() -> None:
    assert parse_nth("odd") == (2, 1)
    assert parse_nth(" 3n - 2 ") == (3, -2)
    assert parse_nth("-n+6") == (-1, 6)
    assert parse_nth("+5") == (0, 5)
    assert parse_nth("n") == (1, 0)


def test_broken_html_recovers_structure() -> None:
    doc = parse_html(BROKEN_HTML)
    product = doc.select_one("div.product")
    assert product is not None and product.get("data-sku") == "SKU-9001"
    assert texts(doc.select("ul.tags > li")) == ["alpha", "beta", "gamma"]
    assert texts(doc.select("table tr")) == ["cell one cell two", "row two"]
    assert texts(doc.select("tr:nth-child(1) > td")) == ["cell one", "cell two"]
    assert len(doc.select("p")) == 3
    assert doc.select_one("input").get("checked") == ""  # type: ignore[union-attr]
    assert doc.select("div.fake") == []
    assert doc.select_one("a[href='/catalog?page=2']") is not None
    assert [e.tag for e in doc.select("html > *")] == ["head", "body"]


def test_misnested_inline_tags() -> None:
    doc = parse_html("<p><a href=x>one <b>two</a> three</b></p>")
    link = doc.select_one("a")
    assert link is not None and link.text() == "one two"
    assert doc.select_one("p").text() == "one two three"  # type: ignore[union-attr]


def test_implied_end_tags() -> None:
    doc = parse_html("<dl><dt>a<dd>1<dt>b<dd>2</dl><select><option>x<option>y</select><p>one<div>block</div>")
    assert texts(doc.select("dl > dd")) == ["1", "2"]
    assert texts(doc.select("select > option")) == ["x", "y"]
    assert doc.select("p div") == []


def test_duplicate_body_attributes_are_merged() -> None:
    doc = parse_html("<body class=a><p>x</p><body id=b class=c><p>y</p>")
    assert len(doc.select("body")) == 1
    body = doc.select_one("body#b")
    assert body is not None and body.get("class") == "a"


def test_self_closing_and_void_elements() -> None:
    doc = parse_html("<div><br/><span/>after<img src=a.png></img></div>")
    div = doc.select_one("div")
    assert div is not None
    assert [c.tag for c in div.element_children] == ["br", "span", "img"]
    assert div.text() == "after"


def test_deep_nesting_is_bounded() -> None:
    doc = parse_html("<div>" * 5000 + "deep" + "</div>" * 5000)
    assert "deep" in doc.text()
    assert len(doc.outer_html()) > 0


def test_serialisation_round_trip() -> None:
    doc = parse_html('<p class="x">a &lt; b <br> <script>1 < 2</script></p>')
    paragraph = doc.select_one("p")
    assert paragraph is not None
    assert paragraph.outer_html() == '<p class="x">a &lt; b <br> <script>1 < 2</script></p>'


def test_catalog_fixture_selectors() -> None:
    doc = parse_html(catalog_page(2, 3, 4))
    cards = doc.select("#listing > div.product")
    assert [c.get("data-sku") for c in cards] == ["SKU-0005", "SKU-0006", "SKU-0007", "SKU-0008"]
    assert texts(doc.select(".pagination a[rel]")) == ["Previous", "Next"]


def test_meta_robots_tokens() -> None:
    doc = parse_html('<meta name="ROBOTS" content="NoIndex, nofollow"><meta name="skein" content="noarchive">')
    assert doc.meta_robots() == {"noindex", "nofollow", "noarchive"}


def test_block_elements_separate_text_on_both_sides() -> None:
    doc = parse_html("<div><p>first</p>second <a href='/x'>third</a><li>fourth</li>fifth</div>")
    assert doc.text() == "first second third fourth fifth", "inline elements still join their neighbours"
    assert doc.select_one("div").text(normalize=False) == "firstsecond thirdfourthfifth"  # type: ignore[union-attr]
