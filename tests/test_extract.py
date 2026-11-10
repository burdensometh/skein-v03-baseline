"""Field rules, coercion, item keys and link extraction."""

from __future__ import annotations

import re

import pytest

from skein.extract import (
    CoercionError,
    DomainScope,
    Extractor,
    FieldRule,
    ItemRule,
    LinkExtractor,
    LinkRule,
    coerce,
    parse_number,
)
from skein.extract.coerce import apply_regex, apply_transforms
from skein.extract.links import url_extension
from skein.html import parse_html
from skein.urls import Canonicalizer, QueryPolicy
from tests.fixtures_site.content import catalog_page, product_page

URL = "http://shop.test/product/7"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("$1,234.50", 1234.5),
        ("1.234,50 €", 1234.5),
        ("Price: 12,5", 12.5),
        ("1,234", 1234),
        ("-42 items", -42),
        ("3.14159", 3.14159),
        ("1 234 567", 1234567),
        ("EUR 0.99", 0.99),
    ],
)
def test_parse_number(text: str, expected: float) -> None:
    assert parse_number(text) == expected


def test_coerce_types() -> None:
    assert coerce("1,200", "int") == 1200
    assert coerce("7.0", "int") == 7
    assert coerce(" 2.5 ", "float") == 2.5
    assert coerce("In stock", "bool") is True
    assert coerce("Sold out", "bool") is False
    assert coerce("March 5, 2024", "date") == "2024-03-05"
    assert coerce("05/03/2024", "date", formats=("%d/%m/%Y",)) == "2024-03-05"
    assert coerce("2024-03-05T10:11:12+02:00", "datetime") == "2024-03-05T10:11:12+02:00"
    assert coerce("../img/a.png#x", "url", base_url="http://s.test/p/q") == "http://s.test/img/a.png"
    assert coerce("anything", "string") == "anything"


@pytest.mark.parametrize(
    "value,type_name", [("7.5", "int"), ("maybe", "bool"), ("31/31/2024", "date"), ("", "int"), ("abc", "number")]
)
def test_coerce_failures(value: str, type_name: str) -> None:
    with pytest.raises(CoercionError):
        coerce(value, type_name)


def test_transforms_and_regex() -> None:
    assert apply_transforms("  Hello   World ", ("strip", "collapse", "lower")) == "hello world"
    assert apply_regex("SKU: AB-12", re.compile(r"SKU:\s*(\S+)")) == "AB-12"
    assert apply_regex("id=9 rev=3", re.compile(r"rev=(?P<value>\d+)")) == "3"
    assert apply_regex("abc", re.compile(r"\d+")) is None
    assert apply_regex("abc 12", re.compile(r"\d+")) == "12"


PRODUCT_RULE = ItemRule(
    name="product",
    url_pattern=re.compile(r"/product/\d+$"),
    key_fields=("sku",),
    fields=(
        FieldRule("sku", "#sku", required=True),
        FieldRule("title", "h1"),
        FieldRule("price", ".price span", type="number"),
        FieldRule("tags", "li.tag", multiple=True),
        FieldRule("in_stock", ".stock", type="bool"),
        FieldRule("image", "img", source="attr", attr="src", type="url"),
        FieldRule("detail_id", ".product-detail", source="attr", attr="id", regex=re.compile(r"p(\d+)"), type="int"),
        FieldRule("rating", ".rating", type="float", default=0.0),
        FieldRule("html_title", "h1", source="html", transforms=("strip",)),
    ),
)


def test_product_extraction() -> None:
    result = Extractor([PRODUCT_RULE]).extract(parse_html(product_page(7)), URL)
    assert result.errors == []
    (item,) = result.items
    assert item.key == "SKU-0007"
    assert item.data == {
        "sku": "SKU-0007",
        "title": "Widget 7",
        "price": 97.49,
        "tags": ["green", "blue"],
        "in_stock": True,
        "image": "http://shop.test/img/7.png",
        "detail_id": 7,
        "rating": 0.0,
        "html_title": "Widget 7",
    }
    assert item.data_json().startswith('{"detail_id":7,')


def test_url_pattern_filters_pages() -> None:
    result = Extractor([PRODUCT_RULE]).extract(parse_html(product_page(7)), "http://shop.test/catalog")
    assert result.items == []


def test_scoped_items_from_listing() -> None:
    rule = ItemRule(
        name="card",
        scope="div.product",
        key_fields=("sku",),
        fields=(
            FieldRule("sku", None, source="attr", attr="data-sku"),
            FieldRule("name", "a.title"),
            FieldRule("link", "a.title", source="attr", attr="href", type="url"),
            FieldRule("price", ".price", type="number"),
        ),
    )
    result = Extractor([rule]).extract(parse_html(catalog_page(1, 3, 4)), "http://shop.test/catalog?page=1")
    assert [i.key for i in result.items] == ["SKU-0001", "SKU-0002", "SKU-0003", "SKU-0004"]
    assert result.items[1].data["link"] == "http://shop.test/product/2?utm_source=catalog"
    assert result.items[0].data["price"] == 22.49


def test_multiple_values_joined() -> None:
    rule = ItemRule("x", fields=(FieldRule("tags", "li.tag", multiple=True, join=", ", transforms=("upper",)),
                                 FieldRule("none", ".nope", multiple=True, join=",")))
    (item,) = Extractor([rule]).extract(parse_html(product_page(2)), URL).items
    assert item.data == {"tags": "BLUE, LARGE, SMALL", "none": None}


def test_required_field_missing_drops_item() -> None:
    rule = ItemRule("x", fields=(FieldRule("missing", ".nope", required=True), FieldRule("t", "h1")))
    result = Extractor([rule]).extract(parse_html(product_page(1)), URL)
    assert result.items == [] and result.dropped == 1
    assert "required field 'missing'" in result.errors[0]


def test_coercion_error_is_reported_but_item_kept() -> None:
    rule = ItemRule("x", fields=(FieldRule("stock", ".stock", type="int"), FieldRule("sku", "#sku")))
    result = Extractor([rule]).extract(parse_html(product_page(1)), URL)
    assert result.items[0].data == {"stock": None, "sku": "SKU-0001"}
    assert result.items[0].key == URL
    assert "field 'stock': no number in 'In stock'" in result.errors[0]


def test_missing_key_field_drops_item() -> None:
    rule = ItemRule("x", key_fields=("code",), fields=(FieldRule("code", ".nope"),))
    result = Extractor([rule]).extract(parse_html(product_page(1)), URL)
    assert result.dropped == 1 and "key fields missing" in result.errors[0]


def test_keys_without_key_fields_for_multiple_scopes() -> None:
    rule = ItemRule("li", scope="ul.tags li", fields=(FieldRule("tag"),))
    result = Extractor([rule]).extract(parse_html(product_page(2)), URL)
    assert [i.key for i in result.items] == [f"{URL}#0", f"{URL}#1", f"{URL}#2"]
    assert [i.data["tag"] for i in result.items] == ["blue", "large", "small"]


LINK_PAGE = """<html><head><base href="http://shop.test/catalog/"></head><body>
<a href="item?id=1&utm_source=x#frag">one</a>
<a href="/catalog/item?id=1">one again</a>
<a href="../about">about</a>
<a href="http://cdn.shop.test/file.pdf">pdf</a>
<a href="https://elsewhere.test/">offsite</a>
<a href="mailto:a@b">mail</a><a href="javascript:void(0)">js</a><a href="#top">top</a><a>no href</a>
<a href="/private/x" rel="nofollow">secret</a>
<area href="/map/area1">
<link rel="sitemap" href="/sitemap.xml">
</body></html>"""


def _links(rules, **kwargs):  # noqa: ANN001, ANN202
    canon = Canonicalizer(QueryPolicy(remove=("utm_*",)))
    extractor = LinkExtractor(rules, canon, DomainScope(["shop.test"]), **kwargs)
    return extractor.extract(parse_html(LINK_PAGE), "http://shop.test/catalog/page", depth=1)


def test_link_extraction_resolves_scopes_and_dedups() -> None:
    links = _links([LinkRule()])
    assert [l.url for l in links] == [
        "http://shop.test/catalog/item?id=1",
        "http://shop.test/about",
        "http://shop.test/map/area1",
    ]
    assert all(l.depth == 2 for l in links)
    assert links[0].anchor_text == "one"


def test_link_rules_allow_deny_priority_and_kind() -> None:
    rules = [
        LinkRule(allow=(re.compile(r"/catalog/"),), priority=5),
        LinkRule(selector="link[rel=sitemap]", kind="sitemap", priority=1),
        LinkRule(deny=(re.compile(r"\.pdf$"),)),
    ]
    links = {l.url: l for l in _links(rules)}
    assert links["http://shop.test/catalog/item?id=1"].priority == 5
    assert links["http://shop.test/sitemap.xml"].kind == "sitemap"
    assert "http://cdn.shop.test/file.pdf" not in links
    assert links["http://shop.test/about"].priority == 0


def test_link_rule_depth_limit_and_source_filter() -> None:
    assert _links([LinkRule(max_depth=1)]) == []
    assert _links([LinkRule(source_pattern=re.compile(r"/nowhere"))]) == []


def test_binary_extensions_are_skipped_unless_allowed() -> None:
    assert "http://cdn.shop.test/file.pdf" in [l.url for l in _links([LinkRule()], deny_extensions=frozenset())]
    assert [l.url for l in _links([LinkRule()], deny_extensions=frozenset({".PDF"}))].count("http://cdn.shop.test/file.pdf") == 0
    assert url_extension("http://x/a/b.tar.GZ?x=1") == "gz"
    assert url_extension("http://x/a.b/c") == ""


def test_nofollow_can_be_ignored() -> None:
    urls = [l.url for l in _links([LinkRule()], obey_nofollow=False)]
    assert "http://shop.test/private/x" in urls


def test_meta_nofollow_suppresses_all_links() -> None:
    doc = parse_html('<meta name="robots" content="nofollow"><a href="/x">x</a>')
    extractor = LinkExtractor([LinkRule()], Canonicalizer(), DomainScope([]))
    assert extractor.extract(doc, "http://a.test/", 0) == []


def test_domain_scope() -> None:
    scope = DomainScope(["example.com", ".shop.test."])
    assert scope.allows("http://example.com/")
    assert scope.allows("https://www.example.com/x")
    assert not scope.allows("http://notexample.com/")
    assert scope.allows("http://a.shop.test/")
    assert DomainScope([]).allows("http://anything.test/")
