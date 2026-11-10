"""URL parsing, RFC 3986 resolution and canonicalisation."""

from __future__ import annotations

import pytest

from skein.urls import Canonicalizer, QueryPolicy, URLError, normalize_host, parse_ipv4, parse_url, resolve
from skein.urls.parse import remove_dot_segments
from skein.urls.percent import PATH_SAFE, normalize_percent, percent_decode

BASE = "http://a/b/c/d;p?q"
canonicalize = Canonicalizer().key

# RFC 3986 section 5.4.1 (normal) and 5.4.2 (abnormal) examples.
RFC_EXAMPLES = [
    ("g:h", "g:h"),
    ("g", "http://a/b/c/g"),
    ("./g", "http://a/b/c/g"),
    ("g/", "http://a/b/c/g/"),
    ("/g", "http://a/g"),
    ("//g", "http://g"),
    ("?y", "http://a/b/c/d;p?y"),
    ("g?y", "http://a/b/c/g?y"),
    ("#s", "http://a/b/c/d;p?q#s"),
    ("g#s", "http://a/b/c/g#s"),
    ("g?y#s", "http://a/b/c/g?y#s"),
    (";x", "http://a/b/c/;x"),
    ("g;x", "http://a/b/c/g;x"),
    ("", "http://a/b/c/d;p?q"),
    (".", "http://a/b/c/"),
    ("./", "http://a/b/c/"),
    ("..", "http://a/b/"),
    ("../", "http://a/b/"),
    ("../g", "http://a/b/g"),
    ("../..", "http://a/"),
    ("../../g", "http://a/g"),
    ("../../../g", "http://a/g"),
    ("/./g", "http://a/g"),
    ("/../g", "http://a/g"),
    ("g.", "http://a/b/c/g."),
    (".g", "http://a/b/c/.g"),
    ("g..", "http://a/b/c/g.."),
    ("..g", "http://a/b/c/..g"),
    ("./../g", "http://a/b/g"),
    ("./g/.", "http://a/b/c/g/"),
    ("g/./h", "http://a/b/c/g/h"),
    ("g/../h", "http://a/b/c/h"),
    ("g;x=1/./y", "http://a/b/c/g;x=1/y"),
    ("g;x=1/../y", "http://a/b/c/y"),
    ("g?y/./x", "http://a/b/c/g?y/./x"),
    ("g#s/../x", "http://a/b/c/g#s/../x"),
]


@pytest.mark.parametrize("reference,expected", RFC_EXAMPLES)
def test_rfc3986_reference_resolution(reference: str, expected: str) -> None:
    assert str(resolve(BASE, reference)) == expected


def test_parse_components() -> None:
    url = parse_url("HTTPS://user:pw@Example.COM:8443/a/b?x=1#frag")
    assert url.scheme == "https"
    assert url.userinfo == "user:pw"
    assert url.host == "example.com"
    assert url.port == 8443
    assert url.path == "/a/b"
    assert url.query == "x=1"
    assert url.fragment == "frag"
    assert url.origin == "https://example.com:8443"
    assert url.request_target == "/a/b?x=1"
    assert str(url) == "https://user:pw@example.com:8443/a/b?x=1#frag"


def test_parse_distinguishes_empty_and_missing_query() -> None:
    assert parse_url("http://x/?").query == ""
    assert parse_url("http://x/").query is None
    assert str(parse_url("http://x/?")) == "http://x/?"


def test_parse_ipv6_and_host_header() -> None:
    url = parse_url("http://[2001:DB8::1]:8080/")
    assert url.host == "2001:db8::1"
    assert url.host_header == "[2001:db8::1]:8080"
    assert parse_url("https://x.org:443/").host_header == "x.org"


def test_parse_strips_whitespace_and_backslashes() -> None:
    url = parse_url("  http://example.com\\a\\b?q=\\x \n")
    assert url.path == "/a/b"
    assert url.query == "q=\\x"


@pytest.mark.parametrize("bad", ["http://x:99999/", "http://x:12ab/", "http://[::1/", "http://a b/"])
def test_parse_rejects_invalid(bad: str) -> None:
    with pytest.raises(URLError):
        parse_url(bad)


def test_remove_dot_segments() -> None:
    assert remove_dot_segments("/a/b/c/./../../g") == "/a/g"
    assert remove_dot_segments("mid/content=5/../6") == "mid/6"
    assert remove_dot_segments("/..") == "/"


def test_percent_normalisation() -> None:
    assert normalize_percent("/%7euser/%3a/a b/é", PATH_SAFE) == "/~user/%3A/a%20b/%C3%A9"
    assert normalize_percent("100%", PATH_SAFE) == "100%25"
    assert percent_decode("a%20b+c", plus_as_space=True) == "a b c"


def test_canonicalize_default_rules() -> None:
    assert canonicalize("HTTP://Www.Example.COM:80/a/./b/../c?b=2&a=1#top") == "http://www.example.com/a/c?b=2&a=1"
    assert canonicalize("https://example.com:443") == "https://example.com/"
    assert canonicalize("http://example.com:8080/%7Efoo") == "http://example.com:8080/~foo"
    assert canonicalize("http://user:pw@example.com/") == "http://example.com/"


def test_canonicalize_idna_hosts() -> None:
    assert canonicalize("http://Bücher.DE/straße") == "http://xn--bcher-kva.de/stra%C3%9Fe"
    assert normalize_host("例え.テスト") == "xn--r8jz45g.xn--zckzah"
    assert normalize_host("example.com.") == "example.com"


def test_canonicalize_numeric_ipv4_spellings() -> None:
    assert normalize_host("0x7f.1") == "127.0.0.1"
    assert normalize_host("2130706433") == "127.0.0.1"
    assert normalize_host("0177.0.0.1") == "127.0.0.1"
    with pytest.raises(URLError):
        normalize_host("256.256.256.256")
    assert parse_ipv4("1.2.3") == "1.2.0.3"
    assert parse_ipv4("example.com") is None
    assert parse_ipv4("1.2.3.4.5") is None


def test_query_policy_sort_remove_keep() -> None:
    sorter = Canonicalizer(QueryPolicy(sort=True, remove=("utm_*", "sessionid")))
    assert sorter.key("http://x/p?utm_source=a&z=1&SessionID=9&a=2&a=1") == "http://x/p?a=2&a=1&z=1"
    assert sorter.key("http://x/p?utm_medium=x") == "http://x/p"
    keeper = Canonicalizer(QueryPolicy(keep_only=("page", "q")))
    assert keeper.key("http://x/s?q=shoes&ref=nav&page=2") == "http://x/s?q=shoes&page=2"
    dropper = Canonicalizer(QueryPolicy(drop_empty_values=True))
    assert dropper.key("http://x/?a=&b=1&c") == "http://x/?b=1"


def test_query_values_are_percent_normalised_but_delimiters_kept() -> None:
    assert canonicalize("http://x/?q=a%2db%26c&r=%41") == "http://x/?q=a-b%26c&r=A"


def test_keep_fragment_option() -> None:
    assert Canonicalizer(keep_fragment=True).key("http://x/a#Sec 1") == "http://x/a#Sec%201"


def test_canonicalize_rejects_relative() -> None:
    with pytest.raises(URLError):
        canonicalize("/relative/path")
