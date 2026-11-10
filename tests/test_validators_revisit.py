"""HTTP dates, cache validators, freshness lifetimes, revisit estimation and content fingerprints."""

from __future__ import annotations

import math
import re

import pytest

from skein.html import parse_html
from skein.incremental import Fingerprinter, RevisitPolicy, Validators, conditional_headers, normalize_etag
from skein.incremental.conditional import validators_from
from skein.incremental.freshness import freshness_lifetime, parse_cache_control
from skein.net.headers import Headers
from skein.net.httpdate import format_http_date, parse_http_date

EPOCH = 784111777.0  # Sun, 06 Nov 1994 08:49:37 GMT, the RFC example


@pytest.mark.parametrize("text", [
    "Sun, 06 Nov 1994 08:49:37 GMT",
    "Sunday, 06-Nov-94 08:49:37 GMT",
    "Sun Nov  6 08:49:37 1994",
    "  Sun, 06 Nov 1994 08:49:37 GMT  ",
    "Sun, 6 Nov 1994 08:49:37 UTC",
])
def test_all_three_http_date_formats(text: str) -> None:
    assert parse_http_date(text, reference=EPOCH) == EPOCH


@pytest.mark.parametrize("text", [
    "", "yesterday", "Sun, 06 Nov 1994 08:49:37", "Sun, 31 Feb 1994 08:49:37 GMT", "Sun, 06 Nov 1994 25:00:00 GMT",
    "Sun, 06 Foo 1994 08:49:37 GMT", "1994-11-06T08:49:37Z",
])
def test_invalid_http_dates(text: str) -> None:
    assert parse_http_date(text) is None


def test_two_digit_years_and_formatting() -> None:
    reference = parse_http_date("Mon, 01 Jan 2024 00:00:00 GMT")
    assert reference is not None
    assert parse_http_date("Friday, 01-Jan-71 00:00:00 GMT", reference=reference) == parse_http_date(
        "Fri, 01 Jan 2071 00:00:00 GMT"), "within 50 years ahead"
    assert parse_http_date("Friday, 01-Jan-99 00:00:00 GMT", reference=reference) == parse_http_date(
        "Fri, 01 Jan 1999 00:00:00 GMT")
    assert format_http_date(EPOCH) == "Sun, 06 Nov 1994 08:49:37 GMT"
    assert parse_http_date(format_http_date(1_700_000_000.7)) == 1_700_000_000.0


def test_etag_normalisation() -> None:
    assert normalize_etag('"abc"') == '"abc"'
    assert normalize_etag('W/"abc"') == 'W/"abc"'
    assert normalize_etag("abc123") == '"abc123"', "unquoted tags are quoted"
    assert normalize_etag('"broken') is None
    assert normalize_etag("a b") is None and normalize_etag("  ") is None and normalize_etag(None) is None


def test_validators_from_response_headers() -> None:
    now = EPOCH + 3600
    headers = Headers([("ETag", 'W/"v1"'), ("Last-Modified", "Sunday, 06-Nov-94 08:49:37 GMT"),
                       ("Date", format_http_date(now))])
    validators = validators_from(headers, now)
    assert validators == Validators('W/"v1"', "Sun, 06 Nov 1994 08:49:37 GMT"), "dates are normalised"
    future = Headers([("Last-Modified", format_http_date(now + 60)), ("Date", format_http_date(now))])
    assert validators_from(future, now).last_modified is None, "Last-Modified after Date is unusable"
    assert validators_from(Headers([("Last-Modified", "garbage")]), now).empty
    assert conditional_headers(validators) == {"If-None-Match": 'W/"v1"',
                                               "If-Modified-Since": "Sun, 06 Nov 1994 08:49:37 GMT"}
    assert conditional_headers(Validators()) == {}


def test_freshness_lifetimes() -> None:
    now = 1_700_000_000.0
    date = format_http_date(now)
    assert freshness_lifetime(Headers([("Cache-Control", "public, max-age=600")]), now) == 600
    assert freshness_lifetime(Headers([("Cache-Control", "max-age=600"), ("Age", "100")]), now) == 500
    assert freshness_lifetime(Headers([("Cache-Control", "max-age=600, no-cache")]), now) == 0
    assert freshness_lifetime(Headers([("Cache-Control", "no-store")]), now) == 0
    expires = Headers([("Date", date), ("Expires", format_http_date(now + 7200))])
    assert freshness_lifetime(expires, now) == 7200
    assert freshness_lifetime(Headers([("Expires", "0")]), now) == 0, "invalid Expires means already stale"
    both = Headers([("Cache-Control", "max-age=60"), ("Expires", format_http_date(now + 7200))])
    assert freshness_lifetime(both, now) == 60, "max-age wins over Expires"
    assert freshness_lifetime(Headers([("Last-Modified", format_http_date(now - 86400))]), now) is None
    assert freshness_lifetime(Headers([("Cache-Control", "max-age=abc")]), now) is None
    assert parse_cache_control(['max-age="30", private', "max-age=99"]) == {"max-age": "30", "private": None}


def test_revisit_rate_estimate() -> None:
    policy = RevisitPolicy()
    assert policy.estimate_rate(0, 0, 0) is None
    assert policy.estimate_rate(4, 0, 4 * 86400) == pytest.approx(-math.log(4.5 / 4.5) / 86400)
    every = policy.estimate_rate(4, 4, 4 * 86400)
    assert every == pytest.approx(-math.log(0.5 / 4.5) / 86400) and math.isfinite(every)
    half = policy.estimate_rate(4, 2, 4 * 86400)
    assert 0 < half < every


def test_revisit_interval_bounds_and_growth() -> None:
    policy = RevisitPolicy(initial_interval=100, min_interval=10, max_interval=1000, max_growth=2, max_shrink=4)
    assert policy.next_interval(checks=0, changes=0, observed_span=0, previous=None) == 100
    assert policy.next_interval(checks=1, changes=0, observed_span=100, previous=100) == 200
    assert policy.next_interval(checks=9, changes=0, observed_span=900, previous=800) == 1000
    assert policy.next_interval(checks=4, changes=4, observed_span=40, previous=100) == 25, "shrinks at most 4x"
    assert policy.next_interval(checks=50, changes=50, observed_span=500, previous=11) == 10
    halfway = policy.next_interval(checks=10, changes=5, observed_span=1000, previous=100)
    assert 25 <= halfway <= 200
    assert policy.next_interval(checks=1, changes=1, observed_span=100, previous=100, freshness=500) == 500
    assert policy.next_interval(checks=1, changes=1, observed_span=100, previous=100, freshness=1e9) == 1000


def test_changefreq_hints_and_failures() -> None:
    policy = RevisitPolicy(initial_interval=86400, min_interval=3600, max_interval=30 * 86400)
    assert policy.first_interval("hourly") == 3600 and policy.first_interval("weekly") == 7 * 86400
    assert policy.first_interval("always") == 3600 and policy.first_interval("never") == 30 * 86400
    assert policy.first_interval("yearly") == 30 * 86400 and policy.first_interval(None) == 86400
    assert policy.failure_interval(True, None) == 3600
    assert policy.failure_interval(False, None) == 30 * 86400
    assert RevisitPolicy(retry_interval=600, min_interval=60, initial_interval=600).failure_interval(True, 5) == 600
    with pytest.raises(ValueError):
        RevisitPolicy(min_interval=10, initial_interval=5)
    with pytest.raises(ValueError):
        RevisitPolicy(target_probability=1.0)


def test_derive_keeps_policies_consistent() -> None:
    base = RevisitPolicy(initial_interval=86400, min_interval=3600, max_interval=30 * 86400)
    assert base.derive(initial_interval=600).min_interval == 600
    assert base.derive(initial_interval=60 * 86400).max_interval == 60 * 86400
    narrowed = base.derive(max_interval=7200)
    assert (narrowed.min_interval, narrowed.initial_interval, narrowed.max_interval) == (3600, 7200, 7200)


def test_fingerprints_follow_visible_content() -> None:
    printer = Fingerprinter()
    a = parse_html("<html><head><title>T</title><script>var x = 1;</script></head>"
                   "<body><p>Hello   world</p><a href='/x'>x</a><!-- c1 --></body></html>")
    b = parse_html("<html><head><title>T</title><script>var x = 2;</script></head>"
                   "<body>\n<p>Hello world</p>\n<a href='/x'>x</a><!-- c2 --></body></html>")
    c = parse_html("<html><head><title>T</title></head><body><p>Hello world</p><a href='/y'>x</a></body></html>")
    assert printer.of_document(a) == printer.of_document(b), "scripts, comments and whitespace do not count"
    assert printer.of_document(a) != printer.of_document(c), "link targets do"
    noisy = [parse_html(f"<p>Price 5</p><footer>Generated at {t}</footer>") for t in ("10:00", "11:00")]
    assert Fingerprinter().of_document(noisy[0]) != Fingerprinter().of_document(noisy[1])
    quiet = Fingerprinter((re.compile(r"Generated at \S+"),))
    assert quiet.of_document(noisy[0]) == quiet.of_document(noisy[1])
    assert printer.of_bytes(b"x").startswith("v1:") and printer.of_bytes(b"x") != printer.of_bytes(b"y")
