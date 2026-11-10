"""robots.txt parsing, matching and the per-origin cache."""

from __future__ import annotations

import asyncio

import pytest

from skein.clock import FakeClock
from skein.net import HttpClient
from skein.robots import RobotsCache, RobotsTxt, agent_token, merge_directives, parse_robots, parse_x_robots_tag
from skein.robots.matcher import PathPattern
from tests.fixtures_site import FixtureSite

UA = "skein/0.1 (+https://skein.invalid/bot)"

ROBOTS = """
# comment line
User-agent: SKEIN
User-agent: friendly-bot
Disallow: /tmp/
Allow: /tmp/public$
Crawl-delay: 3

User-agent: *
Disallow: /
Allow: /public/
Crawl-delay: 10

Sitemap: https://example.com/sitemap.xml
sitemap: https://example.com/news.xml
"""


def allowed(robots: RobotsTxt, url: str, agent: str = UA) -> bool:
    return robots.decide(url, agent).allowed


def test_agent_token() -> None:
    assert agent_token(UA) == "skein"
    assert agent_token("Mozilla/5.0 (compatible)") == "mozilla"
    assert agent_token("  Googlebot-News ") == "googlebot-news"


def test_specific_group_beats_wildcard() -> None:
    robots = parse_robots(ROBOTS)
    assert allowed(robots, "/anything", UA)
    assert not allowed(robots, "/tmp/x", UA)
    assert allowed(robots, "/tmp/public", UA)
    assert not allowed(robots, "/tmp/public/more", UA)
    assert robots.group_for(UA).crawl_delay == 3
    assert robots.sitemaps == ["https://example.com/sitemap.xml", "https://example.com/news.xml"]


def test_wildcard_group_for_unknown_agents() -> None:
    robots = parse_robots(ROBOTS)
    assert not allowed(robots, "/", "otherbot/2.0")
    assert allowed(robots, "/public/page", "otherbot/2.0")
    assert robots.group_for("otherbot").crawl_delay == 10


def test_groups_for_same_agent_are_merged() -> None:
    robots = parse_robots("User-agent: a\nDisallow: /x\n\nUser-agent: b\nDisallow: /y\n\nUser-agent: a\nDisallow: /z\n")
    assert not allowed(robots, "/x", "a")
    assert not allowed(robots, "/z", "a")
    assert allowed(robots, "/y", "a")


def test_longest_match_and_allow_wins_ties() -> None:
    robots = parse_robots("User-agent: *\nDisallow: /page\nAllow: /page\nDisallow: /a/b\nAllow: /a\n")
    assert allowed(robots, "/page", UA)
    assert not allowed(robots, "/a/b/c", UA)
    assert allowed(robots, "/a/c", UA)


@pytest.mark.parametrize(
    "pattern,path,expected",
    [
        ("/fish", "/fish.html", True),
        ("/fish", "/Fish.asp", False),
        ("/fish*", "/fishheads/yummy.html", True),
        ("/fish/", "/fish", False),
        ("/*.php", "/folder/filename.php?parameters", True),
        ("/*.php$", "/filename.php?parameters", False),
        ("/*.php$", "/filename.php", True),
        ("/fish*.php", "/fishheads/catfish.php?parameters", True),
        ("/fish*.php", "/Fish.PHP", False),
        ("*/private", "/a/b/private", True),
        ("/a%3cd", "/a%3Cd", True),
        ("/ä", "/%C3%A4", True),
    ],
)
def test_pattern_matching(pattern: str, path: str, expected: bool) -> None:
    from skein.robots.matcher import ROBOTS_PATH_SAFE
    from skein.urls.percent import normalize_percent

    assert PathPattern(pattern).matches(normalize_percent(path, ROBOTS_PATH_SAFE)) is expected


def test_robots_txt_itself_always_allowed() -> None:
    robots = parse_robots("User-agent: *\nDisallow: /\n")
    assert allowed(robots, "http://example.com/robots.txt", UA)
    assert not allowed(robots, "http://example.com/index.html", UA)


def test_empty_disallow_and_junk_lines() -> None:
    robots = parse_robots(b"\xef\xbb\xbfUser-agent: *\nDisallow:\nthis line is junk\nCrawl-delay: soon\nFoo: bar\n")
    assert allowed(robots, "/", UA)
    assert robots.group_for(UA).crawl_delay is None
    assert any("missing ':'" in w for w in robots.warnings)
    assert any("crawl-delay" in w for w in robots.warnings)


def test_rules_outside_group_are_ignored() -> None:
    robots = parse_robots("Disallow: /\nUser-agent: *\nDisallow: /private\n")
    assert allowed(robots, "/", UA)
    assert robots.warnings


def test_query_string_participates_in_matching() -> None:
    robots = parse_robots("User-agent: *\nDisallow: /*?sessionid=\n")
    assert not allowed(robots, "http://x/page?sessionid=1", UA)
    assert allowed(robots, "http://x/page?id=1", UA)


def test_decision_reports_rule() -> None:
    decision = parse_robots("User-agent: *\nDisallow: /private/\n").decide("/private/x", UA)
    assert not decision.allowed
    assert decision.rule is not None and decision.rule.line == 2
    assert "Disallow: /private/" in decision.reason


def test_disallow_all_and_allow_all() -> None:
    assert not allowed(RobotsTxt.disallow_all(), "/robots.txt", UA)
    assert allowed(RobotsTxt.allow_all(), "/anything", UA)


# -- cache semantics against the fixture site ------------------------------------


def _cache_run(site: FixtureSite, clock: FakeClock, body):  # noqa: ANN001, ANN202
    async def go():  # noqa: ANN202
        async with HttpClient(clock=clock) as client:
            cache = RobotsCache(client, clock, UA, ttl=100, error_ttl=10)
            return await body(cache)

    return asyncio.run(go())


def test_cache_fetches_once_and_applies_rules(site: FixtureSite, clock: FakeClock) -> None:
    async def body(cache: RobotsCache):  # noqa: ANN202
        urls = [site.url(p) for p in ("/", "/private/x", "/private/open")]
        entries = await asyncio.gather(*(cache.get(u) for u in urls))
        return [allowed(e.robots, u) for e, u in zip(entries, urls)], cache.fetches

    decisions, fetches = _cache_run(site, clock, body)
    assert decisions == [True, False, True]
    assert fetches == 1
    assert site.paths().count("/robots.txt") == 1


def test_cache_ttl_expiry(site: FixtureSite, clock: FakeClock) -> None:
    async def body(cache: RobotsCache):  # noqa: ANN202
        await cache.get(site.url("/"))
        clock.advance(50)
        await cache.get(site.url("/"))
        clock.advance(51)
        assert cache.cached(site.url("/")) is None
        await cache.get(site.url("/"))
        return cache.fetches

    assert _cache_run(site, clock, body) == 2


@pytest.mark.parametrize("status", [404, 403, 410])
def test_4xx_means_allow_all(make_site, clock: FakeClock, status: int) -> None:  # noqa: ANN001
    site = make_site(robots_status=status)

    async def body(cache: RobotsCache):  # noqa: ANN202
        entry = await cache.get(site.url("/"))
        return entry.state, allowed(entry.robots, "/private/x", UA)

    assert _cache_run(site, clock, body) == ("unavailable", True)


@pytest.mark.parametrize("status", [500, 503, 429])
def test_5xx_and_429_mean_disallow_all(make_site, clock: FakeClock, status: int) -> None:  # noqa: ANN001
    site = make_site(robots_status=status)

    async def body(cache: RobotsCache):  # noqa: ANN202
        entry = await cache.get(site.url("/"))
        assert entry.expires_at - entry.fetched_at == 10
        return entry.state, allowed(entry.robots, "/", UA)

    assert _cache_run(site, clock, body) == ("unreachable", False)


def test_unreachable_host_disallows(clock: FakeClock) -> None:
    async def go():  # noqa: ANN202
        async with HttpClient(clock=clock) as client:
            cache = RobotsCache(client, clock, UA)
            return await cache.get("http://127.0.0.1:1/page")

    entry = asyncio.run(go())
    assert entry.state == "unreachable"
    assert "ConnectError" in (entry.error or "")
    assert not allowed(entry.robots, "/page", UA)


def test_stale_copy_survives_refresh_failure(make_site, clock: FakeClock) -> None:  # noqa: ANN001
    site = make_site()

    async def body(cache: RobotsCache):  # noqa: ANN202
        first = await cache.get(site.url("/"))
        assert first.state == "fetched"
        site.options.robots_status = 503
        clock.advance(101)
        second = await cache.get(site.url("/"))
        return second.state, allowed(second.robots, "/private/x", UA), allowed(second.robots, "/", UA)

    assert _cache_run(site, clock, body) == ("stale", False, True)


def test_crawl_delay_and_sitemaps_from_fixture(make_site, clock: FakeClock) -> None:  # noqa: ANN001
    site = make_site(crawl_delay=2.5)

    async def body(cache: RobotsCache):  # noqa: ANN202
        entry = await cache.get(site.url("/"))
        return entry.robots.group_for(UA).crawl_delay, entry.robots.sitemaps, allowed(entry.robots, "/doc/a.pdf", UA)

    delay, sitemaps, pdf_allowed = _cache_run(site, clock, body)
    assert delay == 2.5
    assert sitemaps == [site.url("/sitemap_index.xml.gz")]
    assert pdf_allowed is False


def test_x_robots_tag_parsing() -> None:
    assert parse_x_robots_tag(["noindex, NoFollow"], UA) == {"noindex", "nofollow"}
    assert parse_x_robots_tag(["otherbot: noindex", "skein: nofollow"], UA) == {"nofollow"}
    assert parse_x_robots_tag(["none"], UA) == {"none", "noindex", "nofollow"}
    assert parse_x_robots_tag(["unavailable_after: 25 Jun 2010 15:00:00 PST"], UA) == set()
    assert merge_directives({"none"}, set()) >= {"noindex", "nofollow"}
    assert merge_directives({"noarchive", ""}, {"noindex"}) == {"noarchive", "noindex"}
