"""robots.txt parsing (RFC 9309) with the crawl-delay and sitemap extensions."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from skein.robots.matcher import ROBOTS_PATH_SAFE, PathPattern, Rule, best_rule
from skein.urls.parse import URL, URLError, parse_url
from skein.urls.percent import normalize_percent

MAX_ROBOTS_BYTES = 500 * 1024

_KEY_ALIASES = {
    "user-agent": "user-agent",
    "useragent": "user-agent",
    "user agent": "user-agent",
    "allow": "allow",
    "disallow": "disallow",
    "dissallow": "disallow",
    "disalow": "disallow",
    "crawl-delay": "crawl-delay",
    "crawldelay": "crawl-delay",
    "sitemap": "sitemap",
    "site-map": "sitemap",
}
_TOKEN_RE = re.compile(r"[A-Za-z_\-]+")


def agent_token(user_agent: str) -> str:
    """Extract the product token used for group matching (``skein/0.1 (...)`` -> ``skein``)."""
    match = _TOKEN_RE.match(user_agent.strip())
    return match.group(0).lower() if match else ""


@dataclass
class Group:
    """A group of rules that applies to one or more user agents."""

    agents: list[str] = field(default_factory=list)
    rules: list[Rule] = field(default_factory=list)
    crawl_delay: float | None = None


@dataclass(frozen=True)
class Decision:
    """Outcome of an access check, with the rule that decided it (if any)."""

    allowed: bool
    rule: Rule | None = None
    reason: str = ""


@dataclass
class RobotsTxt:
    """A parsed robots.txt file."""

    groups: list[Group] = field(default_factory=list)
    sitemaps: list[str] = field(default_factory=list)
    default_allow: bool = True
    warnings: list[str] = field(default_factory=list)

    @classmethod
    def allow_all(cls, reason: str = "") -> "RobotsTxt":
        return cls(default_allow=True, warnings=[reason] if reason else [])

    @classmethod
    def disallow_all(cls, reason: str = "") -> "RobotsTxt":
        return cls(default_allow=False, warnings=[reason] if reason else [])

    def group_for(self, user_agent: str) -> Group:
        """Merge all groups addressing ``user_agent``; fall back to ``*`` groups."""
        token = agent_token(user_agent)
        specific = [g for g in self.groups if token and token in g.agents]
        chosen = specific or [g for g in self.groups if "*" in g.agents]
        merged = Group(agents=[token] if specific else ["*"])
        for group in chosen:
            merged.rules.extend(group.rules)
            if group.crawl_delay is not None:
                merged.crawl_delay = max(merged.crawl_delay or 0.0, group.crawl_delay)
        return merged

    def decide(self, url: str | URL, user_agent: str) -> Decision:
        """Decide whether ``user_agent`` may fetch ``url`` (absolute URL or path)."""
        if not self.default_allow:
            return Decision(False, reason="robots.txt unreachable; everything disallowed")
        target = _request_path(url)
        if target == "/robots.txt":
            return Decision(True, reason="robots.txt is always allowed")
        group = self.group_for(user_agent)
        rule = best_rule(group.rules, normalize_percent(target, ROBOTS_PATH_SAFE))
        if rule is None:
            return Decision(True, reason="no matching rule")
        return Decision(rule.allow, rule, reason=str(rule))


def _request_path(url: str | URL) -> str:
    if isinstance(url, URL):
        return url.request_target
    if url.startswith("/"):
        return url
    try:
        return parse_url(url).request_target
    except URLError:
        return "/"


def parse_robots(data: str | bytes) -> RobotsTxt:
    """Parse robots.txt content.

    Unknown keys are ignored, rules before the first ``User-agent`` line are
    dropped with a warning, and only the first 500 KiB are considered.
    """
    if isinstance(data, bytes):
        data = data[:MAX_ROBOTS_BYTES]
        if data.startswith(b"\xef\xbb\xbf"):
            data = data[3:]
        text = data.decode("utf-8", "replace")
    else:
        text = data[:MAX_ROBOTS_BYTES]
    robots = RobotsTxt()
    current: Group | None = None
    collecting_agents = False
    for number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if ":" not in line:
            robots.warnings.append(f"line {number}: missing ':'")
            continue
        raw_key, _, value = line.partition(":")
        key = _KEY_ALIASES.get(raw_key.strip().lower())
        value = value.strip()
        if key is None:
            continue
        if key == "sitemap":
            if value:
                robots.sitemaps.append(value)
            continue
        if key == "user-agent":
            if current is None or not collecting_agents:
                current = Group()
                robots.groups.append(current)
                collecting_agents = True
            agent = "*" if value.startswith("*") else agent_token(value)
            if agent:
                current.agents.append(agent)
            continue
        collecting_agents = False
        if current is None:
            robots.warnings.append(f"line {number}: {raw_key.strip()} outside of any group")
            continue
        if key in ("allow", "disallow"):
            if not value:
                continue
            current.rules.append(Rule(key == "allow", PathPattern(value), number))
        elif key == "crawl-delay":
            try:
                delay = float(value)
            except ValueError:
                robots.warnings.append(f"line {number}: invalid crawl-delay {value!r}")
                continue
            if delay >= 0:
                current.crawl_delay = delay
    return robots
