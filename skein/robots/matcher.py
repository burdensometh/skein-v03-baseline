"""Path pattern matching for robots.txt rules (RFC 9309 section 2.2.2-2.2.3)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from skein.urls.percent import normalize_percent

# Characters that may appear unescaped in a robots path pattern. '*' and '$'
# are pattern operators; everything else outside this set is percent-encoded
# so that patterns and URL paths compare in the same normal form.
ROBOTS_PATH_SAFE = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=:@/?"
)


@dataclass(frozen=True)
class PathPattern:
    """A compiled robots.txt path pattern supporting ``*`` and a trailing ``$``."""

    raw: str
    normalized: str = field(init=False)
    _regex: re.Pattern[str] | None = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        normalized = normalize_percent(self.raw, ROBOTS_PATH_SAFE)
        if not normalized.startswith(("/", "*")):
            normalized = "/" + normalized
        object.__setattr__(self, "normalized", normalized)
        anchored = normalized.endswith("$")
        body = normalized[:-1] if anchored else normalized
        if "*" not in body and not anchored:
            object.__setattr__(self, "_regex", None)
            return
        pieces = [re.escape(part) for part in body.split("*")]
        regex = ".*".join(pieces) + (r"\Z" if anchored else "")
        object.__setattr__(self, "_regex", re.compile(regex, re.DOTALL))

    @property
    def specificity(self) -> int:
        """Rule length used to pick the most specific match (octets of the pattern)."""
        return len(self.normalized.encode("utf-8"))

    def matches(self, path: str) -> bool:
        """``path`` must already be normalised with ``normalize_percent(path, ROBOTS_PATH_SAFE)``."""
        if self._regex is None:
            return path.startswith(self.normalized)
        return self._regex.match(path) is not None


@dataclass(frozen=True)
class Rule:
    """One ``Allow`` or ``Disallow`` line."""

    allow: bool
    pattern: PathPattern
    line: int = 0

    def __str__(self) -> str:
        return f"{'Allow' if self.allow else 'Disallow'}: {self.pattern.raw}"


def best_rule(rules: list[Rule], path: str) -> Rule | None:
    """Return the deciding rule for ``path``: longest match wins, ``Allow`` wins ties."""
    winner: Rule | None = None
    for rule in rules:
        if not rule.pattern.matches(path):
            continue
        if winner is None:
            winner = rule
            continue
        if rule.pattern.specificity > winner.pattern.specificity or (
            rule.pattern.specificity == winner.pattern.specificity and rule.allow and not winner.allow
        ):
            winner = rule
    return winner
