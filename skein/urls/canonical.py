"""URL canonicalisation: turning many spellings of a URL into one key.

The canonical form is used for de-duplication in the frontier and as the
stable identity of pages in the store. Rules version 1 applies, in order:

1. scheme and host lower-cased; host IDNA-encoded, trailing dot removed;
2. default port dropped, userinfo dropped unless ``keep_userinfo``;
3. percent-encoding normalised per component;
4. dot segments removed, empty path becomes ``/``;
5. query rewritten according to a :class:`QueryPolicy`;
6. fragment removed unless ``keep_fragment``.

Rules version 2 additionally removes session ids from path parameters,
drops tracking parameters (see :mod:`skein.urls.tracking`) and, unless
the query order is ``preserve``, sorts query parameters by name (the
relative order of repeated names is kept). Every version-2 step is
idempotent and commutes with the version-1 steps, so applying version 2
to a version-1 key gives the same result as applying it to the original
URL; :mod:`skein.store.rekey` relies on that when it upgrades stored keys.
"""

from __future__ import annotations

import encodings.idna
import fnmatch
import ipaddress
from dataclasses import dataclass, field

import hashlib
import json

from skein.urls.parse import DEFAULT_PORTS, URL, URLError, parse_url, remove_dot_segments
from skein.urls.percent import (
    FRAGMENT_SAFE,
    PATH_SAFE,
    QUERY_COMPONENT_SAFE,
    USERINFO_SAFE,
    normalize_percent,
    percent_decode,
)
from skein.urls.tracking import TrackingFilter, strip_session_params

RULE_VERSIONS = (1, 2)
QUERY_ORDERS = ("sort", "preserve")


def _parse_ipv4_part(part: str) -> int | None:
    if not part:
        return None
    try:
        if part[:2] in ("0x", "0X"):
            return int(part[2:] or "0", 16)
        if len(part) > 1 and part[0] == "0":
            return int(part[1:], 8)
        return int(part, 10)
    except ValueError:
        return None


def parse_ipv4(host: str) -> str | None:
    """Interpret WHATWG-style IPv4 spellings (``0x7f.1``, ``2130706433``, ``0177.0.0.1``).

    Returns dotted-quad form, or ``None`` when ``host`` is a domain name.
    Raises :class:`URLError` for numeric hosts that are out of range.
    """
    parts = host.split(".")
    if parts and parts[-1] == "":
        parts.pop()
    if not parts or len(parts) > 4:
        return None
    numbers = [_parse_ipv4_part(p) for p in parts]
    if any(n is None for n in numbers):
        return None
    values = [n for n in numbers if n is not None]
    if any(n > 255 for n in values[:-1]) or values[-1] >= 256 ** (5 - len(values)):
        raise URLError(f"IPv4 address out of range: {host!r}")
    address = values[-1]
    for index, value in enumerate(values[:-1]):
        address += value * 256 ** (3 - index)
    return str(ipaddress.IPv4Address(address))


def normalize_host(host: str) -> str:
    """Lower-case, percent-decode and IDNA-encode a host name.

    IPv6 literals are compressed, IPv4 addresses in any numeric spelling are
    converted to dotted-quad form. Raises :class:`URLError` for labels that
    cannot be encoded.
    """
    host = percent_decode(host).strip().rstrip(".").lower()
    if not host:
        raise URLError("empty host")
    if ":" in host:
        try:
            return str(ipaddress.IPv6Address(host.strip("[]")))
        except ValueError as exc:
            raise URLError(f"invalid IPv6 address {host!r}") from exc
    ipv4 = parse_ipv4(host)
    if ipv4 is not None:
        return ipv4
    labels: list[str] = []
    for label in host.replace("。", ".").replace("．", ".").split("."):
        if not label:
            raise URLError(f"empty label in host {host!r}")
        if label.isascii():
            labels.append(label)
            continue
        try:
            labels.append(encodings.idna.ToASCII(label).decode("ascii"))
        except UnicodeError as exc:
            raise URLError(f"cannot IDNA-encode host label {label!r}: {exc}") from exc
    return ".".join(labels)


@dataclass(frozen=True)
class QueryPolicy:
    """How query strings are rewritten during canonicalisation.

    ``remove`` and ``keep_only`` hold shell-style patterns matched against the
    decoded, lower-cased parameter name (``utm_*``). ``keep_only`` wins over
    ``remove`` when both are set.
    """

    sort: bool = False
    remove: tuple[str, ...] = ()
    keep_only: tuple[str, ...] | None = None
    drop_empty_values: bool = False
    tracking: TrackingFilter | None = None

    def keeps(self, name: str) -> bool:
        lowered = percent_decode(name, plus_as_space=True).lower()
        if self.keep_only is not None:
            return any(fnmatch.fnmatchcase(lowered, pat.lower()) for pat in self.keep_only)
        return not any(fnmatch.fnmatchcase(lowered, pat.lower()) for pat in self.remove)

    def apply(self, query: str | None) -> str | None:
        """Rewrite a raw query string; returns ``None`` when nothing is left."""
        if query is None:
            return None
        pairs: list[tuple[str, str | None]] = []
        for chunk in query.split("&"):
            if not chunk:
                continue
            name, sep, value = chunk.partition("=")
            name = normalize_percent(name, QUERY_COMPONENT_SAFE)
            norm_value = normalize_percent(value, QUERY_COMPONENT_SAFE) if sep else None
            if not self.keeps(name):
                continue
            if self.tracking is not None and self.keep_only is None and self.tracking.is_tracking(name):
                continue
            if self.drop_empty_values and not norm_value:
                continue
            pairs.append((name, norm_value))
        if self.sort:
            pairs.sort(key=lambda pair: pair[0])
        if not pairs:
            return None
        return "&".join(name if value is None else f"{name}={value}" for name, value in pairs)


@dataclass(frozen=True)
class Canonicalizer:
    """Applies the canonicalisation rules described in the module docstring."""

    query: QueryPolicy = field(default_factory=QueryPolicy)
    keep_fragment: bool = False
    keep_userinfo: bool = False
    rules: int = 1

    def __post_init__(self) -> None:
        if self.rules not in RULE_VERSIONS:
            raise ValueError(f"unknown canonicalisation rules version {self.rules!r}")

    @classmethod
    def strict(
        cls,
        *,
        remove: tuple[str, ...] = (),
        keep_only: tuple[str, ...] | None = None,
        drop_empty_values: bool = False,
        tracking: tuple[str, ...] = (),
        query_order: str = "sort",
        keep_fragment: bool = False,
    ) -> "Canonicalizer":
        """A rules-version-2 canonicaliser; with no arguments, the built-in rules used by schema migrations."""
        if query_order not in QUERY_ORDERS:
            raise ValueError(f"query_order must be one of {QUERY_ORDERS}")
        policy = QueryPolicy(sort=query_order == "sort", remove=remove, keep_only=keep_only,
                             drop_empty_values=drop_empty_values, tracking=TrackingFilter(tracking))
        return cls(policy, keep_fragment=keep_fragment, rules=2)

    def signature(self) -> str:
        """A short stable digest of every setting that influences keys.

        Stored with a spider's keys, it tells whether the keys were produced
        by the canonicaliser now configured.
        """
        policy = self.query
        settings = {
            "rules": self.rules,
            "sort": policy.sort,
            "remove": sorted(policy.remove),
            "keep_only": sorted(policy.keep_only) if policy.keep_only is not None else None,
            "drop_empty": policy.drop_empty_values,
            "tracking": sorted(policy.tracking.extra) if policy.tracking is not None else None,
            "fragment": self.keep_fragment,
            "userinfo": self.keep_userinfo,
        }
        encoded = json.dumps(settings, sort_keys=True).encode("utf-8")
        return f"r{self.rules}:{hashlib.sha256(encoded).hexdigest()[:16]}"

    def key(self, url: URL | str) -> str:
        """The canonical string form of ``url``, used as its de-duplication key."""
        parsed = parse_url(url) if isinstance(url, str) else url
        if not parsed.is_absolute:
            raise URLError(f"cannot canonicalise relative URL {str(parsed)!r}")
        scheme = parsed.scheme
        host = parsed.host
        port = parsed.port
        if host is not None:
            host = normalize_host(host)
        if port is not None and DEFAULT_PORTS.get(scheme or "") == port:
            port = None
        userinfo = None
        if self.keep_userinfo and parsed.userinfo:
            userinfo = normalize_percent(parsed.userinfo, USERINFO_SAFE)
        path = remove_dot_segments(normalize_percent(parsed.path, PATH_SAFE))
        if self.rules >= 2:
            path = strip_session_params(path)
        if host is not None and path == "":
            path = "/"
        query = self.query.apply(parsed.query)
        fragment = None
        if self.keep_fragment and parsed.fragment:
            fragment = normalize_percent(parsed.fragment, FRAGMENT_SAFE)
        return str(URL(scheme, userinfo, host, port, path, query, fragment))
