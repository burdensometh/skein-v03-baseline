"""RFC 3986 URL parsing, recomposition and reference resolution."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443, "ftp": 21}
SPECIAL_SCHEMES = frozenset(DEFAULT_PORTS)

_URI_RE = re.compile(
    r"^(?:(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*):)?"
    r"(?://(?P<authority>[^/?#]*))?"
    r"(?P<path>[^?#]*)"
    r"(?:\?(?P<query>[^#]*))?"
    r"(?:#(?P<fragment>.*))?$",
    re.DOTALL,
)
_STRIP_CHARS = "\t\n\r"


class URLError(ValueError):
    """A string could not be parsed as a URL."""


@dataclass(frozen=True)
class URL:
    """A parsed URL. ``None`` means a component is absent, ``""`` means present but empty."""

    scheme: str | None
    userinfo: str | None
    host: str | None
    port: int | None
    path: str
    query: str | None
    fragment: str | None

    @property
    def is_absolute(self) -> bool:
        return self.scheme is not None

    @property
    def has_authority(self) -> bool:
        return self.host is not None

    @property
    def effective_port(self) -> int | None:
        """The explicit port, or the scheme's default port."""
        if self.port is not None:
            return self.port
        return DEFAULT_PORTS.get(self.scheme or "")

    @property
    def authority(self) -> str | None:
        if self.host is None:
            return None
        out = ""
        if self.userinfo is not None:
            out += self.userinfo + "@"
        out += _bracket(self.host)
        if self.port is not None:
            out += f":{self.port}"
        return out

    @property
    def host_header(self) -> str:
        """Value for the HTTP ``Host`` header (port omitted when it is the default)."""
        host = _bracket(self.host or "")
        default = DEFAULT_PORTS.get(self.scheme or "")
        if self.port is not None and self.port != default:
            return f"{host}:{self.port}"
        return host

    @property
    def origin(self) -> str:
        """``scheme://host:port`` with an explicit effective port; used as a cache key."""
        return f"{self.scheme}://{_bracket(self.host or '')}:{self.effective_port}"

    @property
    def request_target(self) -> str:
        """Origin-form request target: path plus query, never empty."""
        target = self.path or "/"
        if self.query is not None:
            target += "?" + self.query
        return target

    def __str__(self) -> str:
        out = ""
        if self.scheme is not None:
            out += self.scheme + ":"
        authority = self.authority
        if authority is not None:
            out += "//" + authority
        out += self.path
        if self.query is not None:
            out += "?" + self.query
        if self.fragment is not None:
            out += "#" + self.fragment
        return out


def _bracket(host: str) -> str:
    return f"[{host}]" if ":" in host and not host.startswith("[") else host


def parse_url(text: str) -> URL:
    """Parse ``text`` into a :class:`URL`.

    Leading/trailing whitespace and embedded tabs/newlines are removed (as
    browsers do). For special schemes backslashes are treated as slashes and
    the scheme is lower-cased. Hosts are kept verbatim apart from case; use
    :mod:`skein.urls.canonical` for full normalisation.
    """
    if not isinstance(text, str):
        raise URLError(f"URL must be a string, not {type(text).__name__}")
    cleaned = text.strip(" \x00\x0c" + _STRIP_CHARS)
    cleaned = "".join(ch for ch in cleaned if ch not in _STRIP_CHARS)
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.\-]*):", cleaned)
    if scheme_match and scheme_match.group(1).lower() in SPECIAL_SCHEMES:
        head, sep, tail = cleaned.partition("?")
        cleaned = head.replace("\\", "/") + sep + tail
    match = _URI_RE.match(cleaned)
    if match is None:  # pragma: no cover - the pattern accepts every string
        raise URLError(f"unparseable URL: {text!r}")
    scheme = match.group("scheme")
    scheme = scheme.lower() if scheme else None
    authority = match.group("authority")
    userinfo = host = None
    port: int | None = None
    if authority is not None:
        userinfo, host, port = _parse_authority(authority, text)
    return URL(scheme, userinfo, host, port, match.group("path"), match.group("query"),
               match.group("fragment"))


def _parse_authority(authority: str, original: str) -> tuple[str | None, str, int | None]:
    userinfo: str | None = None
    if "@" in authority:
        userinfo, _, authority = authority.rpartition("@")
    port: int | None = None
    if authority.startswith("["):
        end = authority.find("]")
        if end < 0:
            raise URLError(f"unterminated IPv6 literal in {original!r}")
        host = authority[1:end].lower()
        rest = authority[end + 1 :]
        if rest:
            if not rest.startswith(":"):
                raise URLError(f"junk after IPv6 literal in {original!r}")
            port = _parse_port(rest[1:], original)
    else:
        host, sep, port_text = authority.rpartition(":") if ":" in authority else (authority, "", "")
        if sep:
            port = _parse_port(port_text, original)
        host = host.lower()
    if any(ch in host for ch in " <>\"`{}|^"):
        raise URLError(f"forbidden character in host of {original!r}")
    return userinfo, host, port


def _parse_port(text: str, original: str) -> int | None:
    if text == "":
        return None
    if not text.isdigit():
        raise URLError(f"invalid port {text!r} in {original!r}")
    value = int(text)
    if value > 65535:
        raise URLError(f"port {value} out of range in {original!r}")
    return value


def remove_dot_segments(path: str) -> str:
    """Remove ``.`` and ``..`` segments as specified in RFC 3986 section 5.2.4."""
    if "." not in path:
        return path
    output: list[str] = []
    rest = path
    while rest:
        if rest.startswith("../"):
            rest = rest[3:]
        elif rest.startswith("./"):
            rest = rest[2:]
        elif rest.startswith("/./"):
            rest = rest[2:]
        elif rest == "/.":
            rest = "/"
        elif rest.startswith("/../"):
            rest = rest[3:]
            if output:
                output.pop()
        elif rest == "/..":
            rest = "/"
            if output:
                output.pop()
        elif rest in (".", ".."):
            rest = ""
        else:
            start = 1 if rest.startswith("/") else 0
            cut = rest.find("/", start)
            if cut < 0:
                cut = len(rest)
            output.append(rest[:cut])
            rest = rest[cut:]
    return "".join(output)


def _merge(base: URL, ref_path: str) -> str:
    if base.has_authority and base.path == "":
        return "/" + ref_path
    cut = base.path.rfind("/")
    return base.path[: cut + 1] + ref_path if cut >= 0 else ref_path


def resolve(base: URL | str, reference: URL | str) -> URL:
    """Resolve ``reference`` against ``base`` (RFC 3986 section 5.2.2, strict mode)."""
    base_url = parse_url(base) if isinstance(base, str) else base
    ref = parse_url(reference) if isinstance(reference, str) else reference
    if not base_url.is_absolute:
        raise URLError(f"base URL must be absolute: {base_url}")
    if ref.scheme is not None and not (ref.scheme == base_url.scheme and ref.scheme in SPECIAL_SCHEMES and not ref.has_authority):
        return replace(ref, path=remove_dot_segments(ref.path))
    if ref.has_authority:
        return URL(base_url.scheme, ref.userinfo, ref.host, ref.port,
                   remove_dot_segments(ref.path), ref.query, ref.fragment)
    if ref.path == "":
        query = ref.query if ref.query is not None else base_url.query
        return replace(base_url, query=query, fragment=ref.fragment)
    if ref.path.startswith("/"):
        path = remove_dot_segments(ref.path)
    else:
        path = remove_dot_segments(_merge(base_url, ref.path))
    return replace(base_url, path=path, query=ref.query, fragment=ref.fragment)
