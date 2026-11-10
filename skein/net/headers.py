"""Case-insensitive, order-preserving HTTP header collection."""

from __future__ import annotations

from typing import Iterable, Iterator, Mapping


class Headers:
    """An ordered multimap of HTTP header fields.

    Lookup is case-insensitive; the original spelling and order are kept for
    serialisation. Repeated fields (``Set-Cookie``, ``Link``) are preserved.
    """

    __slots__ = ("_items",)

    def __init__(self, items: Iterable[tuple[str, str]] | Mapping[str, str] | None = None) -> None:
        self._items: list[tuple[str, str]] = []
        if items is None:
            return
        pairs = items.items() if isinstance(items, Mapping) else items
        for name, value in pairs:
            self.add(name, value)

    @staticmethod
    def _check(name: str, value: str) -> None:
        if not name or any(c in name for c in " \t\r\n:"):
            raise ValueError(f"invalid header name: {name!r}")
        if "\r" in value or "\n" in value:
            raise ValueError(f"header value for {name!r} contains a line break")

    def add(self, name: str, value: str) -> None:
        """Append a field, keeping any existing fields with the same name."""
        value = str(value)
        self._check(name, value)
        self._items.append((name, value))

    def set(self, name: str, value: str) -> None:
        """Replace all fields called ``name`` with a single value."""
        value = str(value)
        self._check(name, value)
        lowered = name.lower()
        kept = [(n, v) for n, v in self._items if n.lower() != lowered]
        kept.append((name, value))
        self._items = kept

    def remove(self, name: str) -> None:
        lowered = name.lower()
        self._items = [(n, v) for n, v in self._items if n.lower() != lowered]

    def get(self, name: str, default: str | None = None) -> str | None:
        """Return the first value for ``name``."""
        lowered = name.lower()
        for n, v in self._items:
            if n.lower() == lowered:
                return v
        return default

    def get_all(self, name: str) -> list[str]:
        lowered = name.lower()
        return [v for n, v in self._items if n.lower() == lowered]

    def tokens(self, name: str) -> list[str]:
        """Lower-cased comma-separated tokens of a list-valued field."""
        out: list[str] = []
        for value in self.get_all(name):
            out.extend(t.strip().lower() for t in value.split(",") if t.strip())
        return out

    def items(self) -> list[tuple[str, str]]:
        return list(self._items)

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and self.get(name) is not None

    def __getitem__(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def __iter__(self) -> Iterator[str]:
        return iter(n for n, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Headers):
            return NotImplemented
        norm = lambda h: sorted((n.lower(), v) for n, v in h._items)  # noqa: E731
        return norm(self) == norm(other)

    def __repr__(self) -> str:
        return f"Headers({self._items!r})"


def parse_header_params(value: str) -> tuple[str, dict[str, str]]:
    """Split ``text/html; charset="utf-8"`` into ``("text/html", {"charset": "utf-8"})``.

    Parameter names are lower-cased; quoted values are unescaped. Malformed
    parameters are skipped rather than rejected, as real servers send plenty.
    """
    parts = _split_unquoted(value, ";")
    main = parts[0].strip().lower() if parts else ""
    params: dict[str, str] = {}
    for part in parts[1:]:
        if "=" not in part:
            continue
        name, _, raw = part.partition("=")
        name = name.strip().lower()
        raw = raw.strip()
        if not name:
            continue
        if len(raw) >= 2 and raw[0] == raw[-1] == '"':
            raw = _unquote(raw[1:-1])
        params.setdefault(name, raw)
    return main, params


def _split_unquoted(value: str, sep: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    escaped = False
    for ch in value:
        if escaped:
            current.append(ch)
            escaped = False
        elif ch == "\\" and quoted:
            current.append(ch)
            escaped = True
        elif ch == '"':
            quoted = not quoted
            current.append(ch)
        elif ch == sep and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return parts


def _unquote(text: str) -> str:
    out: list[str] = []
    chars = iter(text)
    for ch in chars:
        if ch == "\\":
            out.append(next(chars, ""))
        else:
            out.append(ch)
    return "".join(out)
