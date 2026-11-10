"""A small declarative validator for JSON configuration.

Specs validate a decoded JSON value, collect *all* problems with their JSON
path (``spiders[0].items[1].fields.price.type``) and return a normalised
value with defaults filled in. Nothing here knows about skein's settings;
see :mod:`skein.config.loader` for the actual schema.
"""

from __future__ import annotations

import copy
import difflib
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable

from skein.html.selector import SelectorError, parse_selector
from skein.urls.parse import URLError, parse_url


@dataclass(frozen=True)
class Issue:
    """One validation problem."""

    path: str
    message: str

    def __str__(self) -> str:
        return f"{self.path or '<root>'}: {self.message}"


class ConfigError(ValueError):
    """Configuration could not be loaded; ``issues`` lists every problem found."""

    def __init__(self, issues: list[Issue], source: str | None = None) -> None:
        self.issues = issues
        self.source = source
        header = f"invalid configuration{f' in {source}' if source else ''}"
        detail = "\n".join(f"  - {issue}" for issue in issues)
        super().__init__(f"{header} ({len(issues)} problem{'s' if len(issues) != 1 else ''}):\n{detail}")


class _Invalid:
    """Sentinel returned by specs for values that failed validation."""

    def __repr__(self) -> str:
        return "<invalid>"


INVALID: Any = _Invalid()
_REQUIRED: Any = object()


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    return {bool: "boolean", int: "integer", float: "number", str: "string", list: "array", dict: "object"}.get(
        type(value), type(value).__name__
    )


def join_path(base: str, key: str | int) -> str:
    if isinstance(key, int):
        return f"{base}[{key}]"
    return f"{base}.{key}" if base else key


class Spec(ABC):
    """Base class: validates ``value`` at ``path`` and appends problems to ``issues``."""

    nullable: bool = False

    def validate(self, value: Any, path: str, issues: list[Issue]) -> Any:
        if value is None and self.nullable:
            return None
        return self.check(value, path, issues)

    @abstractmethod
    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        """Validate a non-null ``value``; return the normalised value or ``INVALID``."""

    def fail(self, issues: list[Issue], path: str, message: str) -> Any:
        issues.append(Issue(path, message))
        return INVALID


class Str(Spec):
    def __init__(
        self,
        *,
        choices: tuple[str, ...] | None = None,
        non_empty: bool = False,
        pattern: str | None = None,
        nullable: bool = False,
    ) -> None:
        self.choices = choices
        self.non_empty = non_empty
        self.pattern = re.compile(pattern) if pattern else None
        self.nullable = nullable

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        if not isinstance(value, str):
            return self.fail(issues, path, f"expected a string, got {_type_name(value)}")
        if self.non_empty and not value.strip():
            return self.fail(issues, path, "must not be empty")
        if self.choices is not None and value not in self.choices:
            hint = difflib.get_close_matches(value, self.choices, n=1)
            suggestion = f"; did you mean {hint[0]!r}?" if hint else ""
            return self.fail(issues, path, f"expected one of {list(self.choices)}, got {value!r}{suggestion}")
        if self.pattern is not None and not self.pattern.fullmatch(value):
            return self.fail(issues, path, f"{value!r} does not match {self.pattern.pattern}")
        return value


class Number(Spec):
    """Integer or float with optional bounds. Booleans are rejected."""

    def __init__(
        self,
        *,
        integer: bool = False,
        minimum: float | None = None,
        maximum: float | None = None,
        nullable: bool = False,
    ) -> None:
        self.integer = integer
        self.minimum = minimum
        self.maximum = maximum
        self.nullable = nullable

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            kind = "an integer" if self.integer else "a number"
            return self.fail(issues, path, f"expected {kind}, got {_type_name(value)}")
        if self.integer and not isinstance(value, int):
            if not float(value).is_integer():
                return self.fail(issues, path, f"expected an integer, got {value}")
            value = int(value)
        if self.minimum is not None and value < self.minimum:
            return self.fail(issues, path, f"must be >= {self.minimum}, got {value}")
        if self.maximum is not None and value > self.maximum:
            return self.fail(issues, path, f"must be <= {self.maximum}, got {value}")
        return value if self.integer else float(value)


class Bool(Spec):
    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        if not isinstance(value, bool):
            return self.fail(issues, path, f"expected true or false, got {_type_name(value)}")
        return value


class Anything(Spec):
    """Accepts any JSON value unchanged (used for field defaults)."""

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        return value


class RegexStr(Str):
    """A string that must compile as a Python regular expression."""

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        value = super().check(value, path, issues)
        if value is INVALID:
            return value
        try:
            re.compile(value)
        except re.error as exc:
            return self.fail(issues, path, f"invalid regular expression {value!r}: {exc}")
        return value


class SelectorStr(Str):
    """A string that must parse as a supported CSS selector."""

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        value = super().check(value, path, issues)
        if value is INVALID:
            return value
        try:
            parse_selector(value)
        except SelectorError as exc:
            return self.fail(issues, path, f"invalid CSS selector: {exc}")
        return value


class HttpUrl(Str):
    """An absolute http(s) URL."""

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        value = super().check(value, path, issues)
        if value is INVALID:
            return value
        try:
            parsed = parse_url(value)
        except URLError as exc:
            return self.fail(issues, path, f"invalid URL: {exc}")
        if parsed.scheme not in ("http", "https") or not parsed.host:
            return self.fail(issues, path, f"expected an absolute http(s) URL, got {value!r}")
        return value


class ListOf(Spec):
    def __init__(
        self, item: Spec, *, min_items: int = 0, unique: bool = False, nullable: bool = False
    ) -> None:
        self.item = item
        self.min_items = min_items
        self.unique = unique
        self.nullable = nullable

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        if not isinstance(value, list):
            return self.fail(issues, path, f"expected an array, got {_type_name(value)}")
        if len(value) < self.min_items:
            return self.fail(issues, path, f"needs at least {self.min_items} element(s)")
        out = [self.item.validate(v, join_path(path, i), issues) for i, v in enumerate(value)]
        if self.unique:
            seen: set[Any] = set()
            for index, item in enumerate(out):
                if not isinstance(item, (str, int, float)):
                    continue
                if item in seen:
                    issues.append(Issue(join_path(path, index), f"duplicate value {item!r}"))
                seen.add(item)
        return out


class MapOf(Spec):
    """A JSON object with arbitrary keys and uniformly validated values."""

    def __init__(self, value: Spec, *, key_pattern: str | None = None, min_items: int = 0) -> None:
        self.value = value
        self.key_pattern = re.compile(key_pattern) if key_pattern else None
        self.min_items = min_items

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        if not isinstance(value, dict):
            return self.fail(issues, path, f"expected an object, got {_type_name(value)}")
        if len(value) < self.min_items:
            return self.fail(issues, path, f"needs at least {self.min_items} entr{'y' if self.min_items == 1 else 'ies'}")
        out = {}
        for key, item in value.items():
            if self.key_pattern is not None and not self.key_pattern.fullmatch(key):
                issues.append(Issue(join_path(path, key), f"invalid name {key!r} (allowed: {self.key_pattern.pattern})"))
                continue
            out[key] = self.value.validate(item, join_path(path, key), issues)
        return out


@dataclass(frozen=True)
class Field:
    """A member of an :class:`Obj`. Without a ``default`` the member is required."""

    spec: Spec
    default: Any = _REQUIRED

    @property
    def required(self) -> bool:
        return self.default is _REQUIRED


class Obj(Spec):
    """A JSON object with known keys.

    ``shorthand`` names a field that a bare string expands into, e.g. a field
    rule given as ``"h1.title"`` becomes ``{"selector": "h1.title"}``.
    ``checks`` run on the normalised object for cross-field rules and return
    ``(key, message)`` problems; they are skipped when a direct member of
    the object is itself invalid.
    """

    def __init__(
        self,
        fields: dict[str, Field],
        *,
        shorthand: str | None = None,
        checks: tuple[Callable[[dict[str, Any]], list[tuple[str, str]]], ...] = (),
    ) -> None:
        self.fields = fields
        self.shorthand = shorthand
        self.checks = checks

    def check(self, value: Any, path: str, issues: list[Issue]) -> Any:
        if isinstance(value, str) and self.shorthand is not None:
            value = {self.shorthand: value}
        if not isinstance(value, dict):
            return self.fail(issues, path, f"expected an object, got {_type_name(value)}")
        out: dict[str, Any] = {}
        for key in value:
            if key not in self.fields:
                hint = difflib.get_close_matches(key, list(self.fields), n=1, cutoff=0.6)
                suggestion = f"; did you mean {hint[0]!r}?" if hint else ""
                issues.append(Issue(join_path(path, key), f"unknown key{suggestion}"))
        for key, field in self.fields.items():
            if key in value:
                out[key] = field.spec.validate(value[key], join_path(path, key), issues)
            elif field.required:
                issues.append(Issue(join_path(path, key), "is required"))
                out[key] = INVALID
            else:
                # Defaults go through the spec too, so nested objects get their own defaults.
                default = copy.deepcopy(field.default)
                out[key] = None if default is None else field.spec.validate(default, join_path(path, key), issues)
        if not any(v is INVALID for v in out.values()):
            for check in self.checks:
                for key, message in check(out):
                    issues.append(Issue(join_path(path, key) if key else path, message))
        return out


def validate(spec: Spec, value: Any, source: str | None = None) -> Any:
    """Validate ``value`` against ``spec``; raise :class:`ConfigError` listing every issue."""
    issues: list[Issue] = []
    result = spec.validate(value, "", issues)
    if issues:
        raise ConfigError(issues, source)
    return result
