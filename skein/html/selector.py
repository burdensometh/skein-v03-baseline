"""Parser for the supported CSS selector subset.

Supported syntax::

    *  tag  .class  #id
    [attr]  [attr=v]  [attr~=v]  [attr|=v]  [attr^=v]  [attr$=v]  [attr*=v]  [attr=v i]
    :nth-child(an+b)  :nth-last-child(an+b)  :nth-of-type(an+b)
    :first-child  :last-child  :only-child  :first-of-type  :last-of-type  :empty
    :not(compound)
    A B (descendant)  A > B (child)  A + B (adjacent)  A ~ B (sibling)  A, B (group)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache


class SelectorError(ValueError):
    """A selector string is malformed or uses unsupported syntax."""

    def __init__(self, message: str, selector: str, position: int) -> None:
        super().__init__(f"{message} at position {position} in {selector!r}")
        self.selector = selector
        self.position = position


@dataclass(frozen=True)
class AttributeTest:
    name: str
    operator: str | None = None
    value: str = ""
    ignore_case: bool = False


@dataclass(frozen=True)
class NthTest:
    """``a*n + b`` position test; ``of_type``/``from_end`` select the variant."""

    a: int
    b: int
    of_type: bool = False
    from_end: bool = False

    def matches(self, position: int) -> bool:
        if self.a == 0:
            return position == self.b
        n, rem = divmod(position - self.b, self.a)
        return rem == 0 and n >= 0


@dataclass(frozen=True)
class Compound:
    """A sequence of simple selectors with no combinator between them."""

    tag: str | None = None
    ids: tuple[str, ...] = ()
    classes: tuple[str, ...] = ()
    attributes: tuple[AttributeTest, ...] = ()
    nth: tuple[NthTest, ...] = ()
    empty: bool = False
    negations: tuple["Compound", ...] = ()


@dataclass(frozen=True)
class ComplexSelector:
    """Compounds joined by combinators; ``combinators[i]`` sits between parts i and i+1."""

    parts: tuple[Compound, ...]
    combinators: tuple[str, ...]


@dataclass(frozen=True)
class SelectorList:
    selectors: tuple[ComplexSelector, ...]
    source: str


_IDENT_RE = re.compile(r"-?(?:[A-Za-z_]|[^\x00-\x7f]|\\.)(?:[\w\-]|[^\x00-\x7f]|\\.)*")
_NTH_RE = re.compile(r"^(?:(?P<a>[+-]?\d*)n\s*(?:(?P<sign>[+-])\s*(?P<b>\d+))?|(?P<only>[+-]?\d+))$")
_ATTR_OPERATORS = ("~=", "|=", "^=", "$=", "*=", "=")
_STRUCTURAL = {
    "first-child": NthTest(0, 1),
    "last-child": NthTest(0, 1, from_end=True),
    "first-of-type": NthTest(0, 1, of_type=True),
    "last-of-type": NthTest(0, 1, of_type=True, from_end=True),
}


def parse_nth(expression: str) -> tuple[int, int]:
    """Parse the argument of ``:nth-child()`` into ``(a, b)``."""
    expr = expression.strip().lower()
    if expr == "odd":
        return 2, 1
    if expr == "even":
        return 2, 0
    match = _NTH_RE.match(expr)
    if match is None:
        raise ValueError(f"invalid nth expression {expression!r}")
    if match.group("only") is not None:
        return 0, int(match.group("only"))
    a_text = match.group("a")
    a = -1 if a_text == "-" else 1 if a_text in ("", "+") else int(a_text)
    b = int(match.group("b") or 0)
    if match.group("sign") == "-":
        b = -b
    return a, b


class _Parser:
    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def error(self, message: str) -> SelectorError:
        return SelectorError(message, self.text, self.pos)

    def peek(self) -> str:
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def skip_ws(self) -> bool:
        start = self.pos
        while self.pos < len(self.text) and self.text[self.pos] in " \t\r\n\f":
            self.pos += 1
        return self.pos > start

    def ident(self) -> str:
        match = _IDENT_RE.match(self.text, self.pos)
        if match is None:
            raise self.error("expected an identifier")
        self.pos = match.end()
        return re.sub(r"\\(.)", r"\1", match.group(0))

    def string_or_ident(self) -> str:
        quote = self.peek()
        if quote not in ("'", '"'):
            return self.ident()
        self.pos += 1
        out: list[str] = []
        while True:
            ch = self.peek()
            if not ch:
                raise self.error("unterminated string")
            self.pos += 1
            if ch == "\\":
                out.append(self.peek())
                self.pos += 1
            elif ch == quote:
                return "".join(out)
            else:
                out.append(ch)

    def parse_list(self) -> SelectorList:
        selectors = [self.parse_complex()]
        while self.peek() == ",":
            self.pos += 1
            selectors.append(self.parse_complex())
        if self.pos != len(self.text):
            raise self.error(f"unexpected character {self.peek()!r}")
        return SelectorList(tuple(selectors), self.text)

    def parse_complex(self) -> ComplexSelector:
        self.skip_ws()
        parts = [self.parse_compound()]
        combinators: list[str] = []
        while True:
            had_ws = self.skip_ws()
            ch = self.peek()
            if ch in (">", "+", "~"):
                self.pos += 1
                self.skip_ws()
                combinators.append(ch)
            elif ch in ("", ","):
                break
            elif had_ws:
                combinators.append(" ")
            else:
                raise self.error(f"unexpected character {ch!r}")
            parts.append(self.parse_compound())
        return ComplexSelector(tuple(parts), tuple(combinators))

    def parse_compound(self, *, nested: bool = False) -> Compound:
        tag: str | None = None
        ids: list[str] = []
        classes: list[str] = []
        attributes: list[AttributeTest] = []
        nth: list[NthTest] = []
        negations: list[Compound] = []
        empty = False
        start = self.pos
        if self.peek() == "*":
            self.pos += 1
        elif _IDENT_RE.match(self.text, self.pos):
            tag = self.ident().lower()
        while True:
            ch = self.peek()
            if ch == "#":
                self.pos += 1
                ids.append(self.ident())
            elif ch == ".":
                self.pos += 1
                classes.append(self.ident())
            elif ch == "[":
                attributes.append(self.parse_attribute())
            elif ch == ":":
                self.pos += 1
                name = self.ident().lower()
                if name == "not":
                    if nested:
                        raise self.error(":not() cannot be nested")
                    self.expect("(")
                    self.skip_ws()
                    negations.append(self.parse_compound(nested=True))
                    self.skip_ws()
                    self.expect(")")
                elif name in ("nth-child", "nth-last-child", "nth-of-type", "nth-last-of-type"):
                    self.expect("(")
                    close = self.text.find(")", self.pos)
                    if close < 0:
                        raise self.error("unterminated :nth argument")
                    try:
                        a, b = parse_nth(self.text[self.pos : close])
                    except ValueError as exc:
                        raise self.error(str(exc)) from exc
                    self.pos = close + 1
                    nth.append(NthTest(a, b, of_type="of-type" in name, from_end="last" in name))
                elif name in _STRUCTURAL:
                    nth.append(_STRUCTURAL[name])
                elif name == "only-child":
                    nth.extend((_STRUCTURAL["first-child"], _STRUCTURAL["last-child"]))
                elif name == "empty":
                    empty = True
                else:
                    raise self.error(f"unsupported pseudo-class :{name}")
            else:
                break
        if self.pos == start:
            raise self.error("expected a selector")
        return Compound(tag, tuple(ids), tuple(classes), tuple(attributes), tuple(nth), empty, tuple(negations))

    def expect(self, char: str) -> None:
        if self.peek() != char:
            raise self.error(f"expected {char!r}")
        self.pos += 1

    def parse_attribute(self) -> AttributeTest:
        self.expect("[")
        self.skip_ws()
        name = self.ident().lower()
        self.skip_ws()
        operator = next((op for op in _ATTR_OPERATORS if self.text.startswith(op, self.pos)), None)
        if operator is None:
            self.expect("]")
            return AttributeTest(name)
        self.pos += len(operator)
        self.skip_ws()
        value = self.string_or_ident() if self.peek() in ("'", '"') else self._bare_value()
        self.skip_ws()
        ignore_case = False
        if self.peek().lower() == "i":
            self.pos += 1
            ignore_case = True
            self.skip_ws()
        self.expect("]")
        return AttributeTest(name, operator, value, ignore_case)

    def _bare_value(self) -> str:
        start = self.pos
        while self.pos < len(self.text) and self.text[self.pos] not in " \t\r\n]":
            self.pos += 1
        if self.pos == start:
            raise self.error("expected an attribute value")
        return self.text[start : self.pos]


@lru_cache(maxsize=512)
def parse_selector(text: str) -> SelectorList:
    """Parse and cache a selector list; raises :class:`SelectorError`."""
    if not text or not text.strip():
        raise SelectorError("empty selector", text, 0)
    return _Parser(text.strip()).parse_list()
