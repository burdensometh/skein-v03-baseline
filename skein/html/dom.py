"""A small, read-mostly DOM for scraped HTML."""

from __future__ import annotations

import re
from html import escape
from typing import Iterator

VOID_ELEMENTS = frozenset(
    "area base br col embed hr img input keygen link meta param source track wbr".split()
)
RAW_TEXT_ELEMENTS = frozenset({"script", "style", "template", "noscript"})
BLOCK_ELEMENTS = frozenset(
    """address article aside blockquote br dd details dialog div dl dt fieldset figcaption
    figure footer form h1 h2 h3 h4 h5 h6 header hr li main nav ol option p pre section
    table tbody td tfoot th thead tr ul""".split()
)
_WS_RE = re.compile(r"\s+")


def collapse_whitespace(text: str) -> str:
    return _WS_RE.sub(" ", text).strip()


class Node:
    """Base class of all nodes."""

    __slots__ = ("parent",)

    def __init__(self) -> None:
        self.parent: Element | None = None


class Text(Node):
    """Character data (entities already decoded)."""

    __slots__ = ("data",)

    def __init__(self, data: str) -> None:
        super().__init__()
        self.data = data

    def __repr__(self) -> str:
        return f"Text({self.data[:30]!r})"


class Comment(Node):
    """An HTML comment; excluded from text extraction."""

    __slots__ = ("data",)

    def __init__(self, data: str) -> None:
        super().__init__()
        self.data = data


class Element(Node):
    """An element with lower-cased tag and attribute names."""

    __slots__ = ("tag", "attrs", "children", "line", "_element_children")

    def __init__(self, tag: str, attrs: dict[str, str] | None = None, line: int = 0) -> None:
        super().__init__()
        self.tag = tag
        self.attrs: dict[str, str] = attrs or {}
        self.children: list[Node] = []
        self.line = line
        self._element_children: list[Element] | None = None

    # -- tree construction ---------------------------------------------

    def append(self, node: Node) -> None:
        node.parent = self
        if isinstance(node, Text) and self.children and isinstance(self.children[-1], Text):
            self.children[-1].data += node.data
            return
        self.children.append(node)
        if isinstance(node, Element):
            self._element_children = None

    # -- attributes ------------------------------------------------------

    def get(self, name: str, default: str | None = None) -> str | None:
        return self.attrs.get(name.lower(), default)

    def has_attr(self, name: str) -> bool:
        return name.lower() in self.attrs

    @property
    def id(self) -> str | None:
        return self.attrs.get("id")

    @property
    def classes(self) -> list[str]:
        return self.attrs.get("class", "").split()

    def has_class(self, name: str) -> bool:
        return name in self.classes

    # -- navigation ------------------------------------------------------

    @property
    def element_children(self) -> list["Element"]:
        if self._element_children is None:
            self._element_children = [c for c in self.children if isinstance(c, Element)]
        return self._element_children

    def iter(self) -> Iterator["Element"]:
        """Descendant elements in document order (excluding ``self``)."""
        stack: list[Iterator[Node]] = [iter(self.children)]
        while stack:
            node = next(stack[-1], None)
            if node is None:
                stack.pop()
                continue
            if isinstance(node, Element):
                yield node
                if node.children:
                    stack.append(iter(node.children))

    def ancestors(self) -> Iterator["Element"]:
        node = self.parent
        while node is not None:
            yield node
            node = node.parent

    def sibling_position(self, *, of_type: bool = False, from_end: bool = False) -> int:
        """1-based position among element siblings (optionally same-tag only)."""
        if self.parent is None:
            return 1
        siblings = self.parent.element_children
        if of_type:
            siblings = [s for s in siblings if s.tag == self.tag]
        index = next(i for i, s in enumerate(siblings) if s is self)
        return len(siblings) - index if from_end else index + 1

    def previous_element_sibling(self) -> "Element | None":
        if self.parent is None:
            return None
        siblings = self.parent.element_children
        index = next(i for i, s in enumerate(siblings) if s is self)
        return siblings[index - 1] if index > 0 else None

    def previous_element_siblings(self) -> Iterator["Element"]:
        if self.parent is None:
            return
        siblings = self.parent.element_children
        index = next(i for i, s in enumerate(siblings) if s is self)
        yield from reversed(siblings[:index])

    # -- querying --------------------------------------------------------

    def select(self, selector: str) -> list["Element"]:
        """Descendants matching a CSS selector, in document order, without duplicates.

        Like ``querySelectorAll``, combinators may look at ancestors of ``self``.
        """
        from skein.html.select import matches  # select.py imports this module
        from skein.html.selector import parse_selector

        parsed = parse_selector(selector)
        return [element for element in self.iter() if matches(element, parsed)]

    def select_one(self, selector: str) -> "Element | None":
        """First descendant matching ``selector`` (stops searching at the first hit)."""
        from skein.html.select import matches
        from skein.html.selector import parse_selector

        parsed = parse_selector(selector)
        return next((element for element in self.iter() if matches(element, parsed)), None)

    # -- text and serialisation -----------------------------------------

    def iter_text(self, *, separate_blocks: bool = True) -> Iterator[str]:
        """Text fragments of descendants, skipping comments and script/style.

        With ``separate_blocks`` a space is emitted before and after every
        block-level element, so ``<p>a</p>b`` reads ``a b``, not ``ab``.
        """
        stack: list[tuple[Iterator[Node], bool]] = [(iter(self.children), False)]
        while stack:
            children, block = stack[-1]
            node = next(children, None)
            if node is None:
                stack.pop()
                if block:
                    yield " "
                continue
            if isinstance(node, Text):
                yield node.data
            elif isinstance(node, Element):
                if node.tag in RAW_TEXT_ELEMENTS:
                    continue
                is_block = separate_blocks and node.tag in BLOCK_ELEMENTS
                if is_block:
                    yield " "
                stack.append((iter(node.children), is_block))

    def text(self, *, normalize: bool = True) -> str:
        """Concatenated descendant text; whitespace collapsed when ``normalize``."""
        raw = "".join(self.iter_text(separate_blocks=normalize))
        return collapse_whitespace(raw) if normalize else raw

    def own_text(self) -> str:
        """Text of direct child text nodes only."""
        return collapse_whitespace("".join(c.data for c in self.children if isinstance(c, Text)))

    def inner_html(self) -> str:
        return "".join(_serialize(child) for child in self.children)

    def outer_html(self) -> str:
        return _serialize(self)

    def __repr__(self) -> str:
        extra = "".join(f" {k}={v!r}" for k, v in list(self.attrs.items())[:3])
        return f"<{self.tag}{extra}>"


class Document(Element):
    """The root of a parsed page."""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__("#document")

    @property
    def title(self) -> str | None:
        node = self.select_one("title")
        return node.text() if node is not None else None

    @property
    def base_href(self) -> str | None:
        node = self.select_one("base[href]")
        return node.get("href") if node is not None else None

    def meta_robots(self) -> set[str]:
        """Directives from ``<meta name="robots">`` (lower-cased tokens)."""
        tokens: set[str] = set()
        for meta in self.select("meta[name]"):
            if (meta.get("name") or "").lower() in ("robots", "skein"):
                tokens.update(t.strip().lower() for t in (meta.get("content") or "").split(","))
        tokens.discard("")
        return tokens


def _serialize(root: Node) -> str:
    out: list[str] = []
    # Iterative serialisation: deeply nested junk HTML must not hit the recursion limit.
    stack: list[tuple[Node, bool]] = [(root, False)]
    while stack:
        node, closing = stack.pop()
        if isinstance(node, Text):
            raw = node.parent is not None and node.parent.tag in RAW_TEXT_ELEMENTS
            out.append(node.data if raw else escape(node.data, quote=False))
        elif isinstance(node, Comment):
            out.append(f"<!--{node.data}-->")
        elif isinstance(node, Document):
            stack.extend((child, False) for child in reversed(node.children))
        elif isinstance(node, Element):
            if closing:
                out.append(f"</{node.tag}>")
                continue
            attrs = "".join(
                f' {k}="{escape(v, quote=True)}"' if v != "" else f" {k}" for k, v in node.attrs.items()
            )
            out.append(f"<{node.tag}{attrs}>")
            if node.tag in VOID_ELEMENTS:
                continue
            stack.append((node, True))
            stack.extend((child, False) for child in reversed(node.children))
    return "".join(out)
