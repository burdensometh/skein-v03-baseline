"""Tolerant tree construction on top of :class:`html.parser.HTMLParser`.

This is not the full HTML5 tree-construction algorithm. It implements the
parts that matter for extraction from real-world pages: void elements,
implied end tags for ``p``/``li``/``dt``/``dd``/table parts/``option``,
recovery from misnested and stray end tags, duplicate ``html``/``body``
tags, and a nesting limit that keeps pathological documents bounded.
"""

from __future__ import annotations

from html.parser import HTMLParser

from skein.html.dom import VOID_ELEMENTS, Comment, Document, Element, Text

MAX_DEPTH = 256

_CLOSES_P = frozenset(
    """address article aside blockquote details dialog div dl fieldset figcaption figure
    footer form h1 h2 h3 h4 h5 h6 header hr li main menu nav ol p pre section table ul""".split()
)

_P_SCOPE_BOUNDARIES = frozenset({"button", "table", "td", "th", "li", "div"})

# start tag -> (open tags it implicitly closes, tags that bound the search).
# Elements between the top of the stack and the closed element are closed too.
_IMPLIED_END: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "li": (frozenset({"li"}), frozenset({"ul", "ol", "menu", "table"})),
    "dt": (frozenset({"dt", "dd"}), frozenset({"dl", "table"})),
    "dd": (frozenset({"dt", "dd"}), frozenset({"dl", "table"})),
    "tr": (frozenset({"tr"}), frozenset({"table", "thead", "tbody", "tfoot"})),
    "td": (frozenset({"td", "th"}), frozenset({"tr", "table"})),
    "th": (frozenset({"td", "th"}), frozenset({"tr", "table"})),
    "thead": (frozenset({"thead", "tbody", "tfoot", "tr"}), frozenset({"table"})),
    "tbody": (frozenset({"thead", "tbody", "tfoot", "tr"}), frozenset({"table"})),
    "tfoot": (frozenset({"thead", "tbody", "tfoot", "tr"}), frozenset({"table"})),
    "option": (frozenset({"option"}), frozenset({"select", "datalist", "optgroup"})),
    "optgroup": (frozenset({"optgroup"}), frozenset({"select"})),
}
_HEAD_CONTENT = frozenset({"title", "meta", "link", "style", "script", "base", "noscript", "template"})
# end tags that may not close elements beyond these boundaries
_END_TAG_BOUNDARIES = frozenset({"table", "td", "th", "template", "html"})
_SINGLETONS = frozenset({"html", "head", "body"})


class TreeBuilder(HTMLParser):
    """Feeds markup and builds a :class:`~skein.html.dom.Document`."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.document = Document()
        self._stack: list[Element] = [self.document]
        self._singletons: dict[str, Element] = {}
        self.recoveries = 0

    @property
    def _current(self) -> Element:
        return self._stack[-1]

    def _pop_until(self, index: int) -> None:
        del self._stack[index:]

    def _close_head(self) -> None:
        for index in range(len(self._stack) - 1, 0, -1):
            if self._stack[index].tag == "head":
                self._pop_until(index)
                return

    def _close_implied(self, tag: str) -> None:
        if tag not in _HEAD_CONTENT and tag != "head":
            self._close_head()
        if tag in _CLOSES_P:
            self._close_open("p", _P_SCOPE_BOUNDARIES)
        rule = _IMPLIED_END.get(tag)
        if rule is None:
            return
        closable, boundaries = rule
        for index in range(len(self._stack) - 1, 0, -1):
            name = self._stack[index].tag
            if name in boundaries:
                return
            if name in closable:
                self._pop_until(index)
                self.recoveries += 1
                return

    def _close_open(self, tag: str, boundaries: frozenset[str]) -> None:
        for index in range(len(self._stack) - 1, 0, -1):
            name = self._stack[index].tag
            if name == tag:
                self._pop_until(index)
                return
            if name in boundaries:
                return

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._start(tag, attrs, self_closing=False)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._start(tag, attrs, self_closing=True)

    def _start(self, tag: str, attrs: list[tuple[str, str | None]], self_closing: bool) -> None:
        attributes: dict[str, str] = {}
        for name, value in attrs:
            attributes.setdefault(name.lower(), value if value is not None else "")
        if tag in _SINGLETONS and tag in self._singletons:
            existing = self._singletons[tag]
            for name, value in attributes.items():
                existing.attrs.setdefault(name, value)
            self.recoveries += 1
            return
        self._close_implied(tag)
        element = Element(tag, attributes, self.getpos()[0])
        self._current.append(element)
        if tag in _SINGLETONS:
            self._singletons[tag] = element
        if tag in VOID_ELEMENTS or self_closing:
            return
        if len(self._stack) > MAX_DEPTH:
            self.recoveries += 1
            return
        self._stack.append(element)

    def handle_endtag(self, tag: str) -> None:
        if tag in VOID_ELEMENTS:
            if tag == "br":
                self._current.append(Element("br", {}, self.getpos()[0]))
            return
        for index in range(len(self._stack) - 1, 0, -1):
            name = self._stack[index].tag
            if name == tag:
                if index != len(self._stack) - 1:
                    self.recoveries += 1
                self._pop_until(index)
                return
            if name in _END_TAG_BOUNDARIES and tag not in _END_TAG_BOUNDARIES:
                break
        self.recoveries += 1

    def handle_data(self, data: str) -> None:
        if not data:
            return
        if data.strip() and self._current.tag == "head":
            self._close_head()
        self._current.append(Text(data))

    def handle_comment(self, data: str) -> None:
        self._current.append(Comment(data))

    def unknown_decl(self, data: str) -> None:
        if data.upper().startswith("CDATA["):
            self._current.append(Text(data[6:]))


def parse_html(markup: str) -> Document:
    """Parse ``markup`` into a :class:`Document`; never raises on malformed input."""
    builder = TreeBuilder()
    try:
        builder.feed(markup)
        builder.close()
    except (AssertionError, ValueError, IndexError):
        # html.parser can still trip over truly bizarre declarations; keep what we built.
        builder.recoveries += 1
    return builder.document
