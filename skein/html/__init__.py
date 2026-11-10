"""Lightweight HTML DOM built on html.parser, with a CSS selector subset."""

from skein.html.builder import parse_html
from skein.html.dom import Comment, Document, Element, Node, Text
from skein.html.select import compound_matches, matches
from skein.html.selector import SelectorError, parse_selector

__all__ = [
    "Comment",
    "Document",
    "Element",
    "Node",
    "SelectorError",
    "Text",
    "compound_matches",
    "matches",
    "parse_html",
    "parse_selector",
]
