"""Matching parsed selectors against the DOM."""

from __future__ import annotations

from skein.html.dom import Comment, Element, Text
from skein.html.selector import AttributeTest, ComplexSelector, Compound, SelectorList, parse_selector


def _attribute_matches(element: Element, test: AttributeTest) -> bool:
    actual = element.attrs.get(test.name)
    if actual is None:
        return False
    if test.operator is None:
        return True
    expected = test.value
    if test.ignore_case:
        actual, expected = actual.lower(), expected.lower()
    op = test.operator
    if op == "=":
        return actual == expected
    if op == "~=":
        return expected in actual.split()
    if op == "|=":
        return actual == expected or actual.startswith(expected + "-")
    if not expected:
        return False
    if op == "^=":
        return actual.startswith(expected)
    if op == "$=":
        return actual.endswith(expected)
    return expected in actual  # "*="


def _is_empty(element: Element) -> bool:
    return all(isinstance(c, Comment) or (isinstance(c, Text) and not c.data) for c in element.children)


def compound_matches(element: Element, compound: Compound) -> bool:
    """True when ``element`` satisfies every simple selector in ``compound``."""
    if compound.tag is not None and element.tag != compound.tag:
        return False
    if element.tag.startswith("#"):
        return False
    if compound.ids and any(element.attrs.get("id") != ident for ident in compound.ids):
        return False
    if compound.classes:
        classes = element.classes
        if any(name not in classes for name in compound.classes):
            return False
    if any(not _attribute_matches(element, test) for test in compound.attributes):
        return False
    for test in compound.nth:
        if element.parent is None:
            return False
        if not test.matches(element.sibling_position(of_type=test.of_type, from_end=test.from_end)):
            return False
    if compound.empty and not _is_empty(element):
        return False
    return not any(compound_matches(element, negated) for negated in compound.negations)


def _matches_from(element: Element, selector: ComplexSelector, index: int) -> bool:
    """Match ``selector.parts[:index + 1]`` with ``parts[index]`` anchored at ``element``."""
    if not compound_matches(element, selector.parts[index]):
        return False
    if index == 0:
        return True
    combinator = selector.combinators[index - 1]
    if combinator == ">":
        parent = element.parent
        return parent is not None and _matches_from(parent, selector, index - 1)
    if combinator == " ":
        return any(_matches_from(a, selector, index - 1) for a in element.ancestors())
    if combinator == "+":
        previous = element.previous_element_sibling()
        return previous is not None and _matches_from(previous, selector, index - 1)
    return any(_matches_from(s, selector, index - 1) for s in element.previous_element_siblings())


def matches(element: Element, selector: str | SelectorList) -> bool:
    """True when ``element`` matches any selector in the list."""
    parsed = parse_selector(selector) if isinstance(selector, str) else selector
    return any(_matches_from(element, s, len(s.parts) - 1) for s in parsed.selectors)
