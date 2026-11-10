"""Metric families and the Prometheus text exposition format (version 0.0.4).

A :class:`Registry` holds metric families -- counters, gauges and
histograms, each with a fixed set of label names -- and renders them as::

    # HELP skein_requests_total HTTP requests sent.
    # TYPE skein_requests_total counter
    skein_requests_total{spider="shop",worker="local"} 42

Histograms expose cumulative ``_bucket`` series with an ``le`` label
(ending in ``+Inf``) plus ``_sum`` and ``_count``. Label values are
escaped (backslash, double quote, newline), so are HELP texts (backslash,
newline). :func:`write_textfile` replaces a file atomically, for the
node_exporter textfile collector.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Union

from skein.checkpoint.snapshot import atomic_write
from skein.metrics.stats import Histogram

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
KINDS = ("counter", "gauge", "histogram")
_METRIC_NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*\Z")
_LABEL_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*\Z")

LabelValues = tuple[str, ...]
Sample = Union[float, Histogram]


def format_value(value: float) -> str:
    """A sample value as Go's ``strconv`` would print it (``+Inf``, ``NaN``, integers without ``.0``)."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if float(value).is_integer() and abs(value) < 1e15:
        return str(int(value))
    return repr(float(value))


def escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def escape_help(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\n", "\\n")


def _labels(names: Iterable[str], values: Iterable[str], extra: tuple[tuple[str, str], ...] = ()) -> str:
    pairs = [*zip(names, values), *extra]
    if not pairs:
        return ""
    return "{" + ",".join(f'{name}="{escape_label_value(value)}"' for name, value in pairs) + "}"


@dataclass
class MetricFamily:
    """One metric name with its type, help text, label names and samples."""

    name: str
    kind: str
    help: str
    label_names: tuple[str, ...] = ()
    buckets: tuple[float, ...] = ()
    samples: dict[LabelValues, Sample] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _METRIC_NAME.match(self.name):
            raise ValueError(f"invalid metric name {self.name!r}")
        if self.kind not in KINDS:
            raise ValueError(f"unknown metric type {self.kind!r}")
        for label in self.label_names:
            if not _LABEL_NAME.match(label) or label.startswith("__") or label == "le":
                raise ValueError(f"invalid label name {label!r} for {self.name}")
        if self.kind == "counter" and not self.name.endswith("_total"):
            raise ValueError(f"counter {self.name} should end in _total")

    def _key(self, labels: dict[str, object]) -> LabelValues:
        if set(labels) != set(self.label_names):
            raise ValueError(f"{self.name} takes labels {self.label_names}, got {tuple(sorted(labels))}")
        return tuple(str(labels[name]) for name in self.label_names)

    def set(self, value: float, **labels: object) -> None:
        if self.kind == "histogram":
            raise TypeError(f"{self.name} is a histogram; use observe() or set_histogram()")
        if self.kind == "counter" and value < 0:
            raise ValueError(f"counter {self.name} cannot be negative")
        self.samples[self._key(labels)] = float(value)

    def inc(self, amount: float = 1.0, **labels: object) -> None:
        if self.kind == "histogram":
            raise TypeError(f"{self.name} is a histogram")
        if self.kind == "counter" and amount < 0:
            raise ValueError(f"counter {self.name} can only increase")
        key = self._key(labels)
        current = self.samples.get(key, 0.0)
        assert isinstance(current, float)
        self.samples[key] = current + amount

    def observe(self, value: float, **labels: object) -> None:
        if self.kind != "histogram":
            raise TypeError(f"{self.name} is not a histogram")
        key = self._key(labels)
        histogram = self.samples.get(key)
        if histogram is None:
            histogram = Histogram(self.buckets)
            self.samples[key] = histogram
        assert isinstance(histogram, Histogram)
        histogram.observe(value)

    def set_histogram(self, histogram: Histogram, **labels: object) -> None:
        if self.kind != "histogram":
            raise TypeError(f"{self.name} is not a histogram")
        self.samples[self._key(labels)] = histogram

    def render(self) -> list[str]:
        lines = [f"# HELP {self.name} {escape_help(self.help)}", f"# TYPE {self.name} {self.kind}"]
        for key in sorted(self.samples):
            sample = self.samples[key]
            if isinstance(sample, Histogram):
                for bound, count in sample.cumulative():
                    labels = _labels(self.label_names, key, (("le", bound),))
                    lines.append(f"{self.name}_bucket{labels} {count}")
                plain = _labels(self.label_names, key)
                lines.append(f"{self.name}_sum{plain} {format_value(sample.total)}")
                lines.append(f"{self.name}_count{plain} {sample.count}")
            else:
                lines.append(f"{self.name}{_labels(self.label_names, key)} {format_value(sample)}")
        return lines


class Registry:
    """An ordered collection of metric families sharing a name prefix."""

    def __init__(self, prefix: str = "skein_") -> None:
        self.prefix = prefix
        self._families: dict[str, MetricFamily] = {}

    def _family(self, name: str, kind: str, help: str, labels: tuple[str, ...],
                buckets: tuple[float, ...] = ()) -> MetricFamily:
        full = self.prefix + name
        family = self._families.get(full)
        if family is None:
            family = MetricFamily(full, kind, help, labels, buckets)
            self._families[full] = family
        elif (family.kind, family.label_names) != (kind, labels):
            raise ValueError(f"metric {full} already registered as {family.kind}{family.label_names}")
        return family

    def counter(self, name: str, help: str, labels: tuple[str, ...] = ()) -> MetricFamily:
        return self._family(name, "counter", help, labels)

    def gauge(self, name: str, help: str, labels: tuple[str, ...] = ()) -> MetricFamily:
        return self._family(name, "gauge", help, labels)

    def histogram(self, name: str, help: str, buckets: tuple[float, ...], labels: tuple[str, ...] = ()) -> MetricFamily:
        if list(buckets) != sorted(buckets) or not buckets:
            raise ValueError("histogram buckets must be a non-empty ascending sequence")
        return self._family(name, "histogram", help, labels, tuple(buckets))

    def families(self) -> list[MetricFamily]:
        return list(self._families.values())

    def get(self, name: str) -> MetricFamily | None:
        return self._families.get(self.prefix + name)

    def render(self) -> str:
        lines: list[str] = []
        for family in self._families.values():
            if family.samples:
                lines.extend(family.render())
        return "\n".join(lines) + "\n" if lines else ""


def write_textfile(path: Path, registry: Registry) -> None:
    """Atomically replace ``path`` with the registry's exposition (textfile-collector style)."""
    atomic_write(Path(path), registry.render().encode("utf-8"))
