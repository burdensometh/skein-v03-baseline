"""Reading the Prometheus text format back, and merging the textfiles of several workers.

Every worker process writes its own textfile (one writer per file keeps
the atomic replacement simple). :func:`merge` combines them into one
exposition: samples whose label sets become identical once the dropped
labels (by default ``worker``) are removed are added up -- counters,
gauges and histogram buckets, sums and counts alike.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

from skein.metrics.prometheus import escape_help, escape_label_value, format_value

_SAMPLE_RE = re.compile(r"(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+(?P<value>\S+)(?:\s+\d+)?\Z")
_LABEL_RE = re.compile(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"\s*(?:,|\Z)')
_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")


class ExpositionError(ValueError):
    """Text that is not valid Prometheus exposition format."""


@dataclass
class Sample:
    name: str
    labels: dict[str, str]
    value: float


@dataclass
class Family:
    name: str
    kind: str = "untyped"
    help: str = ""
    samples: list[Sample] = field(default_factory=list)


def _unescape(value: str) -> str:
    out = []
    chars = iter(value)
    for char in chars:
        if char != "\\":
            out.append(char)
            continue
        following = next(chars, "")
        out.append({"n": "\n", "\\": "\\", '"': '"'}.get(following, "\\" + following))
    return "".join(out)


def _parse_value(text: str, line_number: int) -> float:
    if text in ("+Inf", "Inf"):
        return math.inf
    if text == "-Inf":
        return -math.inf
    if text == "NaN":
        return math.nan
    try:
        return float(text)
    except ValueError as exc:
        raise ExpositionError(f"line {line_number}: bad sample value {text!r}") from exc


def _parse_labels(text: str, line_number: int) -> dict[str, str]:
    labels: dict[str, str] = {}
    position = 0
    while position < len(text):
        match = _LABEL_RE.match(text, position)
        if match is None:
            raise ExpositionError(f"line {line_number}: malformed labels {text[position:position + 40]!r}")
        if match.group(1) in labels:
            raise ExpositionError(f"line {line_number}: duplicate label {match.group(1)!r}")
        labels[match.group(1)] = _unescape(match.group(2))
        position = match.end()
    return labels


def parse(text: str) -> dict[str, Family]:
    """Families by name, in order of appearance; histogram series are filed under their family."""
    families: dict[str, Family] = {}
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            parts = line.split(None, 3)
            if len(parts) >= 3 and parts[1] in ("HELP", "TYPE"):
                family = families.setdefault(parts[2], Family(parts[2]))
                rest = parts[3] if len(parts) > 3 else ""
                if parts[1] == "HELP":
                    family.help = _unescape(rest)
                elif rest not in ("counter", "gauge", "histogram", "summary", "untyped"):
                    raise ExpositionError(f"line {number}: unknown metric type {rest!r}")
                else:
                    family.kind = rest
            continue
        match = _SAMPLE_RE.match(line)
        if match is None:
            raise ExpositionError(f"line {number}: cannot parse sample {line[:60]!r}")
        name = match.group("name")
        owner = name
        for suffix in _HISTOGRAM_SUFFIXES:
            base = name[: -len(suffix)]
            if name.endswith(suffix) and base in families and families[base].kind == "histogram":
                owner = base
        sample = Sample(name, _parse_labels(match.group("labels") or "", number),
                        _parse_value(match.group("value"), number))
        families.setdefault(owner, Family(owner)).samples.append(sample)
    return families


def merge(texts: list[str], *, drop_labels: tuple[str, ...] = ("worker",)) -> str:
    """One exposition summing every series of ``texts`` after removing ``drop_labels``."""
    merged: dict[str, Family] = {}
    totals: dict[str, dict[tuple[str, tuple[tuple[str, str], ...]], float]] = {}
    for text in texts:
        for name, family in parse(text).items():
            target = merged.setdefault(name, Family(name, family.kind, family.help))
            if target.kind != family.kind:
                raise ExpositionError(f"{name} is a {target.kind} in one file and a {family.kind} in another")
            series = totals.setdefault(name, {})
            for sample in family.samples:
                labels = tuple(sorted((k, v) for k, v in sample.labels.items() if k not in drop_labels))
                key = (sample.name, labels)
                series[key] = series.get(key, 0.0) + sample.value
    lines: list[str] = []
    for name, family in merged.items():
        lines.append(f"# HELP {name} {escape_help(family.help)}")
        lines.append(f"# TYPE {name} {family.kind}")
        for (sample_name, labels), value in sorted(totals[name].items(), key=_series_order):
            rendered = ",".join(f'{k}="{escape_label_value(v)}"' for k, v in labels)
            lines.append(f"{sample_name}{{{rendered}}} {format_value(value)}" if rendered
                         else f"{sample_name} {format_value(value)}")
    return "\n".join(lines) + "\n" if lines else ""


def _series_order(item: tuple[tuple[str, tuple[tuple[str, str], ...]], float]) -> tuple[object, ...]:
    """Sort series by name and labels, ``le`` buckets numerically so ``+Inf`` comes last."""
    (name, labels), _ = item
    plain = tuple((k, v) for k, v in labels if k != "le")
    bound = next((v for k, v in labels if k == "le"), None)
    numeric = math.inf if bound in ("+Inf", None) else float(bound)
    return (plain, name, numeric)
