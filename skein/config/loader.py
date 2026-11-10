"""Loading JSON configuration files (version 2, and version 1 through the compatibility mapper).

A minimal version-2 file::

    {
      "version": 2,
      "spiders": [
        {"name": "shop", "seeds": ["https://shop.example/"],
         "politeness": {"min_delay": 2},
         "extract": {"items": [{"name": "product", "url_pattern": "/product/",
                                "key": ["sku"],
                                "fields": {"sku": "#sku", "title": "h1"}}]}}
      ]
    }
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

from skein.config.build import build_project
from skein.config.compat import PathMap, emit, upgrade_v1
from skein.config.model import ProjectConfig
from skein.config.schema import ConfigError, Issue, join_path, validate
from skein.config.specs import CONFIG_VERSION, PROJECT_SPEC

SUPPORTED_VERSIONS = (1, CONFIG_VERSION)
_ENV_RE = re.compile(r"\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def expand_env(value: Any, env: Mapping[str, str], path: str = "", issues: list[Issue] | None = None) -> Any:
    """Substitute ``${NAME}`` and ``${NAME:-default}`` in every string of a JSON value.

    ``$$`` yields a literal ``$``. Unset variables without a default are
    reported as issues (with their JSON path) instead of silently becoming
    empty strings, so that a missing secret fails the load.
    """
    found = issues if issues is not None else []
    if isinstance(value, dict):
        return {k: expand_env(v, env, join_path(path, k), found) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v, env, join_path(path, i), found) for i, v in enumerate(value)]
    if not isinstance(value, str) or "$" not in value:
        return value

    def substitute(match: re.Match[str]) -> str:
        if match.group(0) == "$$":
            return "$"
        name, default = match.group(1), match.group(2)
        if name in env:
            return env[name]
        if default is not None:
            return default
        found.append(Issue(path, f"environment variable {name} is not set"))
        return ""

    return _ENV_RE.sub(substitute, value)


def _check_version(data: Any, source: str | None) -> int:
    if not isinstance(data, dict):
        return CONFIG_VERSION
    version = data.get("version")
    if version is None:
        raise ConfigError([Issue("version", f"is required (this skein reads version {CONFIG_VERSION})")], source)
    if isinstance(version, bool) or version not in SUPPORTED_VERSIONS:
        expected = " or ".join(str(v) for v in SUPPORTED_VERSIONS)
        raise ConfigError([Issue("version", f"unsupported configuration version {version!r}; expected {expected}")],
                          source)
    return int(version)


def parse_config(
    data: Any,
    *,
    base_dir: Path | None = None,
    source: str | None = None,
    env: Mapping[str, str] | None = None,
) -> ProjectConfig:
    """Validate decoded JSON and build a :class:`ProjectConfig`.

    Environment references are expanded first (from ``env``, default
    ``os.environ``). Version-1 documents are translated to version 2 and
    produce :class:`~skein.config.compat.ConfigDeprecationWarning` warnings;
    their validation errors still point at the v1 key names. Relative
    paths (``database``, ``checkpoint.directory``) are resolved against
    ``base_dir``.
    """
    env_issues: list[Issue] = []
    data = expand_env(data, os.environ if env is None else env, "", env_issues)
    if env_issues:
        raise ConfigError(env_issues, source)
    version = _check_version(data, source)
    paths = PathMap()
    notices: list[str] = []
    if version == 1:
        data, paths, notices = upgrade_v1(data)
    try:
        raw = validate(PROJECT_SPEC, data, source)
    except ConfigError as exc:
        raise paths.translate(exc) from None
    names = [s["name"] for s in raw["spiders"]]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ConfigError([Issue("spiders", f"duplicate spider name(s): {', '.join(duplicates)}")], source)
    emit(notices, source)
    return build_project(raw, version=version, base_dir=base_dir, source=source, deprecations=tuple(notices))


def load_config(path: str | Path, env: Mapping[str, str] | None = None) -> ProjectConfig:
    """Read, validate and build the configuration stored at ``path``."""
    file = Path(path)
    try:
        text = file.read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise ConfigError([Issue("", f"cannot read file: {exc.strerror or exc}")], str(file)) from exc
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(
            [Issue("", f"invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}")], str(file)
        ) from exc
    return parse_config(data, base_dir=file.resolve().parent, source=str(file), env=env)
