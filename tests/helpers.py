"""Helpers shared by the crawl tests: grouped v2 spider specs and a one-call crawl runner."""

from __future__ import annotations

import asyncio
import random
from pathlib import Path
from typing import Any

from skein.clock import FakeClock
from skein.config import parse_config
from skein.crawler import CrawlResult, Crawler
from skein.store import Store
from skein.urls import Canonicalizer
from tests.fixtures_site import FixtureSite

PRODUCT_ITEMS = [
    {
        "name": "product",
        "url_pattern": r"/product/\d+$",
        "key": ["sku"],
        "fields": {
            "sku": {"selector": "#sku", "required": True},
            "title": "h1",
            "price": {"selector": ".price span", "type": "number"},
            "tags": {"selector": "li.tag", "multiple": True},
            "in_stock": {"selector": ".stock", "type": "bool"},
        },
    }
]


def merged(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """``base`` with ``overrides`` applied; nested groups are merged key by key."""
    out = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = merged(out[key], value)
        else:
            out[key] = value
    return out


def spider_spec(site: FixtureSite, **spider: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "name": "shop",
        "seeds": [site.url("/")],
        "politeness": {"min_delay": 1.0},
        "retry": {"max_retries": 2, "backoff_base": 3.0, "backoff_factor": 1, "jitter": 0},
        "canonical": {"remove_params": ["utm_*"]},
    }
    return merged(base, spider)


def crawl(
    site: FixtureSite,
    db: Path,
    clock: FakeClock | None = None,
    http: dict[str, Any] | None = None,
    crawler: dict[str, Any] | None = None,
    project: dict[str, Any] | None = None,
    **spider: Any,
) -> CrawlResult:
    """Crawl ``site`` into ``db`` with a v2 spider built from the defaults plus ``spider`` overrides.

    ``project`` adds project-level groups (``pipeline``, ``dedup``, ``warc``, ...).
    """
    config = parse_config({"version": 2, "http": http or {}, **(project or {}),
                           "spiders": [spider_spec(site, **spider)]}, base_dir=db.parent)
    options: dict[str, Any] = {"rng": random.Random(0), "runtime": config.runtime, **(crawler or {})}

    async def go() -> CrawlResult:
        with Store(db) as store:
            runner = Crawler(config.spiders[0], store, http=config.http, clock=clock or FakeClock(), **options)
            return await runner.run()

    return asyncio.run(go())


def key(url: str) -> str:
    """``url`` as stored by a spider with the default (version 2) canonicalisation rules."""
    return Canonicalizer.strict(remove=("utm_*",)).key(url)


def events(db: Path, url: str | None = None) -> list[dict[str, Any]]:
    with Store(db) as store:
        return list(store.documents.events("shop", url=key(url) if url is not None else None))


def stored_items(db: Path) -> dict[str, Any]:
    with Store(db) as store:
        return {i.key: i for i in store.items.iter("shop")}
