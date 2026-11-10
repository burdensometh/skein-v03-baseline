"""SimHash fingerprints, the banded index in SQLite and near-duplicate handling during crawls."""

from __future__ import annotations

import random
from pathlib import Path

import pytest

from skein.dedup import NearDuplicateIndex, SimHasher, hamming, simhash
from skein.dedup.simhash import BANDS, band_values, feature_hash, shingles, to_signed, to_unsigned
from skein.store import Store
from tests.fixtures_site import FixtureSite, content
from tests.helpers import crawl

ARTICLES = [{"name": "story", "url_pattern": r"/article/\d+", "key": ["title", "edition"],
             "fields": {"title": "h1", "edition": {"selector": ".edition", "default": "plain"}}}]


def test_simhash_basics() -> None:
    assert simhash({}) == 0
    assert feature_hash("abc") == feature_hash("abc") != feature_hash("abd")
    assert shingles("A b c d") == {"a b c": 1, "b c d": 1}
    assert shingles("one two") == {"one": 1, "two": 1}
    words = content.article_text(3)
    hasher = SimHasher()
    base = hasher.of_text(words)
    assert base is not None and base == hasher.of_text(words)
    assert hamming(base, hasher.of_text(words + " plus a short trailing remark")) <= 3  # type: ignore[arg-type]
    assert hamming(base, hasher.of_text(content.article_text(4))) > 10  # type: ignore[arg-type]
    assert hasher.of_text("too short to judge") is None
    with pytest.raises(ValueError):
        SimHasher(shingle_size=0)


def test_band_and_sign_conversions() -> None:
    value = 0xFEDC_BA98_7654_3210
    assert band_values(value) == (0x3210, 0x7654, 0xBA98, 0xFEDC) and len(band_values(value)) == BANDS
    assert to_signed(value) < 0 and to_unsigned(to_signed(value)) == value
    assert to_signed(5) == 5


def test_banded_index_finds_everything_within_three_bits(tmp_path: Path) -> None:
    rng = random.Random(5)
    with Store(tmp_path / "s.db") as store:
        index = NearDuplicateIndex(store.db, max_distance=3)
        base = rng.getrandbits(64)
        with store.transaction():
            index.store("s", "http://a/base", base, 1.0)
            for n in range(200):
                index.store("s", f"http://a/noise{n}", rng.getrandbits(64), 1.0)
        for distance in range(4):
            flipped = base
            for bit in rng.sample(range(64), distance):
                flipped ^= 1 << bit
            match = index.nearest("s", "http://a/new", flipped)
            assert match is not None and match.url == "http://a/base" and match.distance == distance
        far = base ^ 0b1111
        assert index.nearest("s", "http://a/new", far) is None or index.nearest("s", "http://a/new", far).url != \
            "http://a/base"
        assert index.nearest("s", "http://a/base", base) is None or index.nearest(
            "s", "http://a/base", base).url != "http://a/base", "a page is not its own duplicate"
        assert index.stored("s", "http://a/base") == base
        with store.transaction():
            index.store("s", "http://a/base", base ^ 1, 2.0)
        assert store.db.execute("SELECT COUNT(*) FROM simhash_bands WHERE url = 'http://a/base'").fetchone()[0] == 4
        with pytest.raises(ValueError):
            NearDuplicateIndex(store.db, max_distance=4)


def test_crawl_records_near_duplicate_editions(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "news.db"
    result = crawl(site, db, seeds=[site.url("/articles")], canonical={"keep_params": ["edition"]},
                   extract={"items": ARTICLES})
    assert result.status == "finished"
    assert result.stats["dedup"]["near_duplicates"] == 8, "two editions of each of four stories"
    with Store(db) as store:
        clusters = NearDuplicateIndex(store.db).clusters("shop")
        assert len(clusters) == 4 and all(len(cluster) == 3 for cluster in clusters)
        stories = {cluster[0].split("?")[0] for cluster in clusters}
        assert stories == {site.url(f"/article/{n}") for n in range(1, 5)}
        assert store.items.count("shop") == 12, "near duplicates are only recorded by default"


@pytest.mark.parametrize("action,items", [("skip", 4), ("skip_links", 12)])
def test_near_duplicate_actions(site: FixtureSite, tmp_path: Path, action: str, items: int) -> None:
    db = tmp_path / f"{action}.db"
    result = crawl(site, db, seeds=[site.url("/articles")], canonical={"keep_params": ["edition"]},
                   extract={"items": ARTICLES}, concurrency=1, project={
                       "dedup": {"near_duplicates": {"action": action}}, "pipeline": {"write_batch": 1}})
    assert result.stats["dedup"]["near_duplicates"] == 8
    with Store(db) as store:
        assert store.items.count("shop") == items


def test_near_duplicate_detection_can_be_disabled(site: FixtureSite, tmp_path: Path) -> None:
    result = crawl(site, tmp_path / "off.db", seeds=[site.url("/articles")], canonical={"keep_params": ["edition"]},
                   project={"dedup": {"near_duplicates": {"enabled": False}}})
    assert result.stats["dedup"]["near_duplicates"] == 0


def test_relinking_from_stored_hashes_with_another_threshold(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "news.db"
    crawl(site, db, seeds=[site.url("/articles")], canonical={"keep_params": ["edition"]})
    with Store(db) as store:
        strict = NearDuplicateIndex(store.db, max_distance=0)
        with store.transaction():
            exact = strict.relink("shop")
        assert exact < 8, "fewer editions are bit-identical"
        with store.transaction():
            assert NearDuplicateIndex(store.db, max_distance=3).relink("shop") == 8
        originals = {row[0] for row in store.db.execute(
            "SELECT near_duplicate_of FROM documents WHERE near_duplicate_of IS NOT NULL")}
        firsts = {row[0] for row in store.db.execute(
            "SELECT url FROM documents WHERE url LIKE '%/article/%' AND near_duplicate_of IS NULL")}
        assert originals and all("/article/" in url for url in originals)
        assert len(firsts) == 4, "one first-fetched page per story"
