"""The seen-URL Bloom filter: false-positive rate, file format, corruption handling, sharing between workers."""

from __future__ import annotations

import struct
import zlib
from pathlib import Path

import pytest

from skein.clock import FakeClock
from skein.dedup import BloomFileError, BloomFilter, SeenUrls, optimal_parameters
from skein.dedup.bloomfile import HEADER_SIZE, MAGIC, decode, encode, load, read_header, save
from skein.frontier import CrawlRequest, Frontier
from skein.store import Store
from skein.store.schema_workers import database_id
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl

IDENTITY = bytes(range(16))


def test_parameters_and_false_positive_rate() -> None:
    bits, hashes = optimal_parameters(10_000, 0.01)
    assert bits == 95_856 and hashes == 7
    bloom = BloomFilter(bits, hashes)
    members = [f"http://site/{n}" for n in range(10_000)]
    added = bloom.update(members)
    assert 9_900 <= added <= 10_000, "an insertion colliding with earlier ones looks like a duplicate"
    assert all(url in bloom for url in members), "no false negatives"
    strangers = sum(f"http://other/{n}" in bloom for n in range(20_000))
    assert strangers / 20_000 < 0.02
    assert 9_500 < bloom.estimated_count() < 10_500
    assert 0.005 < bloom.false_positive_rate() < 0.02
    assert not bloom.add(members[0]) and bloom.count == added
    with pytest.raises(ValueError):
        optimal_parameters(0, 0.1)
    with pytest.raises(ValueError):
        BloomFilter(12, 3)


def test_union_of_two_filters() -> None:
    left, right = BloomFilter.for_capacity(1000, 0.001), BloomFilter.for_capacity(1000, 0.001)
    left.update(f"a{n}" for n in range(300))
    right.update(f"b{n}" for n in range(300))
    left.union_update(right)
    assert all(f"a{n}" in left and f"b{n}" in left for n in range(300))
    assert 570 <= left.count <= 630
    with pytest.raises(ValueError):
        left.union_update(BloomFilter.for_capacity(10, 0.1))


def test_file_header_layout_and_roundtrip(tmp_path: Path) -> None:
    bloom = BloomFilter.for_capacity(500, 0.01)
    bloom.update(["x", "y"])
    path = tmp_path / "seen.bloom"
    save(path, bloom, capacity=500, error_rate=0.01, database_id=IDENTITY)
    raw = path.read_bytes()
    assert raw[:4] == MAGIC and len(raw) == HEADER_SIZE + bloom.bits // 8
    version, scheme, hashes, bits, count = struct.unpack_from("<HHIQQ", raw, 4)
    assert (version, scheme, hashes, bits, count) == (1, 1, bloom.hashes, bloom.bits, 2)
    header, loaded = load(path)
    assert header.capacity == 500 and header.error_rate == 0.01 and header.database_id == IDENTITY
    assert "x" in loaded and "y" in loaded and loaded.data == bloom.data
    assert not list(tmp_path.glob(".*.tmp")), "atomic save leaves no temporary file"


def damaged(raw: bytes, kind: str) -> bytes:
    data = bytearray(raw)
    if kind == "magic":
        data[0:4] = b"NOPE"
    elif kind == "version":
        data[4] = 9
        data[68:72] = struct.pack("<I", zlib.crc32(bytes(data[:68])))
    elif kind == "header":
        data[20] ^= 0xFF
    elif kind == "payload":
        data[-1] ^= 0x01
    elif kind == "truncated":
        return bytes(data[:-3])
    elif kind == "short":
        return bytes(data[:30])
    return bytes(data)


@pytest.mark.parametrize("kind,message", [
    ("magic", "magic"), ("version", "version"), ("header", "header checksum"), ("payload", "payload checksum"),
    ("truncated", "payload is"), ("short", "too short"),
])
def test_corruption_is_detected(kind: str, message: str) -> None:
    bloom = BloomFilter.for_capacity(100, 0.01)
    bloom.add("z")
    raw = encode(bloom, capacity=100, error_rate=0.01, database_id=IDENTITY)
    assert decode(raw)[1].data == bloom.data
    with pytest.raises(BloomFileError, match=message):
        decode(damaged(raw, kind))
    with pytest.raises(BloomFileError, match="does not exist"):
        load(Path("/nonexistent/seen.bloom"))
    assert read_header(raw).count == 1


def fill_frontier(store: Store, urls: list[str]) -> None:
    Frontier(store.db, "s", FakeClock()).add_many(CrawlRequest(url) for url in urls)


def test_seen_urls_rebuilds_for_every_reason(tmp_path: Path) -> None:
    path = tmp_path / "s.bloom"
    with Store(tmp_path / "a.db") as store:
        fill_frontier(store, [f"http://a/{n}" for n in range(50)])
        seen = SeenUrls(store.db, "s", path, capacity=1000, error_rate=1e-4)
        assert seen.open() == "missing" and seen.might_contain("http://a/7")
        assert not seen.might_contain("http://a/never")
        assert seen.save() and not seen.save(), "nothing new, nothing written"
        assert SeenUrls(store.db, "s", path, capacity=1000).open() == "loaded"
        path.write_bytes(damaged(path.read_bytes(), "payload"))
        assert SeenUrls(store.db, "s", path, capacity=1000).open().startswith("corrupt: payload checksum")
        fill_frontier(store, [f"http://a/more/{n}" for n in range(1000)])
        assert SeenUrls(store.db, "s", path, capacity=1000).open() != "loaded"
        grown = SeenUrls(store.db, "s", path, capacity=1000)
        assert grown.open() in ("grown", "corrupt: payload checksum mismatch")
        assert grown.capacity >= 2 * 1050 and grown.might_contain("http://a/more/999")
        grown.save()
    with Store(tmp_path / "b.db") as other:
        assert database_id(other.db) != ""
        assert SeenUrls(other.db, "s", path).open() == "foreign database", "a filter only describes its database"


def test_two_workers_merge_their_additions_on_save(tmp_path: Path) -> None:
    path = tmp_path / "shared.bloom"
    with Store(tmp_path / "a.db") as store:
        first = SeenUrls(store.db, "s", path, capacity=1000)
        second = SeenUrls(store.db, "s", path, capacity=1000)
        first.open()
        second.open()
        first.add(["http://a/first"])
        second.add(["http://a/second"])
        first.save()
        second.save()
        merged = SeenUrls(store.db, "s", path, capacity=1000)
        assert merged.open() == "loaded"
        assert merged.might_contain("http://a/first") and merged.might_contain("http://a/second")
        assert (path.with_name(path.name + ".lock")).exists()


def test_crawls_use_and_maintain_the_filter(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    first = crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=2)
    assert first.stats["seen_filter"]["loaded_from"] == "missing"
    assert first.stats["dedup"]["links_filtered"] > 0, "links already queued never reach the writer"
    bloom_path = tmp_path / "shop.db.seen" / "shop.bloom"
    header, _ = load(bloom_path)
    with Store(db) as store:
        frontier_size = store.db.execute("SELECT COUNT(*) FROM frontier").fetchone()[0]
    assert header.count >= frontier_size - 2
    second = crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=2, incremental={"mode": "full"})
    assert second.stats["seen_filter"]["loaded_from"] == "loaded"
    assert second.stats["links_enqueued"] == 0
    off = crawl(site, tmp_path / "plain.db", extract={"items": PRODUCT_ITEMS}, max_depth=2,
                project={"dedup": {"bloom": {"enabled": False}}})
    assert "seen_filter" not in off.stats and off.stats["dedup"]["links_filtered"] == 0
    assert off.stats["items"]["new"] == first.stats["items"]["new"]
