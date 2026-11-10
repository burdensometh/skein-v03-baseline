"""The banded SimHash index in SQLite and near-duplicate lookups.

Each stored hash has one row per band in ``simhash_bands``. To find the
pages near a new hash, every page sharing at least one band value is a
candidate; candidates are then checked with the exact Hamming distance.
With four 16-bit bands any hash within distance 3 is guaranteed to be
among the candidates.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from skein.dedup.simhash import MAX_INDEXED_DISTANCE, band_values, hamming, to_signed, to_unsigned


@dataclass(frozen=True)
class NearMatch:
    """The closest earlier page to a new one."""

    url: str
    distance: int


class NearDuplicateIndex:
    """Reads and writes ``simhashes``/``simhash_bands``; callers provide the transaction."""

    def __init__(self, db: sqlite3.Connection, *, max_distance: int = 3) -> None:
        if not 0 <= max_distance <= MAX_INDEXED_DISTANCE:
            raise ValueError(f"max_distance must be between 0 and {MAX_INDEXED_DISTANCE}")
        self.db = db
        self.max_distance = max_distance

    def candidates(self, spider: str, value: int, *, exclude: str | None = None) -> dict[str, int]:
        """Pages sharing a band with ``value``: URL -> stored (unsigned) hash."""
        clauses = " OR ".join("(b.band = ? AND b.value = ?)" for _ in band_values(value))
        params: list[object] = [spider]
        for band, band_value in enumerate(band_values(value)):
            params.extend((band, band_value))
        rows = self.db.execute(
            f"""SELECT DISTINCT s.url, s.simhash FROM simhash_bands b
                JOIN simhashes s ON s.spider = b.spider AND s.url = b.url
                WHERE b.spider = ? AND ({clauses})""",
            params,
        )
        return {row[0]: to_unsigned(row[1]) for row in rows if row[0] != exclude}

    def nearest(self, spider: str, url: str, value: int) -> NearMatch | None:
        """The closest other page within ``max_distance`` (ties: lexically first URL)."""
        best: NearMatch | None = None
        for other, stored in sorted(self.candidates(spider, value, exclude=url).items()):
            distance = hamming(value, stored)
            if distance <= self.max_distance and (best is None or distance < best.distance):
                best = NearMatch(other, distance)
        return best

    def store(self, spider: str, url: str, value: int, now: float) -> None:
        """Insert or replace the hash of ``url`` and its band rows."""
        self.db.execute("DELETE FROM simhash_bands WHERE spider = ? AND url = ?", (spider, url))
        self.db.execute(
            """INSERT INTO simhashes (spider, url, simhash, computed_at) VALUES (?, ?, ?, ?)
               ON CONFLICT (spider, url) DO UPDATE SET simhash = excluded.simhash, computed_at = excluded.computed_at""",
            (spider, url, to_signed(value), now),
        )
        self.db.executemany(
            "INSERT INTO simhash_bands (spider, band, value, url) VALUES (?, ?, ?, ?)",
            [(spider, band, band_value, url) for band, band_value in enumerate(band_values(value))],
        )

    def stored(self, spider: str, url: str) -> int | None:
        row = self.db.execute("SELECT simhash FROM simhashes WHERE spider = ? AND url = ?", (spider, url)).fetchone()
        return to_unsigned(row[0]) if row is not None else None

    def relink(self, spider: str) -> int:
        """Recompute every document's ``near_duplicate_of`` from the stored hashes; returns the duplicates found.

        Documents are visited in the order they were first fetched and each
        is linked to the closest *earlier* page within ``max_distance``, the
        relation a crawl records -- useful after changing ``max_distance``.
        Runs inside the caller's transaction.
        """
        rows = self.db.execute(
            """SELECT d.url, s.simhash FROM documents d JOIN simhashes s ON s.spider = d.spider AND s.url = d.url
               WHERE d.spider = ? ORDER BY d.first_fetched, d.url""",
            (spider,),
        ).fetchall()
        seen: dict[str, int] = {}
        links: list[tuple[str | None, int | None, str, str]] = []
        for url, stored in rows:
            value = to_unsigned(stored)
            best: NearMatch | None = None
            for other in sorted(self.candidates(spider, value, exclude=url)):
                if other not in seen:
                    continue
                distance = hamming(value, seen[other])
                if distance <= self.max_distance and (best is None or distance < best.distance):
                    best = NearMatch(other, distance)
            seen[url] = value
            links.append((best.url if best else None, best.distance if best else None, spider, url))
        self.db.execute("UPDATE documents SET near_duplicate_of = NULL, near_distance = NULL WHERE spider = ?",
                        (spider,))
        self.db.executemany(
            "UPDATE documents SET near_duplicate_of = ?, near_distance = ? WHERE spider = ? AND url = ?", links)
        return sum(1 for link in links if link[0] is not None)

    def clusters(self, spider: str) -> list[list[str]]:
        """Groups of documents linked by recorded near-duplicate relations, largest first.

        Each document points at the earlier page it resembled
        (``documents.near_duplicate_of``); the groups are the connected
        components of those pointers.
        """
        parent: dict[str, str] = {}

        def find(node: str) -> str:
            root = node
            while parent.get(root, root) != root:
                root = parent[root]
            while parent.get(node, node) != root:
                parent[node], node = root, parent[node]
            return root

        rows = self.db.execute(
            "SELECT url, near_duplicate_of FROM documents WHERE spider = ? AND near_duplicate_of IS NOT NULL",
            (spider,),
        )
        for url, original in rows:
            parent.setdefault(url, url)
            parent.setdefault(original, original)
            parent[find(url)] = find(original)
        groups: dict[str, list[str]] = {}
        for node in parent:
            groups.setdefault(find(node), []).append(node)
        return sorted((sorted(members) for members in groups.values()), key=lambda g: (-len(g), g[0]))
