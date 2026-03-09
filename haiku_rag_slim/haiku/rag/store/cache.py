"""SQLite-based metadata cache for document lookups.

Avoids repeated reads of large DocumentRecords from LanceDB (or cloud storage)
when only lightweight metadata (uri, title, metadata JSON) is needed — e.g. during
search result processing.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS document_meta (
    id          TEXT PRIMARY KEY,
    uri         TEXT,
    title       TEXT,
    metadata    TEXT NOT NULL DEFAULT '{}'
) WITHOUT ROWID;
"""


@dataclass(frozen=True, slots=True)
class DocumentMeta:
    """Lightweight projection of a document — only the fields needed by search."""

    id: str
    uri: str | None
    title: str | None
    metadata: dict


class DocumentMetadataCache:
    """Write-through SQLite cache for document metadata.

    All reads and writes go through the local SQLite file, which is
    orders of magnitude faster than querying cloud storage and uses
    constant memory regardless of dataset size.
    """

    def __init__(self, db_path: Path) -> None:
        db_path.mkdir(parents=True, exist_ok=True)
        cache_file = db_path / ".metadata_cache.db"
        self._conn = sqlite3.connect(str(cache_file), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA cache_size=-65536")  # 64 MB page cache
        self._conn.execute(_SCHEMA)
        self._conn.commit()
        count = self._conn.execute("SELECT COUNT(*) FROM document_meta").fetchone()[0]
        if count:
            logger.info("Metadata cache: %d entries available", count)

    # ── reads ────────────────────────────────────────────────────────────

    def get(self, doc_id: str) -> DocumentMeta | None:
        row = self._conn.execute(
            "SELECT id, uri, title, metadata FROM document_meta WHERE id = ?",
            (doc_id,),
        ).fetchone()
        if row is None:
            return None
        return DocumentMeta(
            id=row[0], uri=row[1], title=row[2], metadata=json.loads(row[3])
        )

    def get_many(self, doc_ids: list[str]) -> dict[str, DocumentMeta]:
        if not doc_ids:
            return {}
        placeholders = ",".join("?" for _ in doc_ids)
        rows = self._conn.execute(
            f"SELECT id, uri, title, metadata FROM document_meta WHERE id IN ({placeholders})",
            doc_ids,
        ).fetchall()
        return {
            row[0]: DocumentMeta(
                id=row[0], uri=row[1], title=row[2], metadata=json.loads(row[3])
            )
            for row in rows
        }

    # ── writes ───────────────────────────────────────────────────────────

    def put(
        self,
        doc_id: str,
        uri: str | None,
        title: str | None,
        metadata: str,
    ) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO document_meta (id, uri, title, metadata) VALUES (?, ?, ?, ?)",
            (doc_id, uri, title, metadata),
        )
        self._conn.commit()

    def put_many(
        self,
        rows: list[tuple[str, str | None, str | None, str]],
    ) -> None:
        """Batch insert/update. Each tuple is (id, uri, title, metadata_json)."""
        if not rows:
            return
        self._conn.executemany(
            "INSERT OR REPLACE INTO document_meta (id, uri, title, metadata) VALUES (?, ?, ?, ?)",
            rows,
        )
        self._conn.commit()

    def remove(self, doc_id: str) -> None:
        self._conn.execute("DELETE FROM document_meta WHERE id = ?", (doc_id,))
        self._conn.commit()

    def clear(self) -> None:
        self._conn.execute("DELETE FROM document_meta")
        self._conn.commit()

    # ── filtering ────────────────────────────────────────────────────────

    def filter_ids(self, where_clause: str) -> list[str] | None:
        """Resolve a SQL WHERE clause against cached metadata.

        Returns matching document IDs, or None if the clause references
        columns not present in the cache (caller should fall back to the
        source table).
        """
        try:
            rows = self._conn.execute(
                f"SELECT id FROM document_meta WHERE {where_clause}"
            ).fetchall()
            return [row[0] for row in rows]
        except sqlite3.OperationalError:
            return None

    def is_empty(self) -> bool:
        row = self._conn.execute("SELECT 1 FROM document_meta LIMIT 1").fetchone()
        return row is None

    def close(self) -> None:
        self._conn.close()
