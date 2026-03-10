from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterable


def _norm_doi(value: str | None) -> str | None:
    if not value:
        return None
    v = str(value).strip().lower()
    if v.startswith("https://doi.org/"):
        v = v[len("https://doi.org/") :]
    if v.startswith("http://doi.org/"):
        v = v[len("http://doi.org/") :]
    if v.startswith("doi:"):
        v = v[4:].strip()
    v = v.rstrip(" .,;)")
    return v or None


class Repo:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or os.getenv("DB_PATH", "data/app.db")
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS searches (
                    search_id TEXT PRIMARY KEY,
                    query TEXT NOT NULL,
                    status TEXT NOT NULL,
                    accepted_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    papers_scanned INTEGER NOT NULL DEFAULT 0,
                    relevant_found INTEGER NOT NULL DEFAULT 0,
                    completeness_estimate REAL NOT NULL DEFAULT 0,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS search_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    search_id TEXT NOT NULL,
                    paper_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    score REAL NOT NULL,
                    relevance TEXT NOT NULL,
                    why TEXT NOT NULL,
                    UNIQUE(search_id, paper_id)
                );

                CREATE TABLE IF NOT EXISTS collections (
                    collection_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS collection_papers (
                    collection_id TEXT NOT NULL,
                    paper_id TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(collection_id, paper_id)
                );

                CREATE TABLE IF NOT EXISTS api_papers (
                    paper_id TEXT PRIMARY KEY,
                    doi TEXT NOT NULL UNIQUE,
                    doi_norm TEXT,
                    openalex_id TEXT,
                    citation_count INTEGER,
                    title TEXT NOT NULL,
                    year INTEGER,
                    venue TEXT,
                    abstract TEXT,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS api_references (
                    ref_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    src_paper_id TEXT NOT NULL,
                    ref_order INTEGER NOT NULL,
                    doi TEXT,
                    doi_norm TEXT,
                    ref_openalex_id TEXT,
                    raw_text TEXT NOT NULL,
                    UNIQUE(src_paper_id, ref_order)
                );

                CREATE INDEX IF NOT EXISTS idx_api_refs_src ON api_references(src_paper_id);
                CREATE INDEX IF NOT EXISTS idx_api_refs_doi ON api_references(doi);

                CREATE TABLE IF NOT EXISTS citation_edges (
                    src_paper_id TEXT NOT NULL,
                    dst_paper_id TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    edge_source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(src_paper_id, dst_paper_id)
                );
                CREATE INDEX IF NOT EXISTS idx_edges_src ON citation_edges(src_paper_id);
                CREATE INDEX IF NOT EXISTS idx_edges_dst ON citation_edges(dst_paper_id);
                """
            )
            self._ensure_column(conn, "api_papers", "openalex_id", "TEXT")
            self._ensure_column(conn, "api_papers", "citation_count", "INTEGER")
            self._ensure_column(conn, "api_papers", "doi_norm", "TEXT")
            self._ensure_column(conn, "api_references", "ref_openalex_id", "TEXT")
            self._ensure_column(conn, "api_references", "doi_norm", "TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_api_refs_openalex ON api_references(ref_openalex_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_api_papers_openalex ON api_papers(openalex_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_api_papers_doi_norm ON api_papers(doi_norm)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_api_refs_doi_norm ON api_references(doi_norm)")

            # Backfill normalized DOI columns (idempotent; only missing rows are updated)
            conn.execute("UPDATE api_papers SET doi_norm = lower(trim(doi)) WHERE (doi_norm IS NULL OR trim(doi_norm) = '') AND doi IS NOT NULL AND trim(doi) != ''")
            conn.execute("UPDATE api_references SET doi_norm = lower(trim(doi)) WHERE (doi_norm IS NULL OR trim(doi_norm) = '') AND doi IS NOT NULL AND trim(doi) != ''")

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
        names = {c[1] for c in cols}
        if column not in names:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def insert_search(self, row: dict) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO searches(search_id, query, status, accepted_at, updated_at, papers_scanned, relevant_found, completeness_estimate, error_message)
                VALUES(:search_id, :query, :status, :accepted_at, :updated_at, :papers_scanned, :relevant_found, :completeness_estimate, :error_message)
                """,
                row,
            )

    def update_search(self, search_id: str, **updates) -> None:
        if not updates:
            return
        set_clause = ", ".join([f"{k} = :{k}" for k in updates])
        params = dict(updates)
        params["search_id"] = search_id
        with self._conn() as conn:
            conn.execute(f"UPDATE searches SET {set_clause} WHERE search_id = :search_id", params)

    def get_search(self, search_id: str):
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM searches WHERE search_id = ?", (search_id,))
            return cur.fetchone()

    def insert_results(self, rows: Iterable[dict]) -> None:
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO search_results(search_id, paper_id, title, score, relevance, why)
                VALUES(:search_id, :paper_id, :title, :score, :relevance, :why)
                """,
                list(rows),
            )

    def list_results(self, search_id: str, limit: int = 20, cursor: int = 0):
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT id, search_id, paper_id, title, score, relevance, why
                FROM search_results
                WHERE search_id = ? AND id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (search_id, cursor, limit),
            )
            rows = cur.fetchall()
            next_cursor = rows[-1]["id"] if rows else None
            return rows, next_cursor

    def create_collection(self, row: dict) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO collections(collection_id, name, description, created_at) VALUES(:collection_id, :name, :description, :created_at)",
                row,
            )

    def add_paper_to_collection(self, collection_id: str, paper_id: str, note: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO collection_papers(collection_id, paper_id, note) VALUES(?,?,?)",
                (collection_id, paper_id, note),
            )

    def get_collection(self, collection_id: str):
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM collections WHERE collection_id = ?", (collection_id,))
            return cur.fetchone()

    def upsert_api_paper(self, row: dict) -> None:
        row = dict(row)
        row["doi"] = _norm_doi(row.get("doi")) or row.get("doi")
        row["doi_norm"] = _norm_doi(row.get("doi"))
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO api_papers(paper_id, doi, doi_norm, openalex_id, citation_count, title, year, venue, abstract, source, updated_at)
                VALUES(:paper_id, :doi, :doi_norm, :openalex_id, :citation_count, :title, :year, :venue, :abstract, :source, :updated_at)
                ON CONFLICT(doi) DO UPDATE SET
                  paper_id=excluded.paper_id,
                  doi_norm=excluded.doi_norm,
                  openalex_id=excluded.openalex_id,
                  citation_count=excluded.citation_count,
                  title=excluded.title,
                  year=excluded.year,
                  venue=excluded.venue,
                  abstract=excluded.abstract,
                  source=excluded.source,
                  updated_at=excluded.updated_at
                """,
                row,
            )

    def get_api_paper_by_doi(self, doi: str):
        doi_norm = _norm_doi(doi)
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM api_papers WHERE doi_norm = ?", (doi_norm,))
            return cur.fetchone()

    def find_api_papers_by_title_like(self, title: str, limit: int = 10) -> list[sqlite3.Row]:
        q = (title or "").strip().lower()
        if not q:
            return []
        tokens = [t for t in q.split() if len(t) >= 4][:6]
        if not tokens:
            tokens = [q[:20]]

        where = " AND ".join(["lower(title) LIKE ?" for _ in tokens])
        params = [f"%{t}%" for t in tokens] + [max(1, min(int(limit), 50))]
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM api_papers
                WHERE {where}
                LIMIT ?
                """,
                params,
            ).fetchall()
        return rows

    def get_api_paper_by_id(self, paper_id: str):
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM api_papers WHERE paper_id = ?", (paper_id,))
            return cur.fetchone()

    def get_api_paper_by_openalex_id(self, openalex_id: str):
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM api_papers WHERE openalex_id = ?", (openalex_id,))
            return cur.fetchone()

    def get_api_papers_by_ids(self, paper_ids: list[str]) -> list[dict]:
        if not paper_ids:
            return []
        placeholders = ",".join(["?"] * len(paper_ids))
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT paper_id, doi, title, year, venue, source, citation_count
                FROM api_papers
                WHERE paper_id IN ({placeholders})
                """,
                paper_ids,
            ).fetchall()
        return [dict(r) for r in rows]

    def get_all_paper_ids(self) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute("SELECT paper_id FROM api_papers").fetchall()
        return [str(r["paper_id"]) for r in rows]

    def get_all_edges(self) -> list[tuple[str, str]]:
        with self._conn() as conn:
            rows = conn.execute("SELECT src_paper_id, dst_paper_id FROM citation_edges").fetchall()
        return [(str(r["src_paper_id"]), str(r["dst_paper_id"])) for r in rows]

    def replace_api_references(self, src_paper_id: str, refs: list[dict]) -> None:
        rows = []
        for r in refs:
            x = dict(r)
            x["doi"] = _norm_doi(x.get("doi")) or x.get("doi")
            x["doi_norm"] = _norm_doi(x.get("doi"))
            rows.append(x)
        with self._conn() as conn:
            conn.execute("DELETE FROM api_references WHERE src_paper_id = ?", (src_paper_id,))
            conn.executemany(
                """
                INSERT INTO api_references(src_paper_id, ref_order, doi, doi_norm, ref_openalex_id, raw_text)
                VALUES(:src_paper_id, :ref_order, :doi, :doi_norm, :ref_openalex_id, :raw_text)
                """,
                rows,
            )

    def resolve_edges_doi_match(self, now_iso: str) -> int:
        """Full rebuild. Intended for offline maintenance, not online grow paths."""
        with self._conn() as conn:
            conn.execute("DELETE FROM citation_edges")
            conn.execute(
                """
                INSERT OR REPLACE INTO citation_edges(src_paper_id, dst_paper_id, confidence, edge_source, created_at)
                SELECT r.src_paper_id, p.paper_id, 'high', 'doi_match', ?
                FROM api_references r
                JOIN api_papers p ON r.doi_norm = p.doi_norm
                WHERE r.doi_norm IS NOT NULL AND trim(r.doi_norm) != ''
                """,
                (now_iso,),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO citation_edges(src_paper_id, dst_paper_id, confidence, edge_source, created_at)
                SELECT r.src_paper_id, p.paper_id, 'high', 'openalex_id_match', ?
                FROM api_references r
                JOIN api_papers p ON r.ref_openalex_id = p.openalex_id
                WHERE r.ref_openalex_id IS NOT NULL AND trim(r.ref_openalex_id) != ''
                """,
                (now_iso,),
            )
            cur = conn.execute("SELECT count(*) AS c FROM citation_edges")
            return int(cur.fetchone()["c"])

    def resolve_edges_for_src_paper(self, src_paper_id: str, now_iso: str) -> int:
        with self._conn() as conn:
            conn.execute("DELETE FROM citation_edges WHERE src_paper_id = ?", (src_paper_id,))
            conn.execute(
                """
                INSERT OR REPLACE INTO citation_edges(src_paper_id, dst_paper_id, confidence, edge_source, created_at)
                SELECT r.src_paper_id, p.paper_id, 'high', 'doi_match', ?
                FROM api_references r
                JOIN api_papers p ON r.doi_norm = p.doi_norm
                WHERE r.src_paper_id = ?
                  AND r.doi_norm IS NOT NULL
                  AND trim(r.doi_norm) != ''
                """,
                (now_iso, src_paper_id),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO citation_edges(src_paper_id, dst_paper_id, confidence, edge_source, created_at)
                SELECT r.src_paper_id, p.paper_id, 'high', 'openalex_id_match', ?
                FROM api_references r
                JOIN api_papers p ON r.ref_openalex_id = p.openalex_id
                WHERE r.src_paper_id = ?
                  AND r.ref_openalex_id IS NOT NULL
                  AND trim(r.ref_openalex_id) != ''
                """,
                (now_iso, src_paper_id),
            )
            cur = conn.execute("SELECT count(*) AS c FROM citation_edges WHERE src_paper_id = ?", (src_paper_id,))
            return int(cur.fetchone()["c"])

    def resolve_edges_for_src_papers(self, src_paper_ids: list[str], now_iso: str) -> int:
        ids = [x for x in (src_paper_ids or []) if x]
        if not ids:
            return 0
        placeholders = ",".join(["?"] * len(ids))
        with self._conn() as conn:
            conn.execute(f"DELETE FROM citation_edges WHERE src_paper_id IN ({placeholders})", ids)
            conn.execute(
                f"""
                INSERT OR REPLACE INTO citation_edges(src_paper_id, dst_paper_id, confidence, edge_source, created_at)
                SELECT r.src_paper_id, p.paper_id, 'high', 'doi_match', ?
                FROM api_references r
                JOIN api_papers p ON r.doi_norm = p.doi_norm
                WHERE r.src_paper_id IN ({placeholders})
                  AND r.doi_norm IS NOT NULL
                  AND trim(r.doi_norm) != ''
                """,
                [now_iso] + ids,
            )
            conn.execute(
                f"""
                INSERT OR REPLACE INTO citation_edges(src_paper_id, dst_paper_id, confidence, edge_source, created_at)
                SELECT r.src_paper_id, p.paper_id, 'high', 'openalex_id_match', ?
                FROM api_references r
                JOIN api_papers p ON r.ref_openalex_id = p.openalex_id
                WHERE r.src_paper_id IN ({placeholders})
                  AND r.ref_openalex_id IS NOT NULL
                  AND trim(r.ref_openalex_id) != ''
                """,
                [now_iso] + ids,
            )
            cur = conn.execute(f"SELECT count(*) AS c FROM citation_edges WHERE src_paper_id IN ({placeholders})", ids)
            return int(cur.fetchone()["c"])

    def get_graph_stats(self) -> dict:
        with self._conn() as conn:
            papers = conn.execute("SELECT count(*) AS c FROM api_papers").fetchone()["c"]
            refs = conn.execute("SELECT count(*) AS c FROM api_references").fetchone()["c"]
            refs_with_doi = conn.execute("SELECT count(*) AS c FROM api_references WHERE doi IS NOT NULL AND trim(doi) != ''").fetchone()["c"]
            edges = conn.execute("SELECT count(*) AS c FROM citation_edges").fetchone()["c"]
        return {
            "paper_count": int(papers),
            "reference_count": int(refs),
            "reference_with_doi_count": int(refs_with_doi),
            "edge_count": int(edges),
        }

    def graph_neighbors(self, paper_id: str, direction: str = "both", limit: int = 50) -> dict:
        with self._conn() as conn:
            out_rows = []
            in_rows = []
            if direction in ("out", "both"):
                out_rows = conn.execute(
                    """
                    SELECT e.dst_paper_id AS paper_id, p.doi, p.title, p.year, e.confidence, e.edge_source
                    FROM citation_edges e
                    LEFT JOIN api_papers p ON p.paper_id = e.dst_paper_id
                    WHERE e.src_paper_id = ?
                    LIMIT ?
                    """,
                    (paper_id, limit),
                ).fetchall()
            if direction in ("in", "both"):
                in_rows = conn.execute(
                    """
                    SELECT e.src_paper_id AS paper_id, p.doi, p.title, p.year, e.confidence, e.edge_source
                    FROM citation_edges e
                    LEFT JOIN api_papers p ON p.paper_id = e.src_paper_id
                    WHERE e.dst_paper_id = ?
                    LIMIT ?
                    """,
                    (paper_id, limit),
                ).fetchall()

        return {
            "out": [dict(r) for r in out_rows],
            "in": [dict(r) for r in in_rows],
        }

    def graph_related_coupling(self, paper_id: str, limit: int = 20):
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT e2.src_paper_id AS paper_id, p.doi, p.title, p.year, count(*) AS overlap
                FROM citation_edges e1
                JOIN citation_edges e2 ON e1.dst_paper_id = e2.dst_paper_id
                LEFT JOIN api_papers p ON p.paper_id = e2.src_paper_id
                WHERE e1.src_paper_id = ?
                  AND e2.src_paper_id != ?
                GROUP BY e2.src_paper_id
                ORDER BY overlap DESC, e2.src_paper_id
                LIMIT ?
                """,
                (paper_id, paper_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def graph_related_cocite(self, paper_id: str, limit: int = 20):
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT e2.dst_paper_id AS paper_id, p.doi, p.title, p.year, count(*) AS overlap
                FROM citation_edges e1
                JOIN citation_edges e2 ON e1.src_paper_id = e2.src_paper_id
                LEFT JOIN api_papers p ON p.paper_id = e2.dst_paper_id
                WHERE e1.dst_paper_id = ?
                  AND e2.dst_paper_id != ?
                GROUP BY e2.dst_paper_id
                ORDER BY overlap DESC, e2.dst_paper_id
                LIMIT ?
                """,
                (paper_id, paper_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def graph_related_set_coupling(self, seed_ids: list[str], limit: int = 20):
        if not seed_ids:
            return []
        placeholders = ",".join(["?"] * len(seed_ids))
        params = list(seed_ids) + list(seed_ids) + [limit]
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT e2.src_paper_id AS paper_id, p.doi, p.title, p.year, count(DISTINCT e2.dst_paper_id) AS overlap
                FROM citation_edges e_seed
                JOIN citation_edges e2 ON e_seed.dst_paper_id = e2.dst_paper_id
                LEFT JOIN api_papers p ON p.paper_id = e2.src_paper_id
                WHERE e_seed.src_paper_id IN ({placeholders})
                  AND e2.src_paper_id NOT IN ({placeholders})
                GROUP BY e2.src_paper_id
                ORDER BY overlap DESC, e2.src_paper_id
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    def graph_related_set_cocite(self, seed_ids: list[str], limit: int = 20):
        if not seed_ids:
            return []
        placeholders = ",".join(["?"] * len(seed_ids))
        params = list(seed_ids) + list(seed_ids) + [limit]
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT e2.dst_paper_id AS paper_id, p.doi, p.title, p.year, count(DISTINCT e_seed.src_paper_id) AS overlap
                FROM citation_edges e_seed
                JOIN citation_edges e2 ON e_seed.src_paper_id = e2.src_paper_id
                LEFT JOIN api_papers p ON p.paper_id = e2.dst_paper_id
                WHERE e_seed.dst_paper_id IN ({placeholders})
                  AND e2.dst_paper_id NOT IN ({placeholders})
                GROUP BY e2.dst_paper_id
                ORDER BY overlap DESC, e2.dst_paper_id
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    def get_missing_reference_dois(self, src_paper_ids: list[str], limit: int = 200) -> list[str]:
        if not src_paper_ids:
            return []
        placeholders = ",".join(["?"] * len(src_paper_ids))
        params = list(src_paper_ids) + [max(1, int(limit))]
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT r.doi_norm AS doi
                FROM api_references r
                LEFT JOIN api_papers p ON r.doi_norm = p.doi_norm
                WHERE r.src_paper_id IN ({placeholders})
                  AND r.doi_norm IS NOT NULL
                  AND trim(r.doi_norm) != ''
                  AND p.paper_id IS NULL
                ORDER BY r.doi_norm
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [str(r["doi"]).lower() for r in rows if r["doi"]]
