from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterable


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
                """
            )

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
