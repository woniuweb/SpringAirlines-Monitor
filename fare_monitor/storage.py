from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from fare_monitor.models import FareRecord, SourceRunSummary


class Storage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS fares (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    carrier TEXT NOT NULL,
                    carrier_display_name TEXT NOT NULL DEFAULT '',
                    source_display_name TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',
                    flight_no TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    depart_date TEXT NOT NULL,
                    depart_time TEXT NOT NULL,
                    arrive_time TEXT NOT NULL,
                    stops INTEGER NOT NULL,
                    price_original REAL NOT NULL,
                    currency TEXT NOT NULL,
                    price_total_cny REAL NOT NULL,
                    tax_included INTEGER NOT NULL,
                    booking_url TEXT NOT NULL,
                    search_url TEXT NOT NULL DEFAULT '',
                    collected_at TEXT NOT NULL,
                    raw_hash TEXT NOT NULL,
                    fare_scope_note TEXT NOT NULL DEFAULT '',
                    verification_status TEXT NOT NULL DEFAULT 'verified',
                    notes TEXT NOT NULL DEFAULT '',
                    is_sample INTEGER NOT NULL DEFAULT 0,
                    is_under_1000 INTEGER NOT NULL,
                    is_low_fare_under_800 INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS source_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    queried_routes INTEGER NOT NULL,
                    fare_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL
                );
                """
            )
            fare_columns = {row["name"] for row in conn.execute("PRAGMA table_info(fares)").fetchall()}
            additions = {
                "carrier_display_name": "TEXT NOT NULL DEFAULT ''",
                "source_display_name": "TEXT NOT NULL DEFAULT ''",
                "source_url": "TEXT NOT NULL DEFAULT ''",
                "search_url": "TEXT NOT NULL DEFAULT ''",
                "fare_scope_note": "TEXT NOT NULL DEFAULT ''",
                "verification_status": "TEXT NOT NULL DEFAULT 'verified'",
                "is_sample": "INTEGER NOT NULL DEFAULT 0",
            }
            for column, ddl in additions.items():
                if column not in fare_columns:
                    conn.execute(f"ALTER TABLE fares ADD COLUMN {column} {ddl}")
            if "is_low_fare_under_800" not in fare_columns:
                conn.execute(
                    "ALTER TABLE fares ADD COLUMN is_low_fare_under_800 INTEGER NOT NULL DEFAULT 0"
                )

    def insert_fares(self, fares: list[FareRecord]) -> None:
        if not fares:
            return
        rows = [fare.as_dict() for fare in fares]
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO fares (
                    collection_id, source, carrier, carrier_display_name, source_display_name, source_url,
                    flight_no, origin, destination, depart_date, depart_time, arrive_time, stops,
                    price_original, currency, price_total_cny, tax_included, booking_url, search_url,
                    collected_at, raw_hash, fare_scope_note, verification_status, notes, is_sample,
                    is_under_1000, is_low_fare_under_800
                ) VALUES (
                    :collection_id, :source, :carrier, :carrier_display_name, :source_display_name, :source_url,
                    :flight_no, :origin, :destination, :depart_date, :depart_time, :arrive_time, :stops,
                    :price_original, :currency, :price_total_cny, :tax_included, :booking_url, :search_url,
                    :collected_at, :raw_hash, :fare_scope_note, :verification_status, :notes, :is_sample,
                    :is_under_1000, 0
                )
                """,
                rows,
            )

    def insert_source_runs(self, runs: list[SourceRunSummary]) -> None:
        if not runs:
            return
        rows = [
            {
                "collection_id": run.collection_id,
                "source": run.source,
                "queried_routes": run.queried_routes,
                "fare_count": run.fare_count,
                "status": run.status,
                "message": run.message,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
            }
            for run in runs
        ]
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO source_runs (
                    collection_id, source, queried_routes, fare_count, status,
                    message, started_at, finished_at
                ) VALUES (
                    :collection_id, :source, :queried_routes, :fare_count, :status,
                    :message, :started_at, :finished_at
                )
                """,
                rows,
            )

    def latest_collection_id(self) -> str | None:
        with self.connect() as conn:
            row = conn.execute("SELECT collection_id FROM source_runs ORDER BY id DESC LIMIT 1").fetchone()
            return row["collection_id"] if row else None

    def fares_for_collection(self, collection_id: str) -> list[dict[str, object]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM fares
                WHERE collection_id = ?
                ORDER BY depart_date ASC, depart_time ASC, price_total_cny ASC,
                         carrier_display_name ASC, source_display_name ASC
                """,
                (collection_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def source_runs_for_collection(self, collection_id: str) -> list[dict[str, object]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM source_runs
                WHERE collection_id = ?
                ORDER BY source ASC
                """,
                (collection_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def export_csv(self, path: Path, rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "collection_id",
            "source",
            "carrier",
            "carrier_display_name",
            "source_display_name",
            "source_url",
            "flight_no",
            "origin",
            "destination",
            "depart_date",
            "depart_time",
            "arrive_time",
            "stops",
            "price_original",
            "currency",
            "price_total_cny",
            "tax_included",
            "booking_url",
            "search_url",
            "collected_at",
            "raw_hash",
            "fare_scope_note",
            "verification_status",
            "notes",
            "is_sample",
            "is_under_1000",
        ]
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})
