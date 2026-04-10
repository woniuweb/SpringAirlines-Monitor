import sqlite3

from fare_monitor.models import FareRecord, SourceRunSummary
from fare_monitor.storage import Storage


def test_storage_initializes_compatible_columns_on_old_db(tmp_path) -> None:
    db_path = tmp_path / "fares.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE fares (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id TEXT NOT NULL,
            source TEXT NOT NULL,
            carrier TEXT NOT NULL,
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
            collected_at TEXT NOT NULL,
            raw_hash TEXT NOT NULL,
            notes TEXT NOT NULL DEFAULT '',
            is_under_1000 INTEGER NOT NULL
        );
        CREATE TABLE source_runs (
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
    conn.close()

    storage = Storage(db_path)
    storage.initialize()

    with storage.connect() as conn2:
        columns = {row["name"] for row in conn2.execute("PRAGMA table_info(fares)").fetchall()}
    assert "carrier_display_name" in columns
    assert "source_display_name" in columns
    assert "source_url" in columns
    assert "search_url" in columns
    assert "fare_scope_note" in columns
    assert "verification_status" in columns
    assert "is_sample" in columns


def test_storage_round_trip_with_verified_fields(tmp_path) -> None:
    db_path = tmp_path / "fares.db"
    storage = Storage(db_path)
    storage.initialize()
    fare = FareRecord(
        collection_id="run-1",
        source="spring_airlines",
        carrier="Spring Airlines",
        carrier_display_name="春秋航空 Spring Airlines",
        source_display_name="Spring Airlines 官网",
        source_url="https://en.ch.com/flights/Japan.html",
        flight_no="9C123",
        origin="SJW",
        destination="KIX",
        depart_date="2026-04-10",
        depart_time="08:00",
        arrive_time="12:00",
        stops=0,
        price_original=960.0,
        currency="CNY",
        price_total_cny=960.0,
        tax_included=True,
        booking_url="https://en.ch.com/flights/SJW-KIX.html?FDate=2026-04-10",
        search_url="https://en.ch.com/flights/SJW-KIX.html?FDate=2026-04-10",
        collected_at="2026-04-08T00:00:00+00:00",
        raw_hash="hash",
        fare_scope_note="verified official fare",
        verification_status="verified",
    )
    run = SourceRunSummary(
        collection_id="run-1",
        source="spring_airlines",
        queried_routes=5,
        fare_count=1,
        status="ok",
        message="ok",
        started_at="2026-04-08T00:00:00+00:00",
        finished_at="2026-04-08T00:01:00+00:00",
    )
    storage.insert_fares([fare])
    storage.insert_source_runs([run])
    rows = storage.fares_for_collection("run-1")
    source_rows = storage.source_runs_for_collection("run-1")
    assert rows[0]["flight_no"] == "9C123"
    assert rows[0]["verification_status"] == "verified"
    assert rows[0]["search_url"] == "https://en.ch.com/flights/SJW-KIX.html?FDate=2026-04-10"
    assert source_rows[0]["source"] == "spring_airlines"
