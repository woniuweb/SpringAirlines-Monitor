from fare_monitor.models import FareRecord


def test_fare_record_exports_new_verification_fields() -> None:
    fare = FareRecord(
        collection_id="run-1",
        source="spring_japan",
        carrier="SPRING JAPAN",
        carrier_display_name="SPRING JAPAN",
        source_display_name="SPRING JAPAN 官网",
        source_url="https://jp.ch.com/",
        flight_no="IJ101",
        origin="PEK",
        destination="NRT",
        depart_date="2026-04-10",
        depart_time="08:00",
        arrive_time="12:00",
        stops=0,
        price_original=799.99,
        currency="CNY",
        price_total_cny=799.99,
        tax_included=True,
        booking_url="https://en.ch.com/flights/PEK-NRT.html?FDate=2026-04-10",
        search_url="https://en.ch.com/flights/PEK-NRT.html?FDate=2026-04-10",
        collected_at="2026-04-08T00:00:00+00:00",
        raw_hash="hash",
        fare_scope_note="verified official fare",
        verification_status="verified",
        notes="sample-data",
        is_sample=True,
    )
    payload = fare.as_dict()
    assert fare.is_under_1000 is True
    assert "is_low_fare_under_800" not in payload
    assert payload["verification_status"] == "verified"
    assert payload["search_url"] == "https://en.ch.com/flights/PEK-NRT.html?FDate=2026-04-10"
    assert payload["fare_scope_note"] == "verified official fare"
    assert payload["is_sample"] == 1
