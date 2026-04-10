from __future__ import annotations

from datetime import date

from fare_monitor.constants import AIRLINE_DISPLAY, SAMPLE_SOURCE_ROUTE_KEYS, SOURCE_DISPLAY, SOURCE_HOME_URL
from fare_monitor.models import FareRecord, RawPayload, SearchQuery, SourceFetchResult
from fare_monitor.utils import content_hash, utc_now_iso

SAMPLE_PRICE_BIAS = {
    "spring_airlines": 10,
    "spring_japan": -40,
    "peach": -20,
    "jetstar_japan": -5,
}

SAMPLE_CARRIER_CODE = {
    "spring_airlines": "9C",
    "spring_japan": "IJ",
    "peach": "MM",
    "jetstar_japan": "GK",
}

SAMPLE_CARRIER_NAME = {
    "spring_airlines": "Spring Airlines",
    "spring_japan": "SPRING JAPAN",
    "peach": "Peach Aviation",
    "jetstar_japan": "Jetstar Japan",
}

SAMPLE_DEST_BIAS = {
    "NRT": -80,
    "HND": 40,
    "KIX": -20,
    "ITM": 35,
    "NGO": 20,
    "FUK": -10,
    "CTS": 90,
    "OKA": 140,
}


def sample_route_keys(source: str) -> set[str]:
    return set(SAMPLE_SOURCE_ROUTE_KEYS.get(source, ()))


def build_sample_result(
    source: str,
    query: SearchQuery,
    collection_id: str,
    booking_url: str,
    qualified_threshold: float,
) -> SourceFetchResult:
    day_offset = (query.depart_date - date.today()).days
    if day_offset < 0:
        return SourceFetchResult(source=source, status="empty", message="Past dates are not sampled.")
    if query.route_key not in sample_route_keys(source):
        return SourceFetchResult(source=source, status="skipped", message="")
    if (day_offset + len(query.destination) + len(source)) % 9 == 0:
        return SourceFetchResult(source=source, status="empty", message="No sampled fare for this query.")

    route_bias = SAMPLE_DEST_BIAS.get(query.destination, 0)
    source_bias = SAMPLE_PRICE_BIAS[source]
    base = 690 + source_bias + route_bias + (day_offset % 11) * 23
    depart_hour = 6 + (day_offset % 6) * 2
    arrive_hour = (depart_hour + 4 + (day_offset % 2)) % 24
    flight_code = SAMPLE_CARRIER_CODE[source]
    fare = FareRecord(
        collection_id=collection_id,
        source=source,
        carrier=SAMPLE_CARRIER_NAME[source],
        carrier_display_name=AIRLINE_DISPLAY[source],
        source_display_name=SOURCE_DISPLAY[source],
        source_url=SOURCE_HOME_URL[source],
        flight_no=f"{flight_code}{100 + (day_offset % 180)}",
        origin=query.origin,
        destination=query.destination,
        depart_date=query.depart_date.isoformat(),
        depart_time=f"{depart_hour:02d}:20",
        arrive_time=f"{arrive_hour:02d}:45",
        stops=0,
        price_original=float(base),
        currency="CNY",
        price_total_cny=float(base),
        tax_included=True,
        booking_url=booking_url,
        search_url=booking_url,
        collected_at=utc_now_iso(),
        raw_hash=content_hash(f"{source}|{query.route_key}|{query.depart_date.isoformat()}|{base}"),
        fare_scope_note="样例数据：模拟已验证的官方可售价格。",
        verification_status="verified",
        notes="sample-data",
        is_sample=True,
        is_under_1000=float(base) < qualified_threshold,
    )
    payload = RawPayload(
        name=f"{source}-{query.origin}-{query.destination}-{query.depart_date.isoformat()}",
        extension="json",
        content=(
            '{"sample": true, "source": "%s", "origin": "%s", "destination": "%s", '
            '"depart_date": "%s", "price_total_cny": %.2f, "verification_status": "verified"}'
        )
        % (source, query.origin, query.destination, query.depart_date.isoformat(), fare.price_total_cny),
    )
    return SourceFetchResult(source=source, fares=[fare], status="ok", payloads=[payload])
