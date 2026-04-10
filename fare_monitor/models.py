from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from fare_monitor.constants import DEFAULT_QUALIFIED_THRESHOLD


@dataclass(frozen=True)
class SearchQuery:
    origin: str
    destination: str
    depart_date: date
    adults: int = 1
    cabin: str = "economy"

    @property
    def route_key(self) -> str:
        return f"{self.origin}->{self.destination}"


@dataclass
class RawPayload:
    name: str
    content: str
    extension: str = "txt"


@dataclass
class FareRecord:
    collection_id: str
    source: str
    carrier: str
    carrier_display_name: str
    source_display_name: str
    source_url: str
    flight_no: str
    origin: str
    destination: str
    depart_date: str
    depart_time: str
    arrive_time: str
    stops: int
    price_original: float
    currency: str
    price_total_cny: float
    tax_included: bool
    booking_url: str
    collected_at: str
    raw_hash: str
    search_url: str = ""
    fare_scope_note: str = ""
    verification_status: str = "verified"
    notes: str = ""
    is_sample: bool = False
    is_under_1000: bool | None = None

    def __post_init__(self) -> None:
        if self.is_under_1000 is None:
            self.is_under_1000 = self.price_total_cny < DEFAULT_QUALIFIED_THRESHOLD

    def as_dict(self) -> dict[str, object]:
        return {
            "collection_id": self.collection_id,
            "source": self.source,
            "carrier": self.carrier,
            "carrier_display_name": self.carrier_display_name,
            "source_display_name": self.source_display_name,
            "source_url": self.source_url,
            "flight_no": self.flight_no,
            "origin": self.origin,
            "destination": self.destination,
            "depart_date": self.depart_date,
            "depart_time": self.depart_time,
            "arrive_time": self.arrive_time,
            "stops": self.stops,
            "price_original": self.price_original,
            "currency": self.currency,
            "price_total_cny": self.price_total_cny,
            "tax_included": int(self.tax_included),
            "booking_url": self.booking_url,
            "search_url": self.search_url,
            "collected_at": self.collected_at,
            "raw_hash": self.raw_hash,
            "fare_scope_note": self.fare_scope_note,
            "verification_status": self.verification_status,
            "notes": self.notes,
            "is_sample": int(self.is_sample),
            "is_under_1000": int(self.is_under_1000),
        }


@dataclass
class SourceFetchResult:
    source: str
    fares: list[FareRecord] = field(default_factory=list)
    status: str = "ok"
    message: str = ""
    payloads: list[RawPayload] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)


@dataclass
class SourceRunSummary:
    collection_id: str
    source: str
    queried_routes: int
    fare_count: int
    status: str
    message: str
    started_at: str
    finished_at: str
