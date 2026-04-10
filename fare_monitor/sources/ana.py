from __future__ import annotations

from urllib.parse import urlencode

from fare_monitor.models import SearchQuery
from fare_monitor.sources.base import HtmlProbeAdapter


class AnaAdapter(HtmlProbeAdapter):
    source_name = "ana"
    carrier_name = "ANA"
    carrier_code = "NH"
    probe_url = "https://www.ana.co.jp/en/us/"

    def build_booking_url(self, query: SearchQuery) -> str:
        params = urlencode(
            {
                "origin": query.origin,
                "destination": query.destination,
                "departureDate": query.depart_date.isoformat(),
                "cabin": query.cabin,
                "adults": query.adults,
            }
        )
        return f"https://www.ana.co.jp/en/us/book-plan/flight-search?{params}"
