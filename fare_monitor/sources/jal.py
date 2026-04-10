from __future__ import annotations

from urllib.parse import urlencode

from fare_monitor.models import SearchQuery
from fare_monitor.sources.base import HtmlProbeAdapter


class JalAdapter(HtmlProbeAdapter):
    source_name = "jal"
    carrier_name = "JAL"
    carrier_code = "JL"
    probe_url = "https://www.jal.co.jp/"

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
        return f"https://www.jal.co.jp/jp/en/inter/booking/?{params}"
