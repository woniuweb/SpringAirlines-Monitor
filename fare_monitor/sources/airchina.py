from __future__ import annotations

from urllib.parse import urlencode

from fare_monitor.models import SearchQuery
from fare_monitor.sources.base import HtmlProbeAdapter


class AirChinaAdapter(HtmlProbeAdapter):
    source_name = "air_china"
    carrier_name = "Air China"
    carrier_code = "CA"
    probe_url = "https://www.airchina.com.cn/"

    def build_booking_url(self, query: SearchQuery) -> str:
        params = urlencode(
            {
                "origin": query.origin,
                "destination": query.destination,
                "departDate": query.depart_date.isoformat(),
                "cabin": query.cabin,
                "adults": query.adults,
            }
        )
        return f"https://www.airchina.com.cn/flight/?{params}"
