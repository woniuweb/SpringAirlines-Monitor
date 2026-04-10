from __future__ import annotations

import requests

from fare_monitor.models import SearchQuery, SourceFetchResult
from fare_monitor.sources.base import SourceAdapter


class JetstarJapanAdapter(SourceAdapter):
    source_name = "jetstar_japan"
    carrier_name = "Jetstar Japan"
    live_enabled_by_default = False
    live_skip_reason = "Official Jetstar Japan live verification is not implemented yet, so this source is skipped in stable mode."
    discovery_url = "https://www.jetstar.com/jp/en/flights/shanghai"
    route_keys = {"PVG->NRT", "PVG->KIX"}

    def build_booking_url(self, query: SearchQuery) -> str:
        return self.discovery_url

    def discover_route_keys(self) -> set[str]:
        return {key for key in self.route_keys if key.split("->", 1)[0] in self.config.origins}

    def _search_live(self, query: SearchQuery, collection_id: str) -> SourceFetchResult:
        try:
            html, final_url = self.fetch_text(self.discovery_url)
        except requests.RequestException as exc:
            return SourceFetchResult(source=self.source_name, fares=[], status="failed", message=str(exc))
        payload = self.default_payload(f"{self.source_name}-discovery-page", html)
        return SourceFetchResult(
            source=self.source_name,
            fares=[],
            status="empty",
            message=(
                "Official Jetstar Japan route is tracked, but automated real-time fare verification for this "
                "source is not implemented yet."
            ),
            payloads=[payload, self.default_payload(f"{self.source_name}-source-url", final_url, "txt")],
        )
