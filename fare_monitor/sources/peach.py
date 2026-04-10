from __future__ import annotations

import requests

from fare_monitor.models import SearchQuery, SourceFetchResult
from fare_monitor.sources.base import SourceAdapter


class PeachAdapter(SourceAdapter):
    source_name = "peach"
    carrier_name = "Peach Aviation"
    live_enabled_by_default = False
    live_skip_reason = "Official Peach live verification is not implemented yet, so this source is skipped in stable mode."
    route_map_url = "https://www.flypeach.com/en/lm/st/routemap"
    route_keys = {"PVG->KIX"}

    def build_booking_url(self, query: SearchQuery) -> str:
        return "https://booking.flypeach.com/en"

    def discover_route_keys(self) -> set[str]:
        return {key for key in self.route_keys if key.split("->", 1)[0] in self.config.origins}

    def _search_live(self, query: SearchQuery, collection_id: str) -> SourceFetchResult:
        try:
            html, final_url = self.fetch_text(self.route_map_url)
        except requests.RequestException as exc:
            return SourceFetchResult(source=self.source_name, fares=[], status="failed", message=str(exc))
        payload = self.default_payload(f"{self.source_name}-route-map", html)
        return SourceFetchResult(
            source=self.source_name,
            fares=[],
            status="empty",
            message=(
                "Official Peach route is tracked, but automated real-time fare verification for this source "
                "is not implemented yet."
            ),
            payloads=[payload, self.default_payload(f"{self.source_name}-source-url", final_url, "txt")],
        )
