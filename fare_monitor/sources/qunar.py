from __future__ import annotations

import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from fare_monitor.constants import AIRLINE_DISPLAY, CITY_GROUP_BY_AIRPORT, SOURCE_DISPLAY, SOURCE_HOME_URL
from fare_monitor.models import FareRecord, SearchQuery, SourceFetchResult
from fare_monitor.sources.base import SourceAdapter
from fare_monitor.utils import content_hash, utc_now_iso


class QunarAdapter(SourceAdapter):
    source_name = "qunar"
    carrier_name = "Qunar Aggregated"

    def build_booking_url(self, query: SearchQuery) -> str:
        city_group = CITY_GROUP_BY_AIRPORT.get(query.destination, query.destination)
        return f"https://flight.qunar.com/touch/international-routes-{query.origin}-{city_group}.html"

    def _search_live(self, query: SearchQuery, collection_id: str) -> SourceFetchResult:
        booking_url = self.build_booking_url(query)
        try:
            html, final_url = self.fetch_text(booking_url)
        except requests.RequestException as exc:
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="failed",
                message=str(exc),
            )
        fares = self.parse_route_teaser_html(html=html, query=query, collection_id=collection_id, booking_url=final_url)
        payload = self.default_payload(f"qunar-{query.origin}-{query.destination}", html)
        if fares:
            return SourceFetchResult(
                source=self.source_name,
                fares=fares,
                status="ok",
                message="Parsed route teaser fares.",
                payloads=[payload],
            )
        return SourceFetchResult(
            source=self.source_name,
            fares=[],
            status="empty",
            message="Fetched route teaser page but no exact-date fares were present.",
            payloads=[payload],
        )

    def parse_route_teaser_html(
        self,
        *,
        html: str,
        query: SearchQuery,
        collection_id: str,
        booking_url: str,
    ) -> list[FareRecord]:
        soup = BeautifulSoup(html, "lxml")
        fares: list[FareRecord] = []
        for item in soup.select("ul.ul_route_lst li"):
            price_node = item.select_one("span.pr")
            link = item.select_one("a[target='_blank'][href]")
            if not price_node or not link:
                continue
            href = link.get("href", "")
            date_match = re.search(r"goDate=(\d{4}-\d{2}-\d{2})", href)
            if not date_match:
                continue
            go_date = date_match.group(1)
            if go_date != query.depart_date.isoformat():
                continue
            price_match = re.search(r"(\d+(?:\.\d+)?)", price_node.get_text(" ", strip=True))
            if not price_match:
                continue
            fares.append(
                FareRecord(
                    collection_id=collection_id,
                    source=self.source_name,
                    carrier=self.carrier_name,
                    carrier_display_name=AIRLINE_DISPLAY.get(self.source_name, self.carrier_name),
                    source_display_name=SOURCE_DISPLAY.get(self.source_name, self.source_name),
                    source_url=SOURCE_HOME_URL.get(self.source_name, booking_url),
                    flight_no=f"QF-{query.origin}-{query.destination}",
                    origin=query.origin,
                    destination=query.destination,
                    depart_date=go_date,
                    depart_time="00:00",
                    arrive_time="00:00",
                    stops=0,
                    price_original=float(price_match.group(1)),
                    currency="CNY",
                    price_total_cny=float(price_match.group(1)),
                    tax_included=True,
                    booking_url=urljoin("https://m.flight.qunar.com/", href),
                    collected_at=utc_now_iso(),
                    raw_hash=content_hash(str(item)),
                    notes="qunar-route-teaser",
                )
            )
        return fares
