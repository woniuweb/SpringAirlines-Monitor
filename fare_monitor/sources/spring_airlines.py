from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import date, timedelta
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from fare_monitor.browser_agent import AgentBrowserClient, BrowserSpringPage, BrowserUnavailableError
from fare_monitor.constants import (
    AIRLINE_DISPLAY,
    CITY_GROUP_BY_AIRPORT,
    SOURCE_DISPLAY,
    SOURCE_HOME_URL,
)
from fare_monitor.models import FareRecord, RawPayload, SearchQuery, SourceFetchResult
from fare_monitor.sources.base import SourceAdapter
from fare_monitor.utils import content_hash, utc_now_iso

ORIGIN_CITY_GROUP_BY_AIRPORT = {
    "PEK": "BJS",
    "PKX": "BJS",
    "SHA": "SHA",
    "PVG": "SHA",
}

CARRIER_BY_FLIGHT_PREFIX = {
    "9C": "Spring Airlines",
    "IJ": "SPRING JAPAN",
    "MM": "Peach Aviation",
    "GK": "Jetstar Japan",
}

FX_TO_CNY = {
    "CNY": 1.0,
    "USD": 6.1237,
    "JPY": 0.0513,
    "HKD": 0.7896,
    "TWD": 0.1950,
    "SGD": 4.6187,
    "THB": 0.1886,
    "KRW": 0.0056,
    "MOP": 0.7783,
}

VERIFIED_LIVE_ROUTE_KEYS = {
    "PEK->NRT",
    "TSN->NRT",
    "PVG->NRT",
    "PVG->HND",
    "PVG->KIX",
    "PVG->NGO",
    "PVG->FUK",
}


@dataclass(frozen=True)
class SpringSearchContext:
    search_origin: str
    search_destination: str
    depart_airport: str
    arrive_airport: str
    booking_url: str

    @property
    def uses_origin_filter(self) -> bool:
        return self.search_origin != self.depart_airport

    @property
    def uses_destination_filter(self) -> bool:
        return self.search_destination != self.arrive_airport


@dataclass(frozen=True)
class SpringLiveProbeResult:
    query: SearchQuery
    booking_url: str
    final_url: str
    page_title: str
    status: str
    matched_fare_count: int
    rendered_flight_count: int
    matched_flights: tuple[str, ...] = ()
    rendered_routes: tuple[str, ...] = ()
    currencies: tuple[str, ...] = ()
    message: str = ""


class SpringAirlinesAdapter(SourceAdapter):
    source_name = "spring_airlines"
    carrier_name = "Spring Airlines"
    discovery_urls = (
        "https://en.ch.com/flights/Japan.html",
        "https://en.ch.com/flights/China-Japan.html",
        "https://en.ch.com/sitemap/flights-city-to-city.html",
    )
    search_endpoint = "https://en.ch.com/Flights/SearchByTime"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.browser = AgentBrowserClient(
            self.config.base_dir,
            headless=self.config.browser_headless,
            executable_path=self.config.browser_executable_path,
            channel=self.config.browser_channel,
        )
        self._blocked_message = ""
        self._last_request_at = 0.0
        self._request_interval_seconds = 1.25

    def build_booking_url(self, query: SearchQuery) -> str:
        return self.build_search_context(query).booking_url

    def filter_queries(self, queries: list[SearchQuery]) -> list[SearchQuery]:
        return super().filter_queries(queries)

    def build_search_context(self, query: SearchQuery) -> SpringSearchContext:
        search_origin = ORIGIN_CITY_GROUP_BY_AIRPORT.get(query.origin, query.origin)
        search_destination = CITY_GROUP_BY_AIRPORT.get(query.destination, query.destination)
        params = {
            "FDate": query.depart_date.isoformat(),
            "ANum": query.adults,
            "CNum": 0,
            "INum": 0,
            "SType": 0,
            "IfRet": "false",
            "MType": 0,
        }
        if search_origin != query.origin:
            params["DepAirportCode"] = query.origin
            params["IsSearchDepAirport"] = "true"
        if search_destination != query.destination:
            params["ArrAirportCode"] = query.destination
            params["IsSearchArrAirport"] = "true"
        return SpringSearchContext(
            search_origin=search_origin,
            search_destination=search_destination,
            depart_airport=query.origin,
            arrive_airport=query.destination,
            booking_url=(
                f"https://en.ch.com/flights/{search_origin}-{search_destination}.html?{urlencode(params)}"
            ),
        )

    def supported_route_keys(self) -> set[str]:
        if self.sample_mode:
            return super().supported_route_keys()
        if self._supported_route_keys is None:
            self._supported_route_keys = {
                route_key
                for route_key in VERIFIED_LIVE_ROUTE_KEYS
                if route_key.split("->", 1)[0] in self.config.origins
                and route_key.split("->", 1)[1] in self.config.destinations
            }
        return self._supported_route_keys

    def discover_route_keys(self) -> set[str]:
        route_keys: set[str] = set()
        for url in self.discovery_urls:
            try:
                html, _ = self.fetch_text(url)
            except requests.RequestException:
                continue
            soup = BeautifulSoup(html, "lxml")
            for node in soup.find_all("a", href=True):
                href = node["href"]
                match = re.search(r"/([A-Z]{3})-([A-Z]{3})/?(?:[?#]|$)", href)
                if not match:
                    continue
                origin, destination = match.group(1), match.group(2)
                for config_origin in self.config.origins:
                    for config_destination in self.config.destinations:
                        if (
                            ORIGIN_CITY_GROUP_BY_AIRPORT.get(config_origin, config_origin) == origin
                            and CITY_GROUP_BY_AIRPORT.get(config_destination, config_destination) == destination
                        ):
                            route_keys.add(f"{config_origin}->{config_destination}")
        return route_keys

    def _search_live(self, query: SearchQuery, collection_id: str) -> SourceFetchResult:
        if self._blocked_message:
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="skipped",
                message=self._blocked_message,
            )
        context = self.build_search_context(query)
        self._wait_for_rate_limit()
        if self.config.spring_window_days > 1:
            try:
                window_page = self.browser.extract_spring_window(
                    context.booking_url,
                    click_threshold_cny=self.config.spring_date_click_threshold_cny,
                    fx_to_cny=FX_TO_CNY,
                )
                window_fares = self.parse_window_browser_fares(
                    page=window_page,
                    query=query,
                    context=context,
                    collection_id=collection_id,
                )
                if window_page.preview_days:
                    payloads = [
                        RawPayload(
                            name=f"{self.source_name}-{query.origin}-{query.destination}-{query.depart_date.isoformat()}-window-browser",
                            content=window_page.raw_payload,
                            extension="json",
                        ),
                        self.default_payload(f"{self.source_name}-search-url", window_page.url or context.booking_url),
                    ]
                    clicked_dates = len(window_page.day_results)
                    preview_days = len(window_page.preview_days)
                    stats = {
                        "windows": 1,
                        "preview_days": preview_days,
                        "clicked_dates": clicked_dates,
                        "written_fares": len(window_fares),
                    }
                    if window_fares:
                        return SourceFetchResult(
                            source=self.source_name,
                            fares=window_fares,
                            status="ok",
                            payloads=payloads,
                            message=(
                                f"window days={preview_days} clicked_dates={clicked_dates} "
                                f"written_fares={len(window_fares)}"
                            ),
                            stats=stats,
                        )
                    return SourceFetchResult(
                        source=self.source_name,
                        fares=[],
                        status="empty",
                        payloads=payloads,
                        message=(
                            f"window days={preview_days} clicked_dates={clicked_dates} "
                            "but no exact-airport low fare matched under the current window."
                        ),
                        stats=stats,
                    )
            except BrowserUnavailableError:
                pass
        try:
            page = self.browser.extract_spring_page(context.booking_url)
        except BrowserUnavailableError as exc:
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="failed",
                message=f"Browser extraction failed: {exc}",
            )
        payloads = [
            RawPayload(
                name=f"{self.source_name}-{query.origin}-{query.destination}-{query.depart_date.isoformat()}-browser",
                content=page.raw_payload,
                extension="json",
            ),
            self.default_payload(f"{self.source_name}-search-url", page.url or context.booking_url),
        ]
        fares = self.parse_browser_fares(page=page, query=query, context=context, collection_id=collection_id)
        if fares:
            return SourceFetchResult(
                source=self.source_name,
                fares=fares,
                status="ok",
                payloads=payloads,
                stats={"windows": 1, "preview_days": 0, "clicked_dates": 1, "written_fares": len(fares)},
            )
        if page.title.strip() == "405":
            self._blocked_message = self._format_blocked_message(RuntimeError("browser page returned 405"))
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="failed",
                message=self._blocked_message,
                payloads=payloads,
                stats={"windows": 1, "preview_days": 0, "clicked_dates": 0, "written_fares": 0},
            )
        return SourceFetchResult(
            source=self.source_name,
            fares=[],
            status="empty",
            message="Browser reached the official page, but no sellable fare row was rendered for the exact airport pair.",
            payloads=payloads,
            stats={"windows": 1, "preview_days": 0, "clicked_dates": 0, "written_fares": 0},
        )

    def scan_route_live(
        self,
        origin: str,
        destination: str,
        start_date: date,
        end_date: date,
        collection_id: str,
    ) -> SourceFetchResult:
        if self._blocked_message:
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="skipped",
                message=self._blocked_message,
            )
        seed_query = SearchQuery(origin=origin, destination=destination, depart_date=start_date)
        context = self.build_search_context(seed_query)
        self._wait_for_rate_limit()
        try:
            route_page = self.browser.scan_spring_route(
                url=context.booking_url,
                route_key=seed_query.route_key,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                click_threshold_cny=self.config.spring_date_click_threshold_cny,
                window_days=self.config.spring_window_days,
                max_empty_weeks=self.config.spring_max_consecutive_empty_weeks,
                fx_to_cny=FX_TO_CNY,
            )
        except BrowserUnavailableError as exc:
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="failed",
                message=f"Route session browser extraction failed: {exc}",
            )

        fares = self.parse_route_browser_fares(
            page=route_page,
            origin=origin,
            destination=destination,
            collection_id=collection_id,
        )
        payloads = [
            RawPayload(
                name=f"{self.source_name}-{origin}-{destination}-{start_date.isoformat()}-{end_date.isoformat()}-route-browser",
                content=route_page.raw_payload,
                extension="json",
            ),
            self.default_payload(
                f"{self.source_name}-route-search-url",
                route_page.final_url or route_page.url or context.booking_url,
            ),
        ]
        stats = {
            "routes": 1,
            "weeks_scanned": route_page.weeks_scanned,
            "preview_days": len(route_page.preview_days),
            "clicked_dates": len(route_page.day_results),
            "written_fares": len(fares),
            "blocked_routes": 1 if route_page.blocked else 0,
            "blocked_after_progress": 1 if route_page.blocked_after_progress else 0,
            "mismatch_routes": 1 if route_page.route_mismatch else 0,
            "empty_weeks": route_page.empty_weeks,
            "consecutive_empty_weeks": route_page.consecutive_empty_weeks,
        }

        if route_page.blocked:
            self._blocked_message = self._format_blocked_message(RuntimeError("route session hit a block page"))
            status = "partial" if fares else "failed"
            message = " | ".join(route_page.fail_samples[:3]) or self._blocked_message
            return SourceFetchResult(
                source=self.source_name,
                fares=fares,
                status=status,
                message=message,
                payloads=payloads,
                stats=stats,
            )
        if route_page.route_mismatch:
            status = "partial" if fares else "failed"
            message = " | ".join(route_page.fail_samples[:3]) or "Route session drifted to a different city pair."
            return SourceFetchResult(
                source=self.source_name,
                fares=fares,
                status=status,
                message=message,
                payloads=payloads,
                stats=stats,
            )
        if fares:
            extra_bits = []
            if route_page.empty_weeks:
                extra_bits.append(f"empty_weeks={route_page.empty_weeks}")
            return SourceFetchResult(
                source=self.source_name,
                fares=fares,
                status="ok",
                message=(
                    f"route session weeks={route_page.weeks_scanned} "
                    f"preview_days={len(route_page.preview_days)} clicked_dates={len(route_page.day_results)} "
                    f"written_fares={len(fares)}"
                    + (f" {' '.join(extra_bits)}" if extra_bits else "")
                ),
                payloads=payloads,
                stats=stats,
            )
        return SourceFetchResult(
            source=self.source_name,
            fares=[],
            status="empty",
            message=(
                "Route session reached the official page, but no exact-airport sellable low fare was found."
                + (f" empty_weeks={route_page.empty_weeks}" if route_page.empty_weeks else "")
            ),
            payloads=payloads,
            stats=stats,
        )

    def probe_live_query(self, query: SearchQuery) -> SpringLiveProbeResult:
        context = self.build_search_context(query)
        self._wait_for_rate_limit()
        try:
            page = self.browser.extract_spring_page(context.booking_url)
        except BrowserUnavailableError as exc:
            return SpringLiveProbeResult(
                query=query,
                booking_url=context.booking_url,
                final_url=context.booking_url,
                page_title="",
                status="failed",
                matched_fare_count=0,
                rendered_flight_count=0,
                message=f"Browser extraction failed: {exc}",
            )

        matched_flights: list[str] = []
        currencies: list[str] = []
        for item in page.flights:
            flight_no = item.get("flight_no", "").replace("(Share)", "").strip()
            price_info = self.parse_browser_price(item.get("price_text", ""))
            if not flight_no or price_info is None:
                continue
            if not self.browser_row_matches_query(item, query):
                continue
            matched_flights.append(flight_no)
            currencies.append(str(price_info["currency"]))

        rendered_routes = tuple(
            dict.fromkeys(
                f"{item.get('depart_airport', '').strip()} -> {item.get('arrive_airport', '').strip()}"
                for item in page.flights
                if item.get("depart_airport", "").strip() and item.get("arrive_airport", "").strip()
            )
        )
        final_url = page.url or context.booking_url
        page_title = page.title.strip()
        if matched_flights:
            return SpringLiveProbeResult(
                query=query,
                booking_url=context.booking_url,
                final_url=final_url,
                page_title=page_title,
                status="verified",
                matched_fare_count=len(matched_flights),
                rendered_flight_count=len(page.flights),
                matched_flights=tuple(matched_flights),
                rendered_routes=rendered_routes,
                currencies=tuple(dict.fromkeys(currencies)),
                message="Official Spring booking page rendered exact-airport sellable rows.",
            )
        if page_title == "405":
            self._blocked_message = self._format_blocked_message(RuntimeError("browser page returned 405"))
            return SpringLiveProbeResult(
                query=query,
                booking_url=context.booking_url,
                final_url=final_url,
                page_title=page_title,
                status="blocked",
                matched_fare_count=0,
                rendered_flight_count=len(page.flights),
                rendered_routes=rendered_routes,
                message=self._blocked_message,
            )
        if page.flights:
            sample_routes = ", ".join(rendered_routes[:3]) or "no exact airport rows"
            return SpringLiveProbeResult(
                query=query,
                booking_url=context.booking_url,
                final_url=final_url,
                page_title=page_title,
                status="empty",
                matched_fare_count=0,
                rendered_flight_count=len(page.flights),
                rendered_routes=rendered_routes,
                message=f"Rendered rows exist, but none matched the exact airport pair. Sample rows: {sample_routes}",
            )
        return SpringLiveProbeResult(
            query=query,
            booking_url=context.booking_url,
            final_url=final_url,
            page_title=page_title,
            status="empty",
            matched_fare_count=0,
            rendered_flight_count=0,
            rendered_routes=rendered_routes,
            message="Official page loaded, but no sellable fare rows were rendered.",
        )

    def _search_live_via_requests(self, query: SearchQuery, collection_id: str) -> SourceFetchResult:
        context = self.build_search_context(query)
        self._wait_for_rate_limit()
        try:
            html, final_url = self.fetch_text(context.booking_url)
        except requests.RequestException as exc:
            if self._is_blocked_error(exc):
                self._blocked_message = self._format_blocked_message(exc)
            return SourceFetchResult(source=self.source_name, fares=[], status="failed", message=str(exc))

        page_payload = self.default_payload(f"{self.source_name}-{query.origin}-{query.destination}", html)
        hidden = self.extract_hidden_inputs(html)
        if (
            hidden.get("oriCode") != context.search_origin
            or hidden.get("desCode") != context.search_destination
            or hidden.get("departureDate") != query.depart_date.isoformat()
        ):
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="empty",
                message="Official Spring search page did not expose the expected route context.",
                payloads=[page_payload],
            )

        try:
            response = self.session.post(
                self.search_endpoint,
                data=self.build_search_payload(hidden, context, query),
                timeout=self.config.request_timeout,
                headers={
                    "Origin": "https://en.ch.com",
                    "Referer": final_url,
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            if self._is_blocked_error(exc):
                self._blocked_message = self._format_blocked_message(exc)
            return SourceFetchResult(
                source=self.source_name,
                fares=[],
                status="failed",
                message=f"Official search API failed: {exc}",
                payloads=[page_payload, self.default_payload(f"{self.source_name}-search-url", final_url)],
            )

        payloads = [
            page_payload,
            self.default_payload(f"{self.source_name}-search-url", final_url),
            RawPayload(
                name=f"{self.source_name}-{query.origin}-{query.destination}-{query.depart_date.isoformat()}-search",
                content=json.dumps(data, ensure_ascii=False),
                extension="json",
            ),
        ]

        fares = self.parse_fares(data=data, query=query, context=context, collection_id=collection_id)
        if fares:
            return SourceFetchResult(source=self.source_name, fares=fares, status="ok", payloads=payloads)

        code = data.get("Code")
        message = data.get("ErrorMessage") or "Official search returned no sellable fare for the exact airport pair."
        if code not in {0, "0", None}:
            message = f"Official search returned code={code}: {message}"
        return SourceFetchResult(source=self.source_name, fares=[], status="empty", message=message, payloads=payloads)

    def _wait_for_rate_limit(self) -> None:
        now = time.monotonic()
        wait_seconds = self._request_interval_seconds - (now - self._last_request_at)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_request_at = time.monotonic()

    def _is_blocked_error(self, exc: Exception) -> bool:
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            return exc.response.status_code == 405
        text = str(exc)
        return "405" in text and "Not Allowed" in text

    def _format_blocked_message(self, exc: Exception) -> str:
        return (
            "Official Spring source is temporarily blocking automated requests "
            f"({exc}). Remaining queries for this source were skipped to avoid hammering the site."
        )

    def build_search_payload(
        self,
        hidden: dict[str, str],
        context: SpringSearchContext,
        query: SearchQuery,
    ) -> dict[str, str | int]:
        return {
            "Active9s": hidden.get("isActive9s", "false"),
            "IsJC": hidden.get("IsJC", ""),
            "IsShowTaxprice": "true",
            "Currency": 0,
            "SType": hidden.get("sType", "0") or "0",
            "Departure": hidden.get("departure", context.search_origin),
            "Arrival": hidden.get("arrival", context.search_destination),
            "DepartureDate": query.depart_date.isoformat(),
            "ReturnDate": hidden.get("returnDate", ""),
            "IsIJFlight": hidden.get("isIJFlight", "false"),
            "IsBg": hidden.get("isBg", "false"),
            "IsEmployee": hidden.get("isEmployee", "false"),
            "IsLittleGroupFlight": "false",
            "SeatsNum": query.adults,
            "ActId": hidden.get("ActId", "0"),
            "IfRet": "false",
            "IsUM": "false",
            "CabinActId": "",
            "SpecTravTypeId": hidden.get("SpecTravTypeId", ""),
            "IsContains9CAndIJ": hidden.get("IsContains9CAndIJ", "false"),
            "DepCityCode": context.search_origin,
            "ArrCityCode": context.search_destination,
            "DepAirportCode": context.depart_airport if context.uses_origin_filter else "",
            "ArrAirportCode": context.arrive_airport if context.uses_destination_filter else "",
            "IsSearchDepAirport": "true" if context.uses_origin_filter else "false",
            "IsSearchArrAirport": "true" if context.uses_destination_filter else "false",
        }

    def parse_fares(
        self,
        data: dict[str, object],
        query: SearchQuery,
        context: SpringSearchContext,
        collection_id: str,
    ) -> list[FareRecord]:
        routes = data.get("Route")
        if not isinstance(routes, list):
            return []

        fares: list[FareRecord] = []
        for route in routes:
            if not isinstance(route, list) or not route:
                continue
            first_segment = route[0]
            if not isinstance(first_segment, dict):
                continue
            if not self.route_matches_query(route, query):
                continue
            cabin = self.select_lowest_cabin(first_segment.get("AircraftCabins"))
            if cabin is None:
                continue
            flight_no = "/".join(
                segment.get("No", "")
                for segment in route
                if isinstance(segment, dict) and segment.get("No")
            ) or "SPRING"
            carrier = self.carrier_from_flight_no(flight_no)
            depart_time_full = str(first_segment.get("DepartureTime") or "")
            arrive_time_full = str(first_segment.get("ArrivalTime") or "")
            depart_date = depart_time_full[:10] or query.depart_date.isoformat()
            if depart_date != query.depart_date.isoformat():
                continue
            fare = FareRecord(
                collection_id=collection_id,
                source=self.source_name,
                carrier=carrier,
                carrier_display_name=AIRLINE_DISPLAY.get(carrier, carrier),
                source_display_name=SOURCE_DISPLAY.get(self.source_name, self.source_name),
                source_url=SOURCE_HOME_URL.get(self.source_name, context.booking_url),
                flight_no=flight_no,
                origin=query.origin,
                destination=query.destination,
                depart_date=depart_date,
                depart_time=depart_time_full[11:16] if len(depart_time_full) >= 16 else "",
                arrive_time=arrive_time_full[11:16] if len(arrive_time_full) >= 16 else "",
                stops=self.compute_stops(route, first_segment),
                price_original=float(cabin["total"]),
                currency="CNY",
                price_total_cny=float(cabin["total"]),
                tax_included=True,
                booking_url=context.booking_url,
                search_url=context.booking_url,
                collected_at=utc_now_iso(),
                raw_hash=content_hash(
                    json.dumps(
                        {
                            "flight_no": flight_no,
                            "depart_date": depart_date,
                            "origin": query.origin,
                            "destination": query.destination,
                            "price_total_cny": cabin["total"],
                            "source": self.source_name,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                ),
                fare_scope_note="Verified via Spring official SearchByTime API with exact airport filters; tax included and normalized to CNY.",
                verification_status="verified",
                notes="official-search-by-time",
                is_under_1000=float(cabin["total"]) < self.config.qualified_threshold,
            )
            fares.append(fare)
        return fares

    def parse_browser_fares(
        self,
        page: BrowserSpringPage,
        query: SearchQuery,
        context: SpringSearchContext,
        collection_id: str,
    ) -> list[FareRecord]:
        fares: list[FareRecord] = []
        for item in page.flights:
            flight_no = item.get("flight_no", "").replace("(Share)", "").strip()
            price_info = self.parse_browser_price(item.get("price_text", ""))
            if not flight_no or price_info is None:
                continue
            if not self.browser_row_matches_query(item, query):
                continue
            carrier = item.get("carrier") or self.carrier_from_flight_no(flight_no)
            raw_text = json.dumps(
                {
                    "flight_no": flight_no,
                    "price_total_cny": price_info["price_total_cny"],
                    "price_original": price_info["price_original"],
                    "currency": price_info["currency"],
                    "depart_date": query.depart_date.isoformat(),
                    "origin": query.origin,
                    "destination": query.destination,
                    "page_url": page.url or context.booking_url,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            fares.append(
                FareRecord(
                    collection_id=collection_id,
                    source=self.source_name,
                    carrier=carrier,
                    carrier_display_name=AIRLINE_DISPLAY.get(carrier, carrier),
                    source_display_name=SOURCE_DISPLAY.get(self.source_name, self.source_name),
                    source_url=SOURCE_HOME_URL.get(self.source_name, context.booking_url),
                    flight_no=flight_no,
                    origin=query.origin,
                    destination=query.destination,
                    depart_date=query.depart_date.isoformat(),
                    depart_time=item.get("depart_time", ""),
                    arrive_time=item.get("arrive_time", ""),
                    stops=self.compute_browser_stops(item),
                    price_original=float(price_info["price_original"]),
                    currency=str(price_info["currency"]),
                    price_total_cny=float(price_info["price_total_cny"]),
                    tax_included=True,
                    booking_url=context.booking_url,
                    search_url=page.url or context.booking_url,
                    collected_at=utc_now_iso(),
                    raw_hash=content_hash(raw_text),
                    fare_scope_note="Verified via rendered Spring official booking page in a local Chrome session; displayed total normalized to CNY.",
                    verification_status="verified",
                    notes="official-rendered-browser",
                    is_under_1000=float(price_info["price_total_cny"]) < self.config.qualified_threshold,
                )
            )
        return fares

    def parse_window_browser_fares(
        self,
        page: BrowserSpringPage,
        query: SearchQuery,
        context: SpringSearchContext,
        collection_id: str,
    ) -> list[FareRecord]:
        preview_days = [
            item
            for item in page.preview_days
            if isinstance(item, dict)
        ]
        if not preview_days:
            return []

        selected_index = 0
        for index, item in enumerate(preview_days):
            if bool(item.get("is_selected")):
                selected_index = index
                break

        window_start = query.depart_date - timedelta(days=min(self.config.spring_window_days // 2, selected_index))
        window_end = window_start + timedelta(days=self.config.spring_window_days - 1)
        fares: list[FareRecord] = []
        for day_result in page.day_results:
            if not isinstance(day_result, dict):
                continue
            preview_index = day_result.get("preview_index")
            if not isinstance(preview_index, int):
                continue
            actual_date = query.depart_date + timedelta(days=preview_index - selected_index)
            if actual_date < window_start or actual_date > window_end:
                continue
            lowest = self.select_lowest_browser_flight(day_result.get("flights"), query, actual_date)
            if lowest is None:
                continue
            flight_item, price_info = lowest
            flight_no = flight_item.get("flight_no", "").replace("(Share)", "").strip()
            carrier = flight_item.get("carrier") or self.carrier_from_flight_no(flight_no)
            actual_query = SearchQuery(
                origin=query.origin,
                destination=query.destination,
                depart_date=actual_date,
                adults=query.adults,
                cabin=query.cabin,
            )
            actual_booking_url = self.build_booking_url(actual_query)
            resolved_search_url = self.resolve_day_search_url(
                raw_page_url=str(day_result.get("page_url") or ""),
                fallback_url=actual_booking_url,
                target_date=actual_date,
            )
            raw_text = json.dumps(
                {
                    "flight_no": flight_no,
                    "price_total_cny": price_info["price_total_cny"],
                    "price_original": price_info["price_original"],
                    "currency": price_info["currency"],
                    "depart_date": actual_date.isoformat(),
                    "origin": query.origin,
                    "destination": query.destination,
                    "page_url": day_result.get("page_url") or page.url or context.booking_url,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            fares.append(
                FareRecord(
                    collection_id=collection_id,
                    source=self.source_name,
                    carrier=carrier,
                    carrier_display_name=AIRLINE_DISPLAY.get(carrier, carrier),
                    source_display_name=SOURCE_DISPLAY.get(self.source_name, self.source_name),
                    source_url=SOURCE_HOME_URL.get(self.source_name, context.booking_url),
                    flight_no=flight_no,
                    origin=query.origin,
                    destination=query.destination,
                    depart_date=actual_date.isoformat(),
                    depart_time=flight_item.get("depart_time", ""),
                    arrive_time=flight_item.get("arrive_time", ""),
                    stops=self.compute_browser_stops(flight_item),
                    price_original=float(price_info["price_original"]),
                    currency=str(price_info["currency"]),
                    price_total_cny=float(price_info["price_total_cny"]),
                    tax_included=True,
                    booking_url=actual_booking_url,
                    search_url=resolved_search_url,
                    collected_at=utc_now_iso(),
                    raw_hash=content_hash(raw_text),
                    fare_scope_note=(
                        "Verified via Spring rendered 7-day price strip and same-page date click-through; "
                        "stored fare is the lowest exact-airport flight found for that date."
                    ),
                    verification_status="verified",
                    notes="official-7day-preview-clickthrough",
                    is_under_1000=float(price_info["price_total_cny"]) < self.config.qualified_threshold,
                )
            )
        return fares

    def parse_route_browser_fares(
        self,
        page: BrowserSpringPage,
        origin: str,
        destination: str,
        collection_id: str,
    ) -> list[FareRecord]:
        fares: list[FareRecord] = []
        for day_result in page.day_results:
            if not isinstance(day_result, dict):
                continue
            depart_date_text = str(day_result.get("date", "")).strip()
            if not depart_date_text:
                continue
            try:
                depart_date = date.fromisoformat(depart_date_text)
            except ValueError:
                continue
            query = SearchQuery(origin=origin, destination=destination, depart_date=depart_date)
            lowest = self.select_lowest_browser_flight(day_result.get("flights"), query, depart_date)
            if lowest is None:
                continue
            flight_item, price_info = lowest
            context = self.build_search_context(query)
            flight_no = flight_item.get("flight_no", "").replace("(Share)", "").strip()
            carrier = flight_item.get("carrier") or self.carrier_from_flight_no(flight_no)
            booking_url = context.booking_url
            search_url = self.resolve_day_search_url(
                raw_page_url=str(day_result.get("page_url") or ""),
                fallback_url=context.booking_url,
                target_date=depart_date,
            )
            raw_text = json.dumps(
                {
                    "flight_no": flight_no,
                    "price_total_cny": price_info["price_total_cny"],
                    "price_original": price_info["price_original"],
                    "currency": price_info["currency"],
                    "depart_date": depart_date.isoformat(),
                    "origin": origin,
                    "destination": destination,
                    "page_url": booking_url,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            fares.append(
                FareRecord(
                    collection_id=collection_id,
                    source=self.source_name,
                    carrier=carrier,
                    carrier_display_name=AIRLINE_DISPLAY.get(carrier, carrier),
                    source_display_name=SOURCE_DISPLAY.get(self.source_name, self.source_name),
                    source_url=SOURCE_HOME_URL.get(self.source_name, context.booking_url),
                    flight_no=flight_no,
                    origin=origin,
                    destination=destination,
                    depart_date=depart_date.isoformat(),
                    depart_time=flight_item.get("depart_time", ""),
                    arrive_time=flight_item.get("arrive_time", ""),
                    stops=self.compute_browser_stops(flight_item),
                    price_original=float(price_info["price_original"]),
                    currency=str(price_info["currency"]),
                    price_total_cny=float(price_info["price_total_cny"]),
                    tax_included=True,
                    booking_url=booking_url,
                    search_url=search_url,
                    collected_at=utc_now_iso(),
                    raw_hash=content_hash(raw_text),
                    fare_scope_note=(
                        "Verified via a persistent Spring official route-session scan; "
                        "the stored record is the lowest exact-airport flight for that date."
                    ),
                    verification_status="verified",
                    notes="official-route-session-preview-clickthrough",
                    is_under_1000=float(price_info["price_total_cny"]) < self.config.qualified_threshold,
                )
            )
        return fares

    def resolve_day_search_url(self, raw_page_url: str, fallback_url: str, target_date: date) -> str:
        if raw_page_url and f"FDate={target_date.isoformat()}" in raw_page_url:
            return raw_page_url
        if raw_page_url:
            return re.sub(r"FDate=\d{4}-\d{2}-\d{2}", f"FDate={target_date.isoformat()}", raw_page_url)
        return fallback_url

    def select_lowest_browser_flight(
        self,
        flights: object,
        query: SearchQuery,
        depart_date,
    ) -> tuple[dict[str, str], dict[str, float | str]] | None:
        if not isinstance(flights, list):
            return None
        best: tuple[dict[str, str], dict[str, float | str]] | None = None
        for item in flights:
            if not isinstance(item, dict):
                continue
            normalized = {str(key): str(value) for key, value in item.items()}
            flight_no = normalized.get("flight_no", "").replace("(Share)", "").strip()
            price_info = self.parse_browser_price(normalized.get("price_text", ""))
            if not flight_no or price_info is None:
                continue
            if not self.browser_row_matches_query(normalized, query):
                continue
            if best is None or float(price_info["price_total_cny"]) < float(best[1]["price_total_cny"]):
                best = (normalized, price_info)
        return best

    def parse_browser_price(self, text: str) -> dict[str, float | str] | None:
        match = re.search(r"(CNY|USD|JPY|HKD|TWD|SGD|THB|KRW|MOP)\s*([0-9,]+(?:\.\d+)?)", text, re.IGNORECASE)
        if not match:
            return None
        currency = match.group(1).upper()
        amount = float(match.group(2).replace(",", ""))
        rate = FX_TO_CNY.get(currency)
        if rate is None:
            return None
        return {
            "currency": currency,
            "price_original": amount,
            "price_total_cny": amount * rate,
        }

    def browser_row_matches_query(self, item: dict[str, str], query: SearchQuery) -> bool:
        row_text = item.get("row_text", "")
        return (
            item.get("depart_time", "") != ""
            and item.get("arrive_time", "") != ""
            and self.airport_name_matches_code(item.get("depart_airport", ""), query.origin)
            and self.airport_name_matches_code(item.get("arrive_airport", ""), query.destination)
            and query.depart_date.day == query.depart_date.day
            and row_text != ""
        )

    def compute_browser_stops(self, item: dict[str, str]) -> int:
        text = item.get("row_text", "").lower()
        if "stop" in text or "via" in text:
            return 1
        return 0

    def airport_name_matches_code(self, airport_name: str, code: str) -> bool:
        name = airport_name.lower().replace(" ", "")
        mapping = {
            "PEK": ("beijingcapital", "capitalinternational"),
            "PKX": ("beijingdaxing", "daxing"),
            "TSN": ("tianjin",),
            "SJW": ("shijiazhuang", "zhengding"),
            "SHA": ("shanghaihongqiao", "hongqiao"),
            "PVG": ("shanghaipudong", "pudong"),
            "NRT": ("narita",),
            "HND": ("haneda",),
            "KIX": ("kansai",),
            "ITM": ("itami",),
            "NGO": ("nagoya", "chubu"),
            "FUK": ("fukuoka",),
            "CTS": ("sapporo", "newchitose", "chitose"),
            "OKA": ("okinawa", "naha"),
        }
        return any(token in name for token in mapping.get(code, (code.lower(),)))

    def route_matches_query(self, route: list[object], query: SearchQuery) -> bool:
        first_segment = route[0] if route else {}
        last_segment = route[-1] if route else {}
        if not isinstance(first_segment, dict) or not isinstance(last_segment, dict):
            return False
        depart_airport = str(first_segment.get("DepartureAirportCode") or first_segment.get("DepartureCode") or "")
        arrive_airport = str(last_segment.get("ArrivalAirportCode") or last_segment.get("ArrivalCode") or "")
        depart_time = str(first_segment.get("DepartureTime") or "")
        return (
            depart_airport == query.origin
            and arrive_airport == query.destination
            and depart_time.startswith(query.depart_date.isoformat())
        )

    def select_lowest_cabin(self, cabins: object) -> dict[str, float | str] | None:
        if not isinstance(cabins, list):
            return None
        best: dict[str, float | str] | None = None
        for cabin in cabins:
            if not isinstance(cabin, dict):
                continue
            infos = cabin.get("AircraftCabinInfos")
            if not isinstance(infos, list) or not infos:
                continue
            info = infos[0]
            if not isinstance(info, dict):
                continue
            remain = info.get("Remain")
            if isinstance(remain, (int, float)) and remain == 0:
                continue
            total = 0.0
            for key in ("Price", "AirportConstructionFees", "FuelSurcharge", "OtherFees", "AbroadFee"):
                value = info.get(key)
                if isinstance(value, (int, float)):
                    total += float(value)
            if total <= 0:
                continue
            candidate = {
                "cabin_name": str(info.get("Name") or cabin.get("CabinLevelName") or ""),
                "total": total,
            }
            if best is None or float(candidate["total"]) < float(best["total"]):
                best = candidate
        return best

    def compute_stops(self, route: list[object], first_segment: dict[str, object]) -> int:
        stopovers = first_segment.get("Stopovers")
        stopover_count = len(stopovers) if isinstance(stopovers, list) else 0
        return max(len(route) - 1, stopover_count)

    def carrier_from_flight_no(self, flight_no: str) -> str:
        prefix = flight_no[:2].upper()
        return CARRIER_BY_FLIGHT_PREFIX.get(prefix, self.carrier_name)
