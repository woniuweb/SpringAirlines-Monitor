from datetime import date

from fare_monitor.browser_agent import BrowserSpringPage
from fare_monitor.collector import build_spring_route_tasks
from fare_monitor.config import AppConfig
from fare_monitor.models import SearchQuery
from fare_monitor.probe import parse_code_list, summarize_spring_route_probes
from fare_monitor.sources.jetstar_japan import JetstarJapanAdapter
from fare_monitor.sources.peach import PeachAdapter
from fare_monitor.sources.spring_airlines import SpringAirlinesAdapter, SpringLiveProbeResult
from fare_monitor.sources.spring_japan import SpringJapanAdapter


def test_spring_airlines_discovery_extracts_official_route_links(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)

    def fake_fetch_text(url: str) -> tuple[str, str]:
        html = """
        <html><body>
          <a href="https://en.ch.com/PEK-KIX/">PEK-KIX</a>
          <a href="https://en.ch.com/TSN-NRT/">TSN-NRT</a>
          <a href="https://en.ch.com/SHA-FUK/">SHA-FUK</a>
        </body></html>
        """
        return html, url

    adapter.fetch_text = fake_fetch_text  # type: ignore[method-assign]
    route_keys = adapter.discover_route_keys()
    assert "PEK->KIX" in route_keys
    assert "TSN->NRT" in route_keys
    assert "SHA->FUK" in route_keys


def test_source_booking_urls_cover_spring_routes(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    query = SearchQuery(origin="PEK", destination="NRT", depart_date=date(2026, 4, 20))
    spring = SpringAirlinesAdapter(config=config, sample_mode=False)
    spring_japan = SpringJapanAdapter(config=config, sample_mode=False)
    assert "BJS-TYO.html" in spring.build_booking_url(query)
    assert "FDate=2026-04-20" in spring.build_booking_url(query)
    assert "DepAirportCode=PEK" in spring.build_booking_url(query)
    assert "ArrAirportCode=NRT" in spring.build_booking_url(query)
    assert "BJS-TYO.html" in spring_japan.build_booking_url(query)
    assert "FDate=2026-04-20" in spring_japan.build_booking_url(query)


def test_new_lcc_adapters_expose_shanghai_routes(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    peach = PeachAdapter(config=config, sample_mode=False)
    jetstar = JetstarJapanAdapter(config=config, sample_mode=False)
    assert "PVG->KIX" in peach.discover_route_keys()
    assert "PVG->NRT" in jetstar.discover_route_keys()
    assert "PVG->KIX" in jetstar.discover_route_keys()


def test_stable_spring_live_routes_expand_pvg_and_keep_unverified_sha_sjw_out(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    spring = SpringAirlinesAdapter(config=config, sample_mode=False)
    routes = spring.supported_route_keys()
    assert "PEK->NRT" in routes
    assert "TSN->NRT" in routes
    assert "PVG->NRT" in routes
    assert "PVG->HND" in routes
    assert "PVG->KIX" in routes
    assert "PVG->FUK" in routes
    assert "PVG->NGO" in routes
    assert "PVG->CTS" not in routes
    assert "PVG->OKA" not in routes
    assert "SHA->KIX" not in routes
    assert "SJW->NRT" not in routes


def test_build_spring_route_tasks_groups_daily_queries_into_single_route_session(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    config.origins = ("TSN",)
    config.destinations = ("NRT",)
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)
    queries = config.build_queries(days=14, start_date=date(2026, 4, 10))
    tasks = build_spring_route_tasks(adapter, queries, collection_id="collect-1")
    assert len(tasks) == 1
    assert tasks[0].route_key == "TSN->NRT"
    assert tasks[0].start_date.isoformat() == "2026-04-10"
    assert tasks[0].end_date.isoformat() == "2026-04-23"


def test_spring_parse_fares_uses_exact_airport_match_and_tax_included_total(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)
    query = SearchQuery(origin="PEK", destination="NRT", depart_date=date(2026, 4, 19))
    context = adapter.build_search_context(query)
    payload = {
        "Route": [
            [
                {
                    "No": "IJ018",
                    "DepartureAirportCode": "PEK",
                    "ArrivalAirportCode": "NRT",
                    "DepartureTime": "2026-04-19 10:45:00",
                    "ArrivalTime": "2026-04-19 15:30:00",
                    "Stopovers": [],
                    "AircraftCabins": [
                        {
                            "CabinLevel": 5,
                            "AircraftCabinInfos": [
                                {
                                    "Name": "M",
                                    "Price": 1625,
                                    "AirportConstructionFees": 90,
                                    "FuelSurcharge": 205,
                                    "OtherFees": 0,
                                    "Remain": 3,
                                }
                            ],
                        }
                    ],
                }
            ],
            [
                {
                    "No": "IJ999",
                    "DepartureAirportCode": "PKX",
                    "ArrivalAirportCode": "NRT",
                    "DepartureTime": "2026-04-19 08:00:00",
                    "ArrivalTime": "2026-04-19 12:00:00",
                    "Stopovers": [],
                    "AircraftCabins": [
                        {
                            "CabinLevel": 5,
                            "AircraftCabinInfos": [
                                {
                                    "Name": "M",
                                    "Price": 999,
                                    "AirportConstructionFees": 90,
                                    "FuelSurcharge": 205,
                                    "OtherFees": 0,
                                    "Remain": 3,
                                }
                            ],
                        }
                    ],
                }
            ],
        ],
        "Code": 0,
    }
    fares = adapter.parse_fares(payload, query=query, context=context, collection_id="test-1")
    assert len(fares) == 1
    fare = fares[0]
    assert fare.flight_no == "IJ018"
    assert fare.carrier == "SPRING JAPAN"
    assert fare.price_total_cny == 1920.0
    assert fare.currency == "CNY"
    assert fare.booking_url == context.booking_url


def test_spring_parse_browser_fares_uses_rendered_dom_price(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)
    query = SearchQuery(origin="PEK", destination="NRT", depart_date=date(2026, 4, 19))
    context = adapter.build_search_context(query)
    page = BrowserSpringPage(
        title="Book Cheap flights from Beijing to Tokyo(HANEDA/NARITA) Tickets Online | Spring",
        url=context.booking_url,
        total_text="¥1,820",
        body_text="Beijing - Tokyo(HANEDA/NARITA) SPRING JAPAN IJ018 From CNY1,625",
        raw_payload="{}",
        flights=[
            {
                "carrier": "SPRING JAPAN",
                "flight_no": "IJ018 (Share)",
                "depart_time": "10:45",
                "arrive_time": "15:30",
                "depart_airport": "BeijingCapital International Airport T3",
                "arrive_airport": "TokyoNarita International Airport T3",
                "duration": "3H45M",
                "price_text": "From USD 220.8 Select",
                "row_text": "SPRING JAPAN IJ018 10:45 BeijingCapital International Airport T3 15:30 TokyoNarita International Airport T3 From USD 220.8",
            }
        ],
    )
    fares = adapter.parse_browser_fares(page=page, query=query, context=context, collection_id="browser-1")
    assert len(fares) == 1
    fare = fares[0]
    assert fare.flight_no == "IJ018"
    assert round(fare.price_total_cny, 2) == round(220.8 * 6.1237, 2)
    assert fare.currency == "USD"
    assert fare.price_original == 220.8
    assert fare.carrier == "SPRING JAPAN"
    assert fare.notes == "official-rendered-browser"


def test_spring_parse_window_browser_fares_keeps_only_lowest_flight_for_day(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)
    query = SearchQuery(origin="TSN", destination="NRT", depart_date=date(2026, 4, 14))
    context = adapter.build_search_context(query)
    page = BrowserSpringPage(
        title="Book Cheap flights from Tianjin to Tokyo(HANEDA/NARITA) Tickets Online | Spring",
        url=context.booking_url,
        total_text="",
        body_text="",
        raw_payload="{}",
        flights=[],
        preview_days=[
            {"preview_index": 0, "label": "11 Apr, Sat", "price_total_cny": 1935.0, "is_selected": False},
            {"preview_index": 1, "label": "12 Apr, Sun", "price_total_cny": 1935.0, "is_selected": False},
            {"preview_index": 2, "label": "13 Apr, Mon", "price_total_cny": 1625.0, "is_selected": False},
            {"preview_index": 3, "label": "14 Apr, Tue", "price_total_cny": 1365.0, "is_selected": True},
            {"preview_index": 4, "label": "15 Apr, Wed", "price_total_cny": 1365.0, "is_selected": False},
            {"preview_index": 5, "label": "16 Apr, Thu", "price_total_cny": 1365.0, "is_selected": False},
            {"preview_index": 6, "label": "17 Apr, Fri", "price_total_cny": 1115.0, "is_selected": False},
        ],
        day_results=[
            {
                "preview_index": 6,
                "label": "17 Apr, Fri",
                "page_url": "https://en.ch.com/flights/TSN-TYO.html?FDate=2026-04-14",
                "clicked": True,
                "flights": [
                    {
                        "carrier": "SPRING JAPAN",
                        "flight_no": "IJ254",
                        "depart_time": "17:00",
                        "arrive_time": "21:20",
                        "depart_airport": "TianjinBinhai International Airport T1",
                        "arrive_airport": "TokyoNarita International Airport T3",
                        "duration": "3H20M",
                        "price_text": "From CNY1,165 Select",
                        "row_text": "SPRING JAPAN IJ254 17:00 TianjinBinhai International Airport T1 21:20 TokyoNarita International Airport T3 From CNY1,165 Select",
                    },
                    {
                        "carrier": "SPRING JAPAN",
                        "flight_no": "IJ256",
                        "depart_time": "08:00",
                        "arrive_time": "12:20",
                        "depart_airport": "TianjinBinhai International Airport T1",
                        "arrive_airport": "TokyoNarita International Airport T3",
                        "duration": "3H20M",
                        "price_text": "From CNY1,115 Select",
                        "row_text": "SPRING JAPAN IJ256 08:00 TianjinBinhai International Airport T1 12:20 TokyoNarita International Airport T3 From CNY1,115 Select",
                    },
                ],
            }
        ],
    )
    fares = adapter.parse_window_browser_fares(page=page, query=query, context=context, collection_id="window-1")
    assert len(fares) == 1
    fare = fares[0]
    assert fare.depart_date == "2026-04-17"
    assert fare.flight_no == "IJ256"
    assert fare.price_total_cny == 1115.0
    assert fare.notes == "official-7day-preview-clickthrough"


def test_spring_parse_route_browser_fares_keeps_lowest_flight_per_day(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)
    page = BrowserSpringPage(
        title="Book Cheap flights from Tianjin to Tokyo(HANEDA/NARITA) Tickets Online | Spring",
        url="https://en.ch.com/flights/TSN-TYO.html?FDate=2026-04-14",
        total_text="",
        body_text="",
        raw_payload="{}",
        preview_days=[],
        day_results=[
            {
                "date": "2026-04-17",
                "page_url": "https://en.ch.com/flights/TSN-TYO.html?FDate=2026-04-17&ArrAirportCode=NRT",
                "clicked": True,
                "preview_price_total_cny": 1115.0,
                "flights": [
                    {
                        "carrier": "SPRING JAPAN",
                        "flight_no": "IJ254",
                        "depart_time": "17:00",
                        "arrive_time": "21:20",
                        "depart_airport": "TianjinBinhai International Airport T1",
                        "arrive_airport": "TokyoNarita International Airport T3",
                        "duration": "3H20M",
                        "price_text": "From CNY1,165 Select",
                        "row_text": "SPRING JAPAN IJ254 17:00 TianjinBinhai International Airport T1 21:20 TokyoNarita International Airport T3 From CNY1,165 Select",
                    },
                    {
                        "carrier": "SPRING JAPAN",
                        "flight_no": "IJ256",
                        "depart_time": "08:00",
                        "arrive_time": "12:20",
                        "depart_airport": "TianjinBinhai International Airport T1",
                        "arrive_airport": "TokyoNarita International Airport T3",
                        "duration": "3H20M",
                        "price_text": "From CNY1,115 Select",
                        "row_text": "SPRING JAPAN IJ256 08:00 TianjinBinhai International Airport T1 12:20 TokyoNarita International Airport T3 From CNY1,115 Select",
                    },
                ],
                "error": "",
            }
        ],
        flights=[],
    )
    fares = adapter.parse_route_browser_fares(page=page, origin="TSN", destination="NRT", collection_id="route-1")
    assert len(fares) == 1
    fare = fares[0]
    assert fare.depart_date == "2026-04-17"
    assert fare.flight_no == "IJ256"
    assert fare.price_total_cny == 1115.0
    assert fare.notes == "official-route-session-preview-clickthrough"


def test_spring_scan_route_live_keeps_partial_results_after_block(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)

    def fake_scan_spring_route(**kwargs):
        return BrowserSpringPage(
            title="405",
            url=str(kwargs["url"]),
            final_url=str(kwargs["url"]),
            total_text="",
            body_text="",
            raw_payload="{}",
            blocked=True,
            blocked_after_progress=True,
            weeks_scanned=3,
            empty_weeks=2,
            preview_days=[],
            day_results=[
                {
                    "date": "2026-05-14",
                    "page_url": "https://en.ch.com/flights/TSN-TYO.html?FDate=2026-05-14&ArrAirportCode=NRT",
                    "clicked": True,
                    "preview_price_total_cny": 895.0,
                    "flights": [
                        {
                            "carrier": "SPRING JAPAN",
                            "flight_no": "IJ254",
                            "depart_time": "10:35",
                            "arrive_time": "14:55",
                            "depart_airport": "TianjinBinhai International Airport T1",
                            "arrive_airport": "TokyoNarita International Airport T3",
                            "duration": "3H20M",
                            "price_text": "From CNY895 Select",
                            "row_text": "SPRING JAPAN IJ254 10:35 TianjinBinhai International Airport T1 14:55 TokyoNarita International Airport T3 From CNY895 Select",
                        }
                    ],
                    "error": "",
                }
            ],
            fail_samples=["2026-04-30: blocked=page title returned 405"],
            flights=[],
        )

    adapter.browser.scan_spring_route = fake_scan_spring_route  # type: ignore[method-assign]
    result = adapter.scan_route_live(
        origin="TSN",
        destination="NRT",
        start_date=date(2026, 4, 9),
        end_date=date(2026, 8, 6),
        collection_id="route-live-1",
    )
    assert result.status == "partial"
    assert len(result.fares) == 1
    assert result.fares[0].price_total_cny == 895.0
    assert result.stats["empty_weeks"] == 2
    assert result.stats["blocked_after_progress"] == 1


def test_parse_code_list_preserves_order_and_uppercases() -> None:
    assert parse_code_list("sha, sjw,PVG", ("PEK",)) == ("SHA", "SJW", "PVG")
    assert parse_code_list(None, ("PEK", "TSN")) == ("PEK", "TSN")


def test_spring_route_probe_summary_prefers_verified_result() -> None:
    results = [
        SpringLiveProbeResult(
            query=SearchQuery(origin="SHA", destination="KIX", depart_date=date(2026, 4, 10)),
            booking_url="https://example.com/1",
            final_url="https://example.com/1",
            page_title="Flights",
            status="empty",
            matched_fare_count=0,
            rendered_flight_count=2,
            message="Rendered rows exist, but none matched the exact airport pair.",
        ),
        SpringLiveProbeResult(
            query=SearchQuery(origin="SHA", destination="KIX", depart_date=date(2026, 4, 24)),
            booking_url="https://example.com/2",
            final_url="https://example.com/2",
            page_title="Flights",
            status="verified",
            matched_fare_count=1,
            rendered_flight_count=1,
            matched_flights=("9C6211",),
            currencies=("CNY",),
            message="Official Spring booking page rendered exact-airport sellable rows.",
        ),
    ]
    summaries = summarize_spring_route_probes(results)
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.route_key == "SHA->KIX"
    assert summary.status == "verified"
    assert summary.verified_dates == ("2026-04-24",)
