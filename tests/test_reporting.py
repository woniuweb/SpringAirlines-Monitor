from datetime import date, timedelta

from fare_monitor.reporting import (
    build_destination_weekly_series,
    build_dynamic_report_title,
    build_dynamic_scope_description,
    build_small_multiple_charts,
    origin_chart_color,
    build_weekly_minimums,
    filter_verified_future_qualified_rows,
)


def test_future_filter_excludes_today_unverified_and_over_threshold() -> None:
    today = date.today()
    rows = [
        {
            "depart_date": today.isoformat(),
            "depart_time": "08:00",
            "price_total_cny": 650.0,
            "carrier_display_name": "SPRING JAPAN",
            "source_display_name": "SPRING JAPAN 官网",
            "verification_status": "verified",
        },
        {
            "depart_date": (today + timedelta(days=1)).isoformat(),
            "depart_time": "09:00",
            "price_total_cny": 880.0,
            "carrier_display_name": "SPRING JAPAN",
            "source_display_name": "SPRING JAPAN 官网",
            "verification_status": "verified",
        },
        {
            "depart_date": (today + timedelta(days=1)).isoformat(),
            "depart_time": "07:00",
            "price_total_cny": 880.0,
            "carrier_display_name": "春秋航空 Spring Airlines",
            "source_display_name": "Spring Airlines 官网",
            "verification_status": "unverified",
        },
        {
            "depart_date": (today + timedelta(days=2)).isoformat(),
            "depart_time": "06:00",
            "price_total_cny": 1200.0,
            "carrier_display_name": "SPRING JAPAN",
            "source_display_name": "SPRING JAPAN 官网",
            "verification_status": "verified",
        },
        {
            "depart_date": (today + timedelta(days=3)).isoformat(),
            "depart_time": "06:30",
            "price_total_cny": 870.0,
            "carrier_display_name": "春秋航空 Spring Airlines",
            "source_display_name": "Spring Airlines 官网",
            "verification_status": "verified",
        },
    ]
    filtered = filter_verified_future_qualified_rows(rows, today=today, threshold=1200.0, exclude_today=True)
    assert [row["price_total_cny"] for row in filtered] == [870.0, 880.0]
    assert all(str(row["depart_date"]) > today.isoformat() for row in filtered)
    assert all(row["verification_status"] == "verified" for row in filtered)


def test_weekly_minimums_keep_one_row_per_route_per_week() -> None:
    rows = [
        {
            "origin": "PEK",
            "destination": "NRT",
            "depart_date": "2026-04-13",
            "depart_time": "09:00",
            "price_total_cny": 890.0,
        },
        {
            "origin": "PEK",
            "destination": "NRT",
            "depart_date": "2026-04-15",
            "depart_time": "08:00",
            "price_total_cny": 840.0,
        },
        {
            "origin": "PEK",
            "destination": "NRT",
            "depart_date": "2026-04-22",
            "depart_time": "08:00",
            "price_total_cny": 910.0,
        },
    ]
    weekly = build_weekly_minimums(rows)
    series = weekly["PEK->NRT"]
    assert len(series) == 2
    assert series[0]["price_total_cny"] == 840.0
    assert series[1]["price_total_cny"] == 910.0


def test_destination_weekly_series_groups_multiple_origins_under_one_destination() -> None:
    rows = [
        {
            "origin": "PEK",
            "destination": "NRT",
            "depart_date": "2026-04-13",
            "depart_time": "09:00",
            "price_total_cny": 1115.0,
        },
        {
            "origin": "TSN",
            "destination": "NRT",
            "depart_date": "2026-04-14",
            "depart_time": "10:35",
            "price_total_cny": 895.0,
        },
        {
            "origin": "TSN",
            "destination": "NRT",
            "depart_date": "2026-04-15",
            "depart_time": "10:35",
            "price_total_cny": 1005.0,
        },
    ]
    grouped = build_destination_weekly_series(rows)
    assert "NRT" in grouped
    assert set(grouped["NRT"].keys()) == {"PEK", "TSN"}
    assert len(grouped["NRT"]["PEK"]) == 1
    assert len(grouped["NRT"]["TSN"]) == 1


def test_small_multiple_charts_cover_all_configured_destinations_and_render_empty_cards(tmp_path) -> None:
    from fare_monitor.config import AppConfig

    config = AppConfig.from_base_dir(tmp_path)
    config.origins = ("PEK", "TSN", "SJW")
    config.destinations = ("NRT", "HND")
    grouped = build_destination_weekly_series(
        [
            {
                "origin": "PEK",
                "destination": "NRT",
                "depart_date": "2026-04-13",
                "depart_time": "09:00",
                "price_total_cny": 1115.0,
            },
            {
                "origin": "TSN",
                "destination": "NRT",
                "depart_date": "2026-04-14",
                "depart_time": "10:35",
                "price_total_cny": 895.0,
            },
        ]
    )

    html = build_small_multiple_charts(list(config.destinations), grouped, config)

    assert "东京成田 (NRT)" in html
    assert "东京羽田 (HND)" in html
    assert "当前采集范围内暂无该目的地的已验证票价数据。" in html
    assert "北京首都 (PEK)" in html
    assert "天津滨海 (TSN)" in html
    assert "#0b3954" in html
    assert "#d1495b" in html


def test_origin_chart_color_keeps_fixed_mapping_for_core_origins() -> None:
    assert origin_chart_color("PEK", 0) == "#0b3954"
    assert origin_chart_color("TSN", 1) == "#d1495b"
    assert origin_chart_color("SJW", 2) == "#2a9d8f"


def test_dynamic_report_text_follows_config_scope(tmp_path) -> None:
    from fare_monitor.config import AppConfig

    config = AppConfig.from_base_dir(tmp_path)
    config.origins = ("PEK", "TSN", "SJW")
    config.destinations = ("NRT", "HND")
    title = build_dynamic_report_title(config)
    scope = build_dynamic_scope_description(config, [])
    assert "北京首都（PEK）" in title
    assert "天津滨海（TSN）" in title
    assert "石家庄正定（SJW）" in title
    assert "上海" not in title
    assert "上海" not in scope
