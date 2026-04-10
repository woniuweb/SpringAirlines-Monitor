from fare_monitor.config import AppConfig


def test_config_loads_toml_and_keeps_cli_override_ready(tmp_path) -> None:
    config_path = tmp_path / "fare-monitor.toml"
    config_path.write_text(
        """
[search]
origins = ["PEK", "PVG"]
destinations = ["NRT", "KIX"]
scan_days = 90
qualified_threshold_cny = 1234

[report]
exclude_today = false
top_n = 12
title = "华北飞日本监控"
scope_description = "只看华北出发"
rules_description = "规则说明"
show_connection_candidates = true

[sources]
spring_airlines_enabled = true
spring_japan_enabled = true
peach_enabled = false
jetstar_japan_enabled = false

[performance]
spring_live_workers = 3
request_timeout_seconds = 18
probe_step_days = 21
spring_window_days = 7
spring_date_click_threshold_cny = 1199
spring_max_consecutive_empty_weeks = 5

[logging]
enabled = true
level = "INFO"
stage_summary = true
""".strip(),
        encoding="utf-8",
    )

    config = AppConfig.from_base_dir(tmp_path, config_path=config_path)
    assert config.config_loaded is True
    assert config.origins == ("PEK", "PVG")
    assert config.destinations == ("NRT", "KIX")
    assert config.scan_days == 90
    assert config.qualified_threshold == 1234.0
    assert config.report_exclude_today is False
    assert config.report_top_n == 12
    assert config.report_title == "华北飞日本监控"
    assert config.report_scope_description == "只看华北出发"
    assert config.report_rules_description == "规则说明"
    assert config.report_show_connection_candidates is True
    assert config.spring_live_workers == 3
    assert config.request_timeout == 18
    assert config.probe_step_days == 21
    assert config.spring_window_days == 7
    assert config.spring_date_click_threshold_cny == 1199.0
    assert config.spring_max_consecutive_empty_weeks == 5
    assert config.is_source_enabled("spring_japan") is True


def test_missing_config_falls_back_to_defaults(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    assert config.config_loaded is False
    assert config.origins == ("PEK", "TSN", "SJW")
    assert config.destinations
    assert config.qualified_threshold == 1200.0
    assert config.report_top_n == 25
    assert config.spring_live_workers == 1
    assert config.spring_window_days == 7
    assert config.report_show_connection_candidates is False
    assert config.spring_max_consecutive_empty_weeks == 6
