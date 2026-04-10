from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
import tomllib

from fare_monitor.constants import (
    DEFAULT_DESTINATIONS,
    DEFAULT_LOGGING_ENABLED,
    DEFAULT_LOG_LEVEL,
    DEFAULT_ORIGINS,
    DEFAULT_SPRING_DATE_CLICK_THRESHOLD,
    DEFAULT_SPRING_MAX_CONSECUTIVE_EMPTY_WEEKS,
    DEFAULT_PROBE_STEP_DAYS,
    DEFAULT_QUALIFIED_THRESHOLD,
    DEFAULT_REPORT_EXCLUDE_TODAY,
    DEFAULT_REPORT_RULES_DESCRIPTION,
    DEFAULT_REPORT_SCOPE_DESCRIPTION,
    DEFAULT_REPORT_SHOW_CONNECTION_CANDIDATES,
    DEFAULT_REPORT_TITLE,
    DEFAULT_REPORT_TOP_N,
    DEFAULT_SCAN_DAYS,
    DEFAULT_SOURCE_FLAGS,
    DEFAULT_SPRING_LIVE_WORKERS,
    DEFAULT_SPRING_WINDOW_DAYS,
    DEFAULT_STAGE_SUMMARY,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
)
from fare_monitor.models import SearchQuery


def _validate_code_list(values: object, field_name: str) -> tuple[str, ...]:
    if not isinstance(values, list) or not values:
        raise ValueError(f"{field_name} must be a non-empty list of IATA codes.")
    codes: list[str] = []
    for value in values:
        if not isinstance(value, str) or len(value.strip()) != 3:
            raise ValueError(f"{field_name} contains an invalid airport code: {value!r}")
        codes.append(value.strip().upper())
    return tuple(codes)


def _validate_positive_int(value: object, field_name: str, minimum: int = 1) -> int:
    if not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field_name} must be an integer >= {minimum}.")
    return value


def _validate_non_negative_float(value: object, field_name: str, minimum: float = 0.0) -> float:
    if not isinstance(value, (int, float)) or float(value) < minimum:
        raise ValueError(f"{field_name} must be a number >= {minimum}.")
    return float(value)


def _validate_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be true or false.")
    return value


def _validate_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _validate_table(value: object, field_name: str) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a TOML table.")
    return value


@dataclass
class AppConfig:
    base_dir: Path
    data_dir: Path
    output_dir: Path
    raw_dir: Path
    database_path: Path
    config_path: Path | None = None
    config_loaded: bool = False
    origins: tuple[str, ...] = DEFAULT_ORIGINS
    destinations: tuple[str, ...] = DEFAULT_DESTINATIONS
    scan_days: int = DEFAULT_SCAN_DAYS
    qualified_threshold: float = DEFAULT_QUALIFIED_THRESHOLD
    request_timeout: int = DEFAULT_TIMEOUT
    user_agent: str = DEFAULT_USER_AGENT
    report_exclude_today: bool = DEFAULT_REPORT_EXCLUDE_TODAY
    report_top_n: int = DEFAULT_REPORT_TOP_N
    report_title: str = DEFAULT_REPORT_TITLE
    report_scope_description: str = DEFAULT_REPORT_SCOPE_DESCRIPTION
    report_rules_description: str = DEFAULT_REPORT_RULES_DESCRIPTION
    report_show_connection_candidates: bool = DEFAULT_REPORT_SHOW_CONNECTION_CANDIDATES
    source_flags: dict[str, bool] = field(default_factory=lambda: dict(DEFAULT_SOURCE_FLAGS))
    spring_live_workers: int = DEFAULT_SPRING_LIVE_WORKERS
    probe_step_days: int = DEFAULT_PROBE_STEP_DAYS
    spring_window_days: int = DEFAULT_SPRING_WINDOW_DAYS
    spring_date_click_threshold_cny: float = DEFAULT_SPRING_DATE_CLICK_THRESHOLD
    spring_max_consecutive_empty_weeks: int = DEFAULT_SPRING_MAX_CONSECUTIVE_EMPTY_WEEKS
    logging_enabled: bool = DEFAULT_LOGGING_ENABLED
    log_level: str = DEFAULT_LOG_LEVEL
    stage_summary: bool = DEFAULT_STAGE_SUMMARY
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_base_dir(cls, base_dir: Path, config_path: Path | None = None) -> "AppConfig":
        base_dir = base_dir.resolve()
        candidate_path = config_path.resolve() if config_path else (base_dir / "fare-monitor.toml")
        config = cls(
            base_dir=base_dir,
            data_dir=base_dir / "data",
            output_dir=base_dir / "output",
            raw_dir=base_dir / "data" / "raw",
            database_path=base_dir / "data" / "fares.db",
            config_path=candidate_path if candidate_path.exists() else candidate_path,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        )
        if candidate_path.exists():
            config.apply_toml(candidate_path)
            config.config_loaded = True
        return config

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "latest").mkdir(parents=True, exist_ok=True)

    def apply_toml(self, path: Path) -> None:
        try:
            data = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ValueError(f"Invalid TOML in {path}: {exc}") from exc

        search = _validate_table(data.get("search", {}), "search")
        if search:
            if "origins" in search:
                self.origins = _validate_code_list(search["origins"], "search.origins")
            if "destinations" in search:
                self.destinations = _validate_code_list(search["destinations"], "search.destinations")
            if "scan_days" in search:
                self.scan_days = _validate_positive_int(search["scan_days"], "search.scan_days")
            if "qualified_threshold_cny" in search:
                self.qualified_threshold = _validate_non_negative_float(
                    search["qualified_threshold_cny"],
                    "search.qualified_threshold_cny",
                )

        report = _validate_table(data.get("report", {}), "report")
        if report:
            if "exclude_today" in report:
                self.report_exclude_today = _validate_bool(report["exclude_today"], "report.exclude_today")
            if "top_n" in report:
                self.report_top_n = _validate_positive_int(report["top_n"], "report.top_n")
            if "title" in report:
                self.report_title = str(report["title"]).strip()
            if "scope_description" in report:
                self.report_scope_description = str(report["scope_description"]).strip()
            if "rules_description" in report:
                self.report_rules_description = str(report["rules_description"]).strip()
            if "show_connection_candidates" in report:
                self.report_show_connection_candidates = _validate_bool(
                    report["show_connection_candidates"],
                    "report.show_connection_candidates",
                )

        sources = _validate_table(data.get("sources", {}), "sources")
        if sources:
            for key in DEFAULT_SOURCE_FLAGS:
                field_name = f"{key}_enabled"
                if field_name in sources:
                    self.source_flags[key] = _validate_bool(sources[field_name], f"sources.{field_name}")

        performance = _validate_table(data.get("performance", {}), "performance")
        if performance:
            if "spring_live_workers" in performance:
                self.spring_live_workers = _validate_positive_int(
                    performance["spring_live_workers"],
                    "performance.spring_live_workers",
                )
            if "request_timeout_seconds" in performance:
                self.request_timeout = _validate_positive_int(
                    performance["request_timeout_seconds"],
                    "performance.request_timeout_seconds",
                )
            if "probe_step_days" in performance:
                self.probe_step_days = _validate_positive_int(
                    performance["probe_step_days"],
                    "performance.probe_step_days",
                )
            if "spring_window_days" in performance:
                self.spring_window_days = _validate_positive_int(
                    performance["spring_window_days"],
                    "performance.spring_window_days",
                )
            if "spring_date_click_threshold_cny" in performance:
                self.spring_date_click_threshold_cny = _validate_non_negative_float(
                    performance["spring_date_click_threshold_cny"],
                    "performance.spring_date_click_threshold_cny",
                )
            if "spring_max_consecutive_empty_weeks" in performance:
                self.spring_max_consecutive_empty_weeks = _validate_positive_int(
                    performance["spring_max_consecutive_empty_weeks"],
                    "performance.spring_max_consecutive_empty_weeks",
                )

        logging = _validate_table(data.get("logging", {}), "logging")
        if logging:
            if "enabled" in logging:
                self.logging_enabled = _validate_bool(logging["enabled"], "logging.enabled")
            if "level" in logging:
                self.log_level = _validate_string(logging["level"], "logging.level").upper()
            if "stage_summary" in logging:
                self.stage_summary = _validate_bool(logging["stage_summary"], "logging.stage_summary")

        self.headers = {"User-Agent": self.user_agent}

    def build_queries(self, days: int | None = None, start_date: date | None = None) -> list[SearchQuery]:
        actual_days = days or self.scan_days
        anchor = start_date or date.today()
        queries: list[SearchQuery] = []
        for offset in range(actual_days):
            depart_date = anchor + timedelta(days=offset)
            for origin in self.origins:
                for destination in self.destinations:
                    queries.append(
                        SearchQuery(
                            origin=origin,
                            destination=destination,
                            depart_date=depart_date,
                        )
                    )
        return queries

    def is_source_enabled(self, source_name: str) -> bool:
        return bool(self.source_flags.get(source_name, False))

    def log_file_path(self) -> Path:
        return self.output_dir / "latest" / "run.log"

    def config_label(self) -> str:
        if self.config_loaded and self.config_path is not None:
            return str(self.config_path)
        if self.config_path is None:
            return "(defaults only)"
        return f"{self.config_path} (not found, using defaults)"
