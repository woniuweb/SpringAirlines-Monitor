from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from fare_monitor.config import AppConfig
from fare_monitor.models import SearchQuery
from fare_monitor.sources.spring_airlines import SpringAirlinesAdapter, SpringLiveProbeResult
from fare_monitor.stage_logging import StageLogger


@dataclass(frozen=True)
class SpringRouteProbeSummary:
    route_key: str
    status: str
    attempts: int
    verified_dates: tuple[str, ...]
    message: str


def parse_code_list(raw_value: str | None, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if not raw_value:
        return fallback
    values = tuple(part.strip().upper() for part in raw_value.split(",") if part.strip())
    return values or fallback


def build_probe_queries(
    origins: tuple[str, ...],
    destinations: tuple[str, ...],
    start_date: date,
    days: int,
    step_days: int,
) -> list[SearchQuery]:
    queries: list[SearchQuery] = []
    for offset in range(0, days, step_days):
        depart_date = start_date + timedelta(days=offset)
        for origin in origins:
            for destination in destinations:
                queries.append(
                    SearchQuery(
                        origin=origin,
                        destination=destination,
                        depart_date=depart_date,
                    )
                )
    return queries


def summarize_spring_route_probes(results: list[SpringLiveProbeResult]) -> list[SpringRouteProbeSummary]:
    grouped: dict[str, list[SpringLiveProbeResult]] = {}
    for result in results:
        grouped.setdefault(result.query.route_key, []).append(result)

    summaries: list[SpringRouteProbeSummary] = []
    severity_rank = {"verified": 0, "blocked": 1, "failed": 2, "inconclusive": 3, "empty": 4}
    for route_key, route_results in grouped.items():
        verified_dates = tuple(
            result.query.depart_date.isoformat() for result in route_results if result.status == "verified"
        )
        if verified_dates:
            status = "verified"
            message = f"Verified on {verified_dates[0]}."
        else:
            statuses = {result.status for result in route_results}
            if statuses == {"empty"}:
                status = "empty"
            elif "blocked" in statuses:
                status = "blocked"
            elif "failed" in statuses:
                status = "failed"
            else:
                status = "inconclusive"
            ranked = sorted(route_results, key=lambda item: severity_rank.get(item.status, 9))
            message = ranked[0].message if ranked else ""
        summaries.append(
            SpringRouteProbeSummary(
                route_key=route_key,
                status=status,
                attempts=len(route_results),
                verified_dates=verified_dates,
                message=message,
            )
        )
    return sorted(summaries, key=lambda item: (item.status != "verified", item.route_key))


def probe_spring_routes(
    config: AppConfig,
    origins: tuple[str, ...],
    destinations: tuple[str, ...],
    start_date: date,
    days: int,
    step_days: int,
    logger: StageLogger | None = None,
) -> tuple[list[SpringLiveProbeResult], list[SpringRouteProbeSummary]]:
    adapter = SpringAirlinesAdapter(config=config, sample_mode=False)
    results: list[SpringLiveProbeResult] = []
    if logger is not None:
        logger.log(
            "probe",
            (
                f"start source=spring_airlines origins={','.join(origins)} "
                f"destinations={','.join(destinations)} days={days} step_days={step_days}"
            ),
        )
    for query in build_probe_queries(
        origins=origins,
        destinations=destinations,
        start_date=start_date,
        days=days,
        step_days=step_days,
    ):
        results.append(adapter.probe_live_query(query))
    summaries = summarize_spring_route_probes(results)
    if logger is not None:
        counts = {"verified": 0, "empty": 0, "failed": 0, "blocked": 0, "inconclusive": 0}
        for summary in summaries:
            counts[summary.status] = counts.get(summary.status, 0) + 1
        logger.log(
            "probe",
            (
                f"end verified={counts.get('verified', 0)} empty={counts.get('empty', 0)} "
                f"failed={counts.get('failed', 0)} blocked={counts.get('blocked', 0)} "
                f"inconclusive={counts.get('inconclusive', 0)}"
            ),
        )
    return results, summaries
