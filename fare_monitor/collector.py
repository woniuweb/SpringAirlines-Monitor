from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from fare_monitor.config import AppConfig
from fare_monitor.models import SearchQuery, SourceFetchResult, SourceRunSummary
from fare_monitor.sources import JetstarJapanAdapter, PeachAdapter, SpringAirlinesAdapter, SpringJapanAdapter
from fare_monitor.sources.base import SourceAdapter, save_payloads
from fare_monitor.stage_logging import StageLogger
from fare_monitor.storage import Storage
from fare_monitor.utils import slugify, utc_now_iso


@dataclass
class CollectionArtifacts:
    collection_id: str
    total_fares: int
    qualified_fares: int
    unverified_fares: int
    failed_sources: int
    incomplete_sources: int
    is_inconclusive: bool
    fares_csv: Path
    qualified_csv: Path
    unverified_csv: Path | None = None
    log_path: Path | None = None


@dataclass(frozen=True)
class SpringRouteTask:
    origin: str
    destination: str
    start_date: date
    end_date: date
    collection_id: str

    @property
    def route_key(self) -> str:
        return f"{self.origin}->{self.destination}"

    @property
    def depart_date(self) -> date:
        return self.start_date


def build_adapters(config: AppConfig, sample_mode: bool) -> list[SourceAdapter]:
    return [
        SpringAirlinesAdapter(config=config, sample_mode=sample_mode),
        SpringJapanAdapter(config=config, sample_mode=sample_mode),
        PeachAdapter(config=config, sample_mode=sample_mode),
        JetstarJapanAdapter(config=config, sample_mode=sample_mode),
    ]


def sort_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        rows,
        key=lambda row: (
            float(row["price_total_cny"]),
            str(row["depart_date"]),
            str(row["depart_time"]),
            str(row.get("carrier_display_name", "")),
            str(row.get("source_display_name", "")),
        ),
    )


def _result_is_blocked(result: SourceFetchResult) -> bool:
    message = result.message.lower()
    return (
        "temporarily blocking automated requests" in message
        or "405" in message
        or "not allowed" in message
        or "访问被阻断" in result.message
        or "blocked" in message
    )


def _qualified_rows(rows: list[dict[str, object]], threshold: float) -> list[dict[str, object]]:
    return sort_rows(
        [
            row
            for row in rows
            if float(row["price_total_cny"]) < threshold and row.get("verification_status") == "verified"
        ]
    )


def _search_query_worker(
    adapter_cls: type[SourceAdapter],
    config: AppConfig,
    query,
    collection_id: str,
) -> SourceFetchResult:
    adapter = adapter_cls(config=config, sample_mode=False)
    try:
        return adapter.search(query=query, collection_id=collection_id)
    except Exception as exc:
        return SourceFetchResult(
            source=adapter_cls.source_name,
            fares=[],
            status="failed",
            message=f"Unhandled worker error: {exc}",
        )


def _search_route_worker(
    adapter_cls: type[SpringAirlinesAdapter],
    config: AppConfig,
    task: SpringRouteTask,
) -> SourceFetchResult:
    adapter = adapter_cls(config=config, sample_mode=False)
    try:
        return adapter.scan_route_live(
            origin=task.origin,
            destination=task.destination,
            start_date=task.start_date,
            end_date=task.end_date,
            collection_id=task.collection_id,
        )
    except Exception as exc:
        return SourceFetchResult(
            source=adapter_cls.source_name,
            fares=[],
            status="failed",
            message=f"Unhandled route worker error: {exc}",
        )


def _worker_count_for_source(adapter: SourceAdapter, config: AppConfig, sample_mode: bool) -> int:
    if sample_mode:
        return 1
    if adapter.source_name == "spring_airlines":
        return max(1, config.spring_live_workers)
    return 1


def _summarize_source_status(source_fares: list, had_failures: bool) -> str:
    if source_fares and had_failures:
        return "partial"
    if source_fares:
        return "ok"
    if had_failures:
        return "failed"
    return "empty"


def build_spring_route_tasks(
    adapter: SpringAirlinesAdapter,
    queries: list[SearchQuery],
    collection_id: str,
) -> list[SpringRouteTask]:
    supported = [query for query in queries if adapter.supports_query(query)]
    grouped: dict[str, list[SearchQuery]] = {}
    for query in supported:
        grouped.setdefault(query.route_key, []).append(query)

    tasks: list[SpringRouteTask] = []
    for route_key in sorted(grouped):
        route_queries = sorted(grouped[route_key], key=lambda item: item.depart_date)
        if not route_queries:
            continue
        tasks.append(
            SpringRouteTask(
                origin=route_queries[0].origin,
                destination=route_queries[0].destination,
                start_date=route_queries[0].depart_date,
                end_date=route_queries[-1].depart_date,
                collection_id=collection_id,
            )
        )
    return tasks


def _collect_serial_source(
    adapter: SourceAdapter,
    source_queries: list,
    collection_id: str,
    config: AppConfig,
) -> tuple[list, int, list[str], dict[str, int], bool, dict[str, int]]:
    source_fares = []
    source_messages: list[str] = []
    counts = {"ok": 0, "empty": 0, "failed": 0, "skipped": 0}
    aggregate_stats: dict[str, int] = {}
    blocked = False
    attempted_queries = 0
    for query in source_queries:
        attempted_queries += 1
        result = adapter.search(query=query, collection_id=collection_id)
        source_fares.extend(result.fares)
        if result.payloads:
            save_payloads(config.raw_dir, adapter.source_name, collection_id, query, result.payloads)
        if result.message:
            source_messages.append(f"{query.route_key} {query.depart_date.isoformat()}: {result.message}")
        counts[result.status] = counts.get(result.status, 0) + 1
        for key, value in result.stats.items():
            aggregate_stats[key] = aggregate_stats.get(key, 0) + int(value)
        if _result_is_blocked(result):
            blocked = True
        if result.status == "skipped" and "Remaining queries for this source were skipped" in result.message:
            break
    return source_fares, attempted_queries, source_messages, counts, blocked, aggregate_stats


def _collect_concurrent_source(
    adapter: SourceAdapter,
    source_queries: list,
    collection_id: str,
    config: AppConfig,
) -> tuple[list, int, list[str], dict[str, int], bool, dict[str, int]]:
    source_fares = []
    source_messages: list[str] = []
    counts = {"ok": 0, "empty": 0, "failed": 0, "skipped": 0}
    aggregate_stats: dict[str, int] = {}
    blocked = False
    attempted_queries = 0
    pending_queries = iter(source_queries)
    futures: dict[Future[SourceFetchResult], object] = {}
    worker_count = _worker_count_for_source(adapter, config, sample_mode=False)

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=adapter.source_name) as executor:
        while len(futures) < worker_count:
            try:
                query = next(pending_queries)
            except StopIteration:
                break
            futures[executor.submit(_search_query_worker, type(adapter), config, query, collection_id)] = query
            attempted_queries += 1

        while futures:
            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                query = futures.pop(future)
                result = future.result()
                source_fares.extend(result.fares)
                if result.payloads:
                    save_payloads(config.raw_dir, adapter.source_name, collection_id, query, result.payloads)
                if result.message:
                    source_messages.append(f"{query.route_key} {query.depart_date.isoformat()}: {result.message}")
                counts[result.status] = counts.get(result.status, 0) + 1
                for key, value in result.stats.items():
                    aggregate_stats[key] = aggregate_stats.get(key, 0) + int(value)
                if _result_is_blocked(result):
                    blocked = True
                if blocked:
                    continue
                try:
                    next_query = next(pending_queries)
                except StopIteration:
                    continue
                futures[executor.submit(_search_query_worker, type(adapter), config, next_query, collection_id)] = next_query
                attempted_queries += 1

    return source_fares, attempted_queries, source_messages, counts, blocked, aggregate_stats


def _collect_spring_route_source(
    adapter: SpringAirlinesAdapter,
    route_tasks: list[SpringRouteTask],
    config: AppConfig,
) -> tuple[list, int, list[str], dict[str, int], bool, dict[str, int]]:
    source_fares = []
    source_messages: list[str] = []
    counts = {"ok": 0, "empty": 0, "failed": 0, "skipped": 0, "partial": 0}
    aggregate_stats: dict[str, int] = {}
    blocked = False
    attempted_routes = 0
    pending_tasks = iter(route_tasks)
    futures: dict[Future[SourceFetchResult], SpringRouteTask] = {}
    worker_count = _worker_count_for_source(adapter, config, sample_mode=False)

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=f"{adapter.source_name}-route") as executor:
        while len(futures) < worker_count:
            try:
                task = next(pending_tasks)
            except StopIteration:
                break
            futures[executor.submit(_search_route_worker, type(adapter), config, task)] = task
            attempted_routes += 1

        while futures:
            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                task = futures.pop(future)
                result = future.result()
                source_fares.extend(result.fares)
                if result.payloads:
                    save_payloads(config.raw_dir, adapter.source_name, task.collection_id, task, result.payloads)
                if result.message and result.message != "ok":
                    source_messages.append(f"{task.route_key} {task.start_date.isoformat()}..{task.end_date.isoformat()}: {result.message}")
                counts[result.status] = counts.get(result.status, 0) + 1
                for key, value in result.stats.items():
                    aggregate_stats[key] = aggregate_stats.get(key, 0) + int(value)
                if _result_is_blocked(result):
                    blocked = True
                if blocked:
                    continue
                try:
                    next_task = next(pending_tasks)
                except StopIteration:
                    continue
                futures[executor.submit(_search_route_worker, type(adapter), config, next_task)] = next_task
                attempted_routes += 1

    return source_fares, attempted_routes, source_messages, counts, blocked, aggregate_stats


def collect(
    config: AppConfig,
    days: int | None = None,
    sample_mode: bool = False,
    logger: StageLogger | None = None,
) -> CollectionArtifacts:
    config.ensure_dirs()
    storage = Storage(config.database_path)
    storage.initialize()
    queries = config.build_queries(days=days)
    collection_id = slugify(utc_now_iso())
    all_fares = []
    summaries: list[SourceRunSummary] = []
    active_sources = [
        adapter.source_name
        for adapter in build_adapters(config=config, sample_mode=sample_mode)
        if adapter.is_live_enabled()
    ]

    if logger is not None:
        logger.log(
            "run",
            (
                f"start collection_id={collection_id} mode={'sample' if sample_mode else 'live'} "
                f"days={days or config.scan_days} config={config.config_label()} "
                f"sources={','.join(active_sources) or '-'}"
            ),
        )

    for adapter in build_adapters(config=config, sample_mode=sample_mode):
        started_at = utc_now_iso()
        worker_count = _worker_count_for_source(adapter, config, sample_mode)
        if not adapter.is_live_enabled():
            message = adapter.live_skip_reason or "Live collection is disabled for this source."
            summaries.append(
                SourceRunSummary(
                    collection_id=collection_id,
                    source=adapter.source_name,
                    queried_routes=0,
                    fare_count=0,
                    status="skipped",
                    message=message,
                    started_at=started_at,
                    finished_at=utc_now_iso(),
                )
            )
            if logger is not None:
                logger.log("source", f"{adapter.source_name} skipped reason={message}")
            continue

        if not sample_mode and adapter.source_name == "spring_airlines":
            route_tasks = build_spring_route_tasks(adapter, queries, collection_id)
            if not route_tasks:
                message = "No verified Spring live routes matched the configured origin/destination scope."
                summaries.append(
                    SourceRunSummary(
                        collection_id=collection_id,
                        source=adapter.source_name,
                        queried_routes=0,
                        fare_count=0,
                        status="skipped",
                        message=message,
                        started_at=started_at,
                        finished_at=utc_now_iso(),
                    )
                )
                if logger is not None:
                    logger.log("source", f"{adapter.source_name} skipped reason={message}")
                continue
            if logger is not None:
                logger.log(
                    "source",
                    f"{adapter.source_name} start routes={len(route_tasks)} workers={worker_count} mode=route-session",
                )
            source_fares, attempted_queries, source_messages, counts, blocked, aggregate_stats = _collect_spring_route_source(
                adapter=adapter,
                route_tasks=route_tasks,
                config=config,
            )
        else:
            source_queries = adapter.filter_queries(queries)
            if not source_queries:
                message = "No official routes discovered for the configured origin/destination scope."
                summaries.append(
                    SourceRunSummary(
                        collection_id=collection_id,
                        source=adapter.source_name,
                        queried_routes=0,
                        fare_count=0,
                        status="skipped",
                        message=message,
                        started_at=started_at,
                        finished_at=utc_now_iso(),
                    )
                )
                if logger is not None:
                    logger.log("source", f"{adapter.source_name} skipped reason={message}")
                continue

            if logger is not None:
                logger.log(
                    "source",
                    f"{adapter.source_name} start queries={len(source_queries)} workers={worker_count}",
                )

            if worker_count > 1:
                source_fares, attempted_queries, source_messages, counts, blocked, aggregate_stats = _collect_concurrent_source(
                    adapter=adapter,
                    source_queries=source_queries,
                    collection_id=collection_id,
                    config=config,
                )
            else:
                source_fares, attempted_queries, source_messages, counts, blocked, aggregate_stats = _collect_serial_source(
                    adapter=adapter,
                    source_queries=source_queries,
                    collection_id=collection_id,
                    config=config,
                )

        had_failures = counts.get("failed", 0) > 0 or counts.get("partial", 0) > 0
        status = _summarize_source_status(source_fares, had_failures)
        all_fares.extend(source_fares)
        message = " | ".join(source_messages[:5]) if source_messages else "ok"
        summaries.append(
            SourceRunSummary(
                collection_id=collection_id,
                source=adapter.source_name,
                queried_routes=attempted_queries,
                fare_count=len(source_fares),
                status=status,
                message=message,
                started_at=started_at,
                finished_at=utc_now_iso(),
            )
        )
        if logger is not None:
            stat_bits = []
            for key in (
                "windows",
                "routes",
                "weeks_scanned",
                "preview_days",
                "clicked_dates",
                "written_fares",
                "empty_weeks",
                "consecutive_empty_weeks",
                "blocked_routes",
                "blocked_after_progress",
                "mismatch_routes",
            ):
                if key in aggregate_stats:
                    stat_bits.append(f"{key}={aggregate_stats[key]}")
            logger.log(
                "source",
                (
                    f"{adapter.source_name} progress ok={counts.get('ok', 0)} empty={counts.get('empty', 0)} "
                    f"failed={counts.get('failed', 0)} partial={counts.get('partial', 0)} "
                    f"blocked={'yes' if blocked else 'no'}"
                    + (f" {' '.join(stat_bits)}" if stat_bits else "")
                ),
            )
            counter_label = "routes" if (not sample_mode and adapter.source_name == "spring_airlines") else "queried"
            logger.log(
                "source",
                f"{adapter.source_name} end status={status} fares={len(source_fares)} {counter_label}={attempted_queries}",
            )

    storage.insert_fares(all_fares)
    storage.insert_source_runs(summaries)

    fares_rows = sort_rows([fare.as_dict() for fare in all_fares])
    qualified_rows = _qualified_rows(fares_rows, config.qualified_threshold)
    unverified_rows = sort_rows([row for row in fares_rows if row.get("verification_status") != "verified"])

    latest_dir = config.output_dir / "latest"
    fares_csv = latest_dir / "fares.csv"
    qualified_csv = latest_dir / "qualified_fares.csv"
    unverified_csv = latest_dir / "unverified_fares_debug.csv"

    storage.export_csv(fares_csv, fares_rows)
    storage.export_csv(qualified_csv, qualified_rows)
    if unverified_rows:
        storage.export_csv(unverified_csv, unverified_rows)
    elif unverified_csv.exists():
        unverified_csv.unlink()
        unverified_csv = None
    else:
        unverified_csv = None

    failed_sources = sum(1 for summary in summaries if summary.status == "failed")
    incomplete_sources = sum(1 for summary in summaries if summary.status in {"failed", "partial"})
    is_inconclusive = len(qualified_rows) == 0 and incomplete_sources > 0

    if logger is not None:
        logger.log(
            "run",
            (
                f"end collection_id={collection_id} total_fares={len(all_fares)} "
                f"qualified_fares={len(qualified_rows)} failed_sources={failed_sources} "
                f"incomplete_sources={incomplete_sources} inconclusive={is_inconclusive}"
            ),
        )

    return CollectionArtifacts(
        collection_id=collection_id,
        total_fares=len(all_fares),
        qualified_fares=len(qualified_rows),
        unverified_fares=len(unverified_rows),
        failed_sources=failed_sources,
        incomplete_sources=incomplete_sources,
        is_inconclusive=is_inconclusive,
        fares_csv=fares_csv,
        qualified_csv=qualified_csv,
        unverified_csv=unverified_csv,
        log_path=config.log_file_path() if config.logging_enabled else None,
    )
