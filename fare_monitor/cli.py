from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import typer

from fare_monitor.collector import collect
from fare_monitor.config import AppConfig
from fare_monitor.probe import parse_code_list, probe_spring_routes
from fare_monitor.reporting import generate_report
from fare_monitor.stage_logging import StageLogger

app = typer.Typer(add_completion=False, no_args_is_help=True)


def build_config(base_dir: Path, config_path: Path | None = None) -> AppConfig:
    config = AppConfig.from_base_dir(base_dir, config_path=config_path)
    config.ensure_dirs()
    return config


def build_logger(config: AppConfig) -> StageLogger:
    enabled = config.logging_enabled and config.stage_summary
    return StageLogger(enabled=enabled, log_path=config.log_file_path() if enabled else None)


@app.command("collect")
def collect_command(
    base_dir: Path = typer.Option(Path(__file__).resolve().parents[1], "--base-dir"),
    config_path: Path | None = typer.Option(None, "--config"),
    days: int | None = typer.Option(None, "--days", min=1, max=365),
    sample_data: bool = typer.Option(False, "--sample-data"),
) -> None:
    config = build_config(base_dir, config_path=config_path)
    logger = build_logger(config)
    artifacts = collect(config=config, days=days, sample_mode=sample_data, logger=logger)
    typer.echo(f"collection_id={artifacts.collection_id}")
    typer.echo(f"total_fares={artifacts.total_fares}")
    typer.echo(f"qualified_fares={artifacts.qualified_fares}")
    typer.echo(f"unverified_fares={artifacts.unverified_fares}")
    typer.echo(f"failed_sources={artifacts.failed_sources}")
    typer.echo(f"incomplete_sources={artifacts.incomplete_sources}")
    typer.echo(f"is_inconclusive={artifacts.is_inconclusive}")
    typer.echo(f"fares_csv={artifacts.fares_csv}")
    typer.echo(f"qualified_csv={artifacts.qualified_csv}")
    if artifacts.unverified_csv:
        typer.echo(f"unverified_csv={artifacts.unverified_csv}")
    if artifacts.log_path:
        typer.echo(f"run_log={artifacts.log_path}")
    if artifacts.is_inconclusive:
        typer.echo("warning=This run is inconclusive because one or more live sources failed or were blocked.")


@app.command()
def report(
    base_dir: Path = typer.Option(Path(__file__).resolve().parents[1], "--base-dir"),
    config_path: Path | None = typer.Option(None, "--config"),
    collection_id: str | None = typer.Option(None, "--collection-id"),
) -> None:
    config = build_config(base_dir, config_path=config_path)
    logger = build_logger(config)
    logger.log("report", f"start collection_id={collection_id or 'latest'} config={config.config_label()}")
    report_path = generate_report(config=config, collection_id=collection_id)
    logger.log("report", f"end path={report_path}")
    typer.echo(f"report={report_path}")
    if config.logging_enabled and config.stage_summary:
        typer.echo(f"run_log={config.log_file_path()}")


@app.command()
def run(
    base_dir: Path = typer.Option(Path(__file__).resolve().parents[1], "--base-dir"),
    config_path: Path | None = typer.Option(None, "--config"),
    days: int | None = typer.Option(None, "--days", min=1, max=365),
    sample_data: bool = typer.Option(False, "--sample-data"),
) -> None:
    config = build_config(base_dir, config_path=config_path)
    logger = build_logger(config)
    artifacts = collect(config=config, days=days, sample_mode=sample_data, logger=logger)
    logger.log("report", f"start collection_id={artifacts.collection_id}")
    report_path = generate_report(config=config, collection_id=artifacts.collection_id)
    logger.log("report", f"end path={report_path}")
    typer.echo(f"collection_id={artifacts.collection_id}")
    typer.echo(f"qualified_fares={artifacts.qualified_fares}")
    typer.echo(f"unverified_fares={artifacts.unverified_fares}")
    typer.echo(f"failed_sources={artifacts.failed_sources}")
    typer.echo(f"incomplete_sources={artifacts.incomplete_sources}")
    typer.echo(f"is_inconclusive={artifacts.is_inconclusive}")
    typer.echo(f"report={report_path}")
    if artifacts.log_path:
        typer.echo(f"run_log={artifacts.log_path}")
    if artifacts.is_inconclusive:
        typer.echo("warning=This run is inconclusive because one or more live sources failed or were blocked.")


@app.command("probe-spring")
def probe_spring_command(
    base_dir: Path = typer.Option(Path(__file__).resolve().parents[1], "--base-dir"),
    config_path: Path | None = typer.Option(None, "--config"),
    origins: str | None = typer.Option(None, "--origins", help="Comma-separated IATA codes."),
    destinations: str | None = typer.Option(None, "--destinations", help="Comma-separated IATA codes."),
    days: int | None = typer.Option(None, "--days", min=1, max=365),
    step_days: int | None = typer.Option(None, "--step-days", min=1, max=60),
    start_date: str | None = typer.Option(None, "--start-date", help="YYYY-MM-DD. Defaults to tomorrow."),
) -> None:
    config = build_config(base_dir, config_path=config_path)
    logger = build_logger(config)
    actual_origins = parse_code_list(origins, config.origins)
    actual_destinations = parse_code_list(destinations, config.destinations)
    actual_days = days or config.scan_days
    actual_step_days = step_days or config.probe_step_days
    actual_start_date = date.fromisoformat(start_date) if start_date else (date.today() + timedelta(days=1))
    _, summaries = probe_spring_routes(
        config=config,
        origins=actual_origins,
        destinations=actual_destinations,
        start_date=actual_start_date,
        days=actual_days,
        step_days=actual_step_days,
        logger=logger,
    )
    typer.echo(f"probe_start_date={actual_start_date.isoformat()}")
    typer.echo(f"probe_days={actual_days}")
    typer.echo(f"probe_step_days={actual_step_days}")
    for summary in summaries:
        verified_text = ",".join(summary.verified_dates[:3]) if summary.verified_dates else "-"
        typer.echo(
            f"{summary.route_key}\tstatus={summary.status}\tattempts={summary.attempts}\tverified_dates={verified_text}\t{summary.message}"
        )
    verified = [summary.route_key for summary in summaries if summary.status == "verified"]
    typer.echo(f"verified_route_keys={','.join(sorted(verified))}")
    if config.logging_enabled and config.stage_summary:
        typer.echo(f"run_log={config.log_file_path()}")
