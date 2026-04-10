from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import typer

from fare_monitor.collector import collect
from fare_monitor.config import AppConfig
from fare_monitor.emailer import build_collection_email_bundle, send_collection_email
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


@app.command("email-report")
def email_report_command(
    base_dir: Path = typer.Option(Path(__file__).resolve().parents[1], "--base-dir"),
    config_path: Path | None = typer.Option(None, "--config"),
    collection_id: str | None = typer.Option(None, "--collection-id"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    config = build_config(base_dir, config_path=config_path)
    logger = build_logger(config)
    try:
        bundle = build_collection_email_bundle(config=config, collection_id=collection_id)
        result = send_collection_email(config=config, bundle=bundle, dry_run=dry_run, logger=logger)
    except Exception as exc:
        if logger is not None:
            logger.log("email", f"failed collection_id={collection_id or 'latest'} error={exc}")
        typer.echo(f"email_status=failed")
        typer.echo(f"email_error={exc}")
        raise typer.Exit(code=1)
    typer.echo(f"collection_id={bundle.collection_id}")
    typer.echo(f"email_status={result.status}")
    typer.echo(f"qualified_fares={bundle.qualified_fares}")
    typer.echo(f"failed_sources={bundle.failed_sources}")
    typer.echo(f"incomplete_sources={bundle.incomplete_sources}")
    typer.echo(f"report={bundle.report_path}")
    typer.echo(f"qualified_csv={bundle.qualified_csv}")
    typer.echo(f"email_subject={result.subject}")
    typer.echo(f"email_recipients={','.join(result.recipients)}")
    typer.echo(f"email_attachments={','.join(str(path) for path in result.attachments)}")
    if result.message:
        typer.echo(f"email_message={result.message}")


@app.command("run-and-email")
def run_and_email_command(
    base_dir: Path = typer.Option(Path(__file__).resolve().parents[1], "--base-dir"),
    config_path: Path | None = typer.Option(None, "--config"),
    days: int | None = typer.Option(None, "--days", min=1, max=365),
    sample_data: bool = typer.Option(False, "--sample-data"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    config = build_config(base_dir, config_path=config_path)
    logger = build_logger(config)
    artifacts = collect(config=config, days=days, sample_mode=sample_data, logger=logger)
    logger.log("report", f"start collection_id={artifacts.collection_id}")
    report_path = generate_report(config=config, collection_id=artifacts.collection_id)
    logger.log("report", f"end path={report_path}")
    bundle = build_collection_email_bundle(
        config=config,
        collection_id=artifacts.collection_id,
        report_path=report_path,
        fares_csv=artifacts.fares_csv,
        qualified_csv=artifacts.qualified_csv,
        run_log_path=artifacts.log_path,
    )
    exit_code = 0
    try:
        result = send_collection_email(config=config, bundle=bundle, dry_run=dry_run, logger=logger)
    except Exception as exc:
        if logger is not None:
            logger.log("email", f"failed collection_id={artifacts.collection_id} error={exc}")
        typer.echo(f"collection_id={artifacts.collection_id}")
        typer.echo(f"qualified_fares={artifacts.qualified_fares}")
        typer.echo(f"failed_sources={artifacts.failed_sources}")
        typer.echo(f"incomplete_sources={artifacts.incomplete_sources}")
        typer.echo(f"is_inconclusive={artifacts.is_inconclusive}")
        typer.echo(f"report={report_path}")
        typer.echo("email_status=failed")
        typer.echo(f"email_error={exc}")
        raise typer.Exit(code=1)

    typer.echo(f"collection_id={artifacts.collection_id}")
    typer.echo(f"qualified_fares={artifacts.qualified_fares}")
    typer.echo(f"failed_sources={artifacts.failed_sources}")
    typer.echo(f"incomplete_sources={artifacts.incomplete_sources}")
    typer.echo(f"is_inconclusive={artifacts.is_inconclusive}")
    typer.echo(f"report={report_path}")
    typer.echo(f"run_log={artifacts.log_path}")
    typer.echo(f"email_status={result.status}")
    typer.echo(f"email_subject={result.subject}")
    typer.echo(f"email_recipients={','.join(result.recipients)}")
    typer.echo(f"email_attachments={','.join(str(path) for path in result.attachments)}")
    if artifacts.is_inconclusive or artifacts.failed_sources > 0 or artifacts.incomplete_sources > 0:
        exit_code = 1
    raise typer.Exit(code=exit_code)


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
