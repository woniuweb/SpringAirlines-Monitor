from __future__ import annotations

import mimetypes
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path

from fare_monitor.config import AppConfig
from fare_monitor.reporting import generate_report
from fare_monitor.stage_logging import StageLogger
from fare_monitor.storage import Storage


@dataclass
class CollectionEmailBundle:
    collection_id: str
    total_fares: int
    qualified_fares: int
    failed_sources: int
    incomplete_sources: int
    is_inconclusive: bool
    report_path: Path
    fares_csv: Path
    qualified_csv: Path
    run_log_path: Path | None
    failure_details: list[str]

    @property
    def is_failure(self) -> bool:
        return self.failed_sources > 0 or self.incomplete_sources > 0 or self.is_inconclusive


@dataclass
class EmailDispatchResult:
    status: str
    subject: str
    recipients: tuple[str, ...]
    attachments: tuple[Path, ...]
    message: str = ""


def _sort_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
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


def _qualified_rows(rows: list[dict[str, object]], threshold: float) -> list[dict[str, object]]:
    return _sort_rows(
        [
            row
            for row in rows
            if float(row["price_total_cny"]) < threshold and str(row.get("verification_status", "")) == "verified"
        ]
    )


def build_collection_email_bundle(
    config: AppConfig,
    collection_id: str | None = None,
    report_path: Path | None = None,
    fares_csv: Path | None = None,
    qualified_csv: Path | None = None,
    run_log_path: Path | None = None,
) -> CollectionEmailBundle:
    storage = Storage(config.database_path)
    resolved_collection_id = collection_id or storage.latest_collection_id()
    if resolved_collection_id is None:
        raise RuntimeError("No collection found. Run collect first.")

    all_rows = _sort_rows(storage.fares_for_collection(resolved_collection_id))
    source_rows = storage.source_runs_for_collection(resolved_collection_id)
    qualified_rows = _qualified_rows(all_rows, config.qualified_threshold)

    resolved_report_path = report_path or generate_report(config=config, collection_id=resolved_collection_id)
    resolved_fares_csv = fares_csv or (config.output_dir / "latest" / "fares.csv")
    resolved_qualified_csv = qualified_csv or (config.output_dir / "latest" / "qualified_fares.csv")
    storage.export_csv(resolved_fares_csv, all_rows)
    storage.export_csv(resolved_qualified_csv, qualified_rows)

    failed_sources = sum(1 for row in source_rows if str(row.get("status")) == "failed")
    incomplete_sources = sum(1 for row in source_rows if str(row.get("status")) in {"failed", "partial"})
    is_inconclusive = len(qualified_rows) == 0 and incomplete_sources > 0
    failure_details = [
        f"{row['source']}: {row['status']} {row['message']}"
        for row in source_rows
        if str(row.get("status")) in {"failed", "partial"}
    ]

    resolved_run_log = run_log_path or (config.log_file_path() if config.log_file_path().exists() else None)
    return CollectionEmailBundle(
        collection_id=resolved_collection_id,
        total_fares=len(all_rows),
        qualified_fares=len(qualified_rows),
        failed_sources=failed_sources,
        incomplete_sources=incomplete_sources,
        is_inconclusive=is_inconclusive,
        report_path=resolved_report_path,
        fares_csv=resolved_fares_csv,
        qualified_csv=resolved_qualified_csv,
        run_log_path=resolved_run_log,
        failure_details=failure_details,
    )


def send_collection_email(
    config: AppConfig,
    bundle: CollectionEmailBundle,
    dry_run: bool = False,
    logger: StageLogger | None = None,
) -> EmailDispatchResult:
    if not config.email_enabled:
        raise RuntimeError("Email delivery is disabled. Set [email].enabled = true in fare-monitor.toml.")

    if config.email_smtp_use_tls and config.email_smtp_use_ssl:
        raise RuntimeError("Only one of email.smtp_use_tls or email.smtp_use_ssl can be true.")

    recipients = tuple(address.strip() for address in config.email_to_addresses if address.strip())
    if not recipients:
        raise RuntimeError("No email recipients configured. Set [email].to_addresses.")

    if bundle.is_failure and not config.email_send_on_failure:
        return EmailDispatchResult(
            status="skipped",
            subject="",
            recipients=recipients,
            attachments=(),
            message="Failure email is disabled by configuration.",
        )
    if not bundle.is_failure and not config.email_send_on_success:
        return EmailDispatchResult(
            status="skipped",
            subject="",
            recipients=recipients,
            attachments=(),
            message="Success email is disabled by configuration.",
        )

    smtp_host = config.email_smtp_host.strip()
    if not smtp_host:
        raise RuntimeError("Missing email.smtp_host in configuration.")

    from_address = config.email_from_address.strip() or config.email_smtp_username.strip()
    if not from_address:
        raise RuntimeError("Missing email.from_address and email.smtp_username.")

    password = config.email_password()
    if config.email_smtp_username.strip() and not password:
        raise RuntimeError(
            f"Missing SMTP password in environment variable: {config.email_smtp_password_env or '(not configured)'}"
        )

    subject = build_email_subject(config, bundle)
    body = build_email_body(bundle)
    attachments = build_email_attachments(config, bundle)

    if logger is not None:
        logger.log("email", f"start collection_id={bundle.collection_id} recipients={','.join(recipients)} dry_run={dry_run}")

    if dry_run:
        result = EmailDispatchResult(
            status="dry-run",
            subject=subject,
            recipients=recipients,
            attachments=tuple(attachments),
            message="Email was composed but not sent.",
        )
        if logger is not None:
            logger.log("email", f"end status={result.status} attachments={len(attachments)}")
        return result

    message = EmailMessage()
    message["From"] = from_address
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.set_content(body)
    for attachment in attachments:
        if not attachment.exists():
            continue
        content_type, _ = mimetypes.guess_type(str(attachment))
        maintype, subtype = (content_type or "application/octet-stream").split("/", 1)
        with attachment.open("rb") as handle:
            message.add_attachment(handle.read(), maintype=maintype, subtype=subtype, filename=attachment.name)

    smtp_timeout = 30
    if config.email_smtp_use_ssl:
        client = smtplib.SMTP_SSL(smtp_host, config.email_smtp_port, timeout=smtp_timeout)
    else:
        client = smtplib.SMTP(smtp_host, config.email_smtp_port, timeout=smtp_timeout)

    with client:
        if config.email_smtp_use_tls:
            client.starttls(context=ssl.create_default_context())
        if config.email_smtp_username.strip():
            client.login(config.email_smtp_username.strip(), password)
        client.send_message(message)

    result = EmailDispatchResult(
        status="sent",
        subject=subject,
        recipients=recipients,
        attachments=tuple(attachments),
        message="Email sent successfully.",
    )
    if logger is not None:
        logger.log("email", f"end status={result.status} attachments={len(attachments)}")
    return result


def build_email_subject(config: AppConfig, bundle: CollectionEmailBundle) -> str:
    prefix = config.email_subject_prefix.strip()
    status = "ALERT" if bundle.is_failure else "Daily Report"
    return f"{prefix} {status} {bundle.collection_id}".strip()


def build_email_body(bundle: CollectionEmailBundle) -> str:
    lines = [
        f"collection_id: {bundle.collection_id}",
        f"total_fares: {bundle.total_fares}",
        f"qualified_fares: {bundle.qualified_fares}",
        f"failed_sources: {bundle.failed_sources}",
        f"incomplete_sources: {bundle.incomplete_sources}",
        f"is_inconclusive: {bundle.is_inconclusive}",
        f"report_path: {bundle.report_path}",
        f"fares_csv: {bundle.fares_csv}",
        f"qualified_csv: {bundle.qualified_csv}",
    ]
    if bundle.is_failure and bundle.failure_details:
        lines.append("")
        lines.append("failure_details:")
        lines.extend(f"- {item}" for item in bundle.failure_details[:5])
    return "\n".join(lines)


def build_email_attachments(config: AppConfig, bundle: CollectionEmailBundle) -> list[Path]:
    attachments: list[Path] = []
    if config.email_attach_report_html and bundle.report_path.exists():
        attachments.append(bundle.report_path)
    if config.email_attach_qualified_csv and bundle.qualified_csv.exists():
        attachments.append(bundle.qualified_csv)
    if bundle.is_failure and config.email_attach_run_log_on_failure and bundle.run_log_path and bundle.run_log_path.exists():
        attachments.append(bundle.run_log_path)
    return attachments
