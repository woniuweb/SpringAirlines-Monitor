import os
import subprocess
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from fare_monitor.browser_agent import AgentBrowserClient, PLAYWRIGHT_EXTRACT_SCRIPT
from fare_monitor.cli import app
from fare_monitor.config import AppConfig
from fare_monitor.emailer import CollectionEmailBundle, build_collection_email_bundle, build_email_attachments, build_email_body, send_collection_email


class FakeSMTP:
    sent_messages = []

    def __init__(self, host: str, port: int, timeout: int = 30) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started_tls = False
        self.logged_in = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def starttls(self, context=None) -> None:
        self.started_tls = True

    def login(self, username: str, password: str) -> None:
        self.logged_in = (username, password)

    def send_message(self, message) -> None:
        self.__class__.sent_messages.append(message)


def test_agent_browser_client_uses_linux_fallback_and_headless(tmp_path) -> None:
    client = AgentBrowserClient(tmp_path, headless=True, executable_path="", channel="")
    with patch("fare_monitor.browser_agent.shutil.which", return_value="/usr/bin/chromium"):
        options = client._browser_launch_options()
    assert options["executable_path"] == "/usr/bin/chromium"
    assert options["headless"] is True


def test_agent_browser_client_runs_worker_via_module_entrypoint(tmp_path) -> None:
    client = AgentBrowserClient(tmp_path, headless=True, executable_path="", channel="")

    def fake_run(cmd, **kwargs):
        assert cmd[0]
        assert cmd[1:5] == ["-m", "fare_monitor", "--browser-worker", "extract"]
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout='{"title":"ok","url":"https://example.com","flights":[],"preview_days":[],"day_results":[]}',
            stderr="",
        )

    with patch("fare_monitor.browser_agent.shutil.which", return_value="/usr/bin/chromium"):
        with patch("fare_monitor.browser_agent.subprocess.run", side_effect=fake_run):
            payload = client._run_script(PLAYWRIGHT_EXTRACT_SCRIPT, ["https://example.com"])
    assert payload["title"] == "ok"


def test_send_collection_email_supports_multiple_recipients_and_attachments(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    config.email_enabled = True
    config.email_smtp_host = "smtp.example.com"
    config.email_smtp_port = 587
    config.email_smtp_username = "bot@example.com"
    config.email_smtp_password_env = "TEST_SMTP_PASSWORD"
    config.email_from_address = "bot@example.com"
    config.email_to_addresses = ("alice@example.com", "bob@example.com")
    config.email_subject_prefix = "[Fare Monitor]"
    config.email_smtp_use_tls = False
    config.email_smtp_use_ssl = False

    report_path = tmp_path / "report.html"
    qualified_csv = tmp_path / "qualified_fares.csv"
    run_log = tmp_path / "run.log"
    fares_csv = tmp_path / "fares.csv"
    report_path.write_text("<html></html>", encoding="utf-8")
    qualified_csv.write_text("a,b\n", encoding="utf-8")
    run_log.write_text("ok", encoding="utf-8")
    fares_csv.write_text("x,y\n", encoding="utf-8")

    bundle = CollectionEmailBundle(
        collection_id="collect-1",
        total_fares=10,
        qualified_fares=4,
        failed_sources=0,
        incomplete_sources=0,
        is_inconclusive=False,
        report_path=report_path,
        fares_csv=fares_csv,
        qualified_csv=qualified_csv,
        run_log_path=run_log,
        failure_details=[],
        top_qualified_rows=[
            {
                "origin": "TSN",
                "destination": "NRT",
                "depart_date": "2026-05-14",
                "depart_time": "08:15",
                "price_total_cny": 895.0,
                "carrier_display_name": "春秋航空 Spring Airlines",
                "source_display_name": "Spring Airlines 官网",
                "flight_no": "IJ254",
                "booking_url": "https://example.com/detail",
            }
        ],
    )

    with patch.dict(os.environ, {"TEST_SMTP_PASSWORD": "secret"}, clear=False):
        with patch("fare_monitor.emailer.smtplib.SMTP", FakeSMTP):
            FakeSMTP.sent_messages.clear()
            result = send_collection_email(config=config, bundle=bundle, dry_run=False, logger=None)

    assert result.status == "sent"
    assert result.recipients == ("alice@example.com", "bob@example.com")
    assert len(result.attachments) == 2
    assert len(FakeSMTP.sent_messages) == 1
    message = FakeSMTP.sent_messages[0]
    assert message["To"] == "alice@example.com, bob@example.com"
    body = message.get_body(preferencelist=("plain",))
    assert body is not None
    content = body.get_content()
    assert "机票监控日报" in content
    assert "最低价摘要" in content
    assert "895 元 | 2026-05-14 08:15" in content
    assert "report.html: 完整静态报表" in content
    assert "qualified_fares.csv: 所有符合阈值的已验证票价明细" in content


def test_send_collection_email_requires_password_env_when_username_is_set(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    config.email_enabled = True
    config.email_smtp_host = "smtp.example.com"
    config.email_smtp_username = "bot@example.com"
    config.email_smtp_password_env = "MISSING_SMTP_PASSWORD"
    config.email_from_address = "bot@example.com"
    config.email_to_addresses = ("alice@example.com",)

    bundle = CollectionEmailBundle(
        collection_id="collect-1",
        total_fares=1,
        qualified_fares=1,
        failed_sources=0,
        incomplete_sources=0,
        is_inconclusive=False,
        report_path=tmp_path / "report.html",
        fares_csv=tmp_path / "fares.csv",
        qualified_csv=tmp_path / "qualified.csv",
        run_log_path=None,
        failure_details=[],
        top_qualified_rows=[],
    )
    bundle.report_path.write_text("", encoding="utf-8")
    bundle.fares_csv.write_text("", encoding="utf-8")
    bundle.qualified_csv.write_text("", encoding="utf-8")

    with patch.dict(os.environ, {}, clear=True):
        try:
            send_collection_email(config=config, bundle=bundle, dry_run=False, logger=None)
        except RuntimeError as exc:
            assert "MISSING_SMTP_PASSWORD" in str(exc)
        else:
            raise AssertionError("Expected missing SMTP password error.")


def test_run_and_email_dry_run_sample_mode_succeeds(tmp_path) -> None:
    config_path = tmp_path / "fare-monitor.toml"
    config_path.write_text(
        """
[email]
enabled = true
smtp_host = "smtp.example.com"
smtp_port = 587
smtp_username = ""
smtp_password_env = "FARE_MONITOR_SMTP_PASSWORD"
from_address = "bot@example.com"
to_addresses = ["alice@example.com", "bob@example.com"]
subject_prefix = "[Fare Monitor]"
send_on_success = true
send_on_failure = true
attach_report_html = true
attach_qualified_csv = true
attach_run_log_on_failure = true
smtp_use_tls = false
smtp_use_ssl = false
""".strip(),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-and-email",
            "--base-dir",
            str(tmp_path),
            "--config",
            str(config_path),
            "--days",
            "7",
            "--sample-data",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "email_status=dry-run" in result.stdout
    assert "qualified_fares=" in result.stdout


def test_build_collection_email_bundle_reexports_collection_outputs(tmp_path) -> None:
    from fare_monitor.collector import collect
    from fare_monitor.stage_logging import StageLogger

    config = AppConfig.from_base_dir(tmp_path)
    config.ensure_dirs()
    logger = StageLogger(enabled=False, log_path=None)
    artifacts = collect(config=config, days=7, sample_mode=True, logger=logger)

    bundle = build_collection_email_bundle(config=config, collection_id=artifacts.collection_id)
    assert bundle.collection_id == artifacts.collection_id
    assert bundle.report_path.exists()
    assert bundle.fares_csv.exists()
    assert bundle.qualified_csv.exists()
    assert isinstance(bundle.top_qualified_rows, list)


def test_build_email_body_includes_failure_and_attachment_notes(tmp_path) -> None:
    config = AppConfig.from_base_dir(tmp_path)
    config.email_attach_report_html = True
    config.email_attach_qualified_csv = True
    config.email_attach_run_log_on_failure = True

    report_path = tmp_path / "report.html"
    qualified_csv = tmp_path / "qualified_fares.csv"
    run_log = tmp_path / "run.log"
    fares_csv = tmp_path / "fares.csv"
    report_path.write_text("<html></html>", encoding="utf-8")
    qualified_csv.write_text("a,b\n", encoding="utf-8")
    run_log.write_text("warn", encoding="utf-8")
    fares_csv.write_text("x,y\n", encoding="utf-8")

    bundle = CollectionEmailBundle(
        collection_id="collect-2",
        total_fares=5,
        qualified_fares=0,
        failed_sources=1,
        incomplete_sources=1,
        is_inconclusive=True,
        report_path=report_path,
        fares_csv=fares_csv,
        qualified_csv=qualified_csv,
        run_log_path=run_log,
        failure_details=["spring_airlines: partial blocked after 3 weeks"],
        top_qualified_rows=[],
    )

    attachments = build_email_attachments(config, bundle)
    body = build_email_body(bundle, attachments)
    assert "失败摘要" in body
    assert "spring_airlines: partial blocked after 3 weeks" in body
    assert "run.log: 本次运行阶段日志" in body
    assert "本次没有符合价格阈值的已验证票价" in body
