from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


class BrowserUnavailableError(RuntimeError):
    pass


@dataclass
class BrowserSpringPage:
    title: str
    url: str
    total_text: str
    body_text: str
    flights: list[dict[str, str]]
    raw_payload: str
    final_url: str = ""
    blocked: bool = False
    blocked_after_progress: bool = False
    route_mismatch: bool = False
    weeks_scanned: int = 0
    empty_weeks: int = 0
    consecutive_empty_weeks: int = 0
    fail_samples: list[str] = field(default_factory=list)
    preview_days: list[dict[str, object]] = field(default_factory=list)
    day_results: list[dict[str, object]] = field(default_factory=list)


PLAYWRIGHT_EXTRACT_SCRIPT = r'''
import json
import sys
from playwright.sync_api import sync_playwright

browser_options = json.loads(sys.argv[1])
url = sys.argv[2]

script = r"""
() => {
  const text = (root, selector) => {
    const node = root.querySelector(selector);
    return node ? (node.innerText || node.textContent || '').trim() : '';
  };
  const flights = [...document.querySelectorAll('[data-route-index]')].map((row) => ({
    carrier: text(row, '.c-company'),
    flight_no: text(row, '.c-flight-no').replace(/\s+/g, ' ').trim(),
    depart_time: text(row, '.td-left .tm'),
    arrive_time: text(row, '.td-right .tm'),
    depart_airport: text(row, '.td-left .local'),
    arrive_airport: text(row, '.td-right .local'),
    duration: text(row, '.td-center .timebox'),
    price_text: text(row, '.td-price-item'),
    row_text: (row.innerText || '').trim(),
  })).filter((item) => item.flight_no && item.price_text);
  return {
    title: document.title || '',
    url: location.href || '',
    total_text: text(document, '.J-total'),
    body_text: (document.body && document.body.innerText ? document.body.innerText : '').trim(),
    flights,
  };
}
"""

def launch_browser(playwright):
    launch_kwargs = {
        'headless': bool(browser_options.get('headless', True)),
        'args': ['--disable-blink-features=AutomationControlled'],
    }
    executable_path = browser_options.get('executable_path') or None
    channel = browser_options.get('channel') or None
    if executable_path:
        launch_kwargs['executable_path'] = executable_path
    elif channel:
        launch_kwargs['channel'] = channel
    return playwright.chromium.launch(**launch_kwargs)


with sync_playwright() as p:
    browser = launch_browser(p)
    context = browser.new_context(locale='en-US')
    page = context.new_page()
    page.set_extra_http_headers({'Accept-Language': 'en-US,en;q=0.9'})
    page.goto(url, wait_until='domcontentloaded', timeout=120000)
    try:
        page.wait_for_selector('[data-route-index], .flight-list, .J-date-picker', timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(2500)
    payload = page.evaluate(script)
    print(json.dumps(payload, ensure_ascii=False))
    context.close()
    browser.close()
'''


PLAYWRIGHT_WINDOW_SCRIPT = r'''
import json
import re
import sys
from playwright.sync_api import sync_playwright

browser_options = json.loads(sys.argv[1])
url = sys.argv[2]
threshold = float(sys.argv[3])
fx_to_cny = json.loads(sys.argv[4])

PRICE_PATTERN = re.compile(r"(CNY|USD|JPY|HKD|TWD|SGD|THB|KRW|MOP)\s*([0-9,]+(?:\.\d+)?)", re.I)
DATE_PATTERN = re.compile(r"(\d{1,2}\s+[A-Za-z]{3},\s+[A-Za-z]{3})")


def clean(value):
    return " ".join((value or "").split())


def parse_price(text):
    match = PRICE_PATTERN.search(text or "")
    if not match:
        return None
    currency = match.group(1).upper()
    amount = float(match.group(2).replace(",", ""))
    rate = fx_to_cny.get(currency)
    if rate is None:
        return None
    return {
        "currency": currency,
        "amount": amount,
        "amount_text": match.group(2),
        "price_total_cny": amount * float(rate),
    }


def flight_to_dict(row):
    def text(selector):
        try:
            return clean(row.locator(selector).first.inner_text())
        except Exception:
            return ""

    try:
        row_text = clean(row.inner_text())
    except Exception:
        row_text = ""
    return {
        "carrier": text(".c-company"),
        "flight_no": clean(text(".c-flight-no").replace("(Share)", "")),
        "depart_time": text(".td-left .tm"),
        "arrive_time": text(".td-right .tm"),
        "depart_airport": text(".td-left .local"),
        "arrive_airport": text(".td-right .local"),
        "duration": text(".td-center .timebox"),
        "price_text": text(".td-price-item"),
        "row_text": row_text,
    }


def collect_flights(page):
    flights = []
    rows = page.locator("[data-route-index]")
    count = rows.count()
    for index in range(count):
        item = flight_to_dict(rows.nth(index))
        if item["flight_no"] and item["price_text"]:
            flights.append(item)
    return flights


def collect_preview_items(page):
    items = []
    seen_labels = set()
    nodes = page.locator(".J-date-picker li")
    count = nodes.count()
    for index in range(count):
        node = nodes.nth(index)
        try:
            text = clean(node.inner_text())
        except Exception:
            continue
        date_match = DATE_PATTERN.search(text)
        if not date_match:
            continue
        price_info = parse_price(text)
        if price_info is None:
            continue
        label = date_match.group(1)
        if label in seen_labels:
            continue
        seen_labels.add(label)
        classes = clean((node.get_attribute("class") or "") + " " + ((node.locator("a").first.get_attribute("class") or "") if node.locator("a").count() else ""))
        items.append(
            {
                "preview_index": len(items),
                "dom_index": index,
                "label": label,
                "price_text": f"{price_info['currency']}{price_info['amount_text']}",
                "price_total_cny": price_info["price_total_cny"],
                "currency": price_info["currency"],
                "price_original": price_info["amount"],
                "is_selected": any(token in classes.lower() for token in ("active", "selected", "current", "cur", "on")),
            }
        )
    return items


def click_preview(page, target):
    dom_nodes = page.locator(".J-date-picker li")
    count = dom_nodes.count()
    for index in range(count):
        node = dom_nodes.nth(index)
        try:
            text = clean(node.inner_text())
        except Exception:
            continue
        if target["label"] in text:
            node.click(timeout=10000)
            page.wait_for_timeout(1800)
            return True
    return False


def launch_browser(playwright):
    launch_kwargs = {
        "headless": bool(browser_options.get("headless", True)),
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    executable_path = browser_options.get("executable_path") or None
    channel = browser_options.get("channel") or None
    if executable_path:
        launch_kwargs["executable_path"] = executable_path
    elif channel:
        launch_kwargs["channel"] = channel
    return playwright.chromium.launch(**launch_kwargs)


with sync_playwright() as p:
    browser = launch_browser(p)
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    try:
        page.wait_for_selector("[data-route-index], .flight-list, .J-date-picker", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(2500)

    preview_days = collect_preview_items(page)
    day_results = []
    for target in preview_days:
        if float(target["price_total_cny"]) >= threshold:
            continue
        clicked = click_preview(page, target)
        flights = collect_flights(page)
        day_results.append(
            {
                "preview_index": target["preview_index"],
                "label": target["label"],
                "page_url": page.url,
                "clicked": clicked,
                "flights": flights,
            }
        )

    payload = {
        "title": page.title(),
        "url": page.url,
        "total_text": "",
        "body_text": clean(page.locator("body").inner_text()),
        "flights": collect_flights(page),
        "preview_days": preview_days,
        "day_results": day_results,
    }
    print(json.dumps(payload, ensure_ascii=False))
    context.close()
    browser.close()
'''


PLAYWRIGHT_ROUTE_SCAN_SCRIPT = r'''
import json
import re
import sys
from datetime import date, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from playwright.sync_api import sync_playwright

browser_options = json.loads(sys.argv[1])
url = sys.argv[2]
route_key = sys.argv[3]
start_date = date.fromisoformat(sys.argv[4])
end_date = date.fromisoformat(sys.argv[5])
threshold = float(sys.argv[6])
window_days = int(sys.argv[7])
max_empty_weeks = int(sys.argv[8])
fx_to_cny = json.loads(sys.argv[9])

PRICE_PATTERN = re.compile(r"(CNY|USD|JPY|HKD|TWD|SGD|THB|KRW|MOP)\s*([0-9,]+(?:\.\d+)?)", re.I)
DATE_PATTERN = re.compile(r"(\d{1,2}\s+[A-Za-z]{3},\s+[A-Za-z]{3})")
BLOCK_PATTERNS = (
    "访问被阻断",
    "安全威胁",
    "access denied",
    "request blocked",
    "temporarily blocked",
)


def clean(value):
    return " ".join((value or "").split())


def parse_price(text):
    match = PRICE_PATTERN.search(text or "")
    if not match:
        return None
    currency = match.group(1).upper()
    amount = float(match.group(2).replace(",", ""))
    rate = fx_to_cny.get(currency)
    if rate is None:
        return None
    return {
        "currency": currency,
        "amount": amount,
        "amount_text": match.group(2),
        "price_total_cny": amount * float(rate),
    }


def flight_to_dict(row):
    def text(selector):
        try:
            return clean(row.locator(selector).first.inner_text())
        except Exception:
            return ""

    try:
        row_text = clean(row.inner_text())
    except Exception:
        row_text = ""
    return {
        "carrier": text(".c-company"),
        "flight_no": clean(text(".c-flight-no").replace("(Share)", "")),
        "depart_time": text(".td-left .tm"),
        "arrive_time": text(".td-right .tm"),
        "depart_airport": text(".td-left .local"),
        "arrive_airport": text(".td-right .local"),
        "duration": text(".td-center .timebox"),
        "price_text": text(".td-price-item"),
        "row_text": row_text,
    }


def collect_flights(page):
    flights = []
    rows = page.locator("[data-route-index]")
    count = rows.count()
    for index in range(count):
        item = flight_to_dict(rows.nth(index))
        if item["flight_no"] and item["price_text"]:
            flights.append(item)
    return flights


def detect_block(page):
    body_text = clean(page.locator("body").inner_text()) if page.locator("body").count() else ""
    title = clean(page.title())
    combined = f"{title} {body_text}".lower()
    if title == "405":
        return True, "page title returned 405"
    for pattern in BLOCK_PATTERNS:
        if pattern.lower() in combined:
            return True, pattern
    return False, ""


def infer_date(label, anchor):
    base = datetime.strptime(label, "%d %b, %a")
    candidates = []
    for year in {anchor.year - 1, anchor.year, anchor.year + 1}:
        try:
            candidates.append(date(year, base.month, base.day))
        except ValueError:
            continue
    if not candidates:
        return None
    return min(candidates, key=lambda candidate: abs((candidate - anchor).days))


def current_route_slug(page):
    match = re.search(r"/flights/([A-Z]{3}-[A-Z]{3})\.html", page.url or "")
    return match.group(1) if match else ""


def collect_preview_items(page, anchor):
    items = []
    seen_dates = set()
    nodes = page.locator(".J-date-picker li")
    count = nodes.count()
    for index in range(count):
        node = nodes.nth(index)
        try:
            text = clean(node.inner_text())
        except Exception:
            continue
        date_match = DATE_PATTERN.search(text)
        if not date_match:
            continue
        price_info = parse_price(text)
        if price_info is None:
            continue
        actual_date = infer_date(date_match.group(1), anchor)
        if actual_date is None or actual_date.isoformat() in seen_dates:
            continue
        seen_dates.add(actual_date.isoformat())
        classes = clean(
            (node.get_attribute("class") or "")
            + " "
            + ((node.locator("a").first.get_attribute("class") or "") if node.locator("a").count() else "")
        )
        items.append(
            {
                "preview_index": len(items),
                "dom_index": index,
                "label": date_match.group(1),
                "date": actual_date.isoformat(),
                "price_text": f"{price_info['currency']}{price_info['amount_text']}",
                "price_total_cny": price_info["price_total_cny"],
                "currency": price_info["currency"],
                "price_original": price_info["amount"],
                "is_selected": any(token in classes.lower() for token in ("active", "selected", "current", "cur", "on")),
            }
        )
    return items


def click_preview(page, target):
    nodes = page.locator(".J-date-picker li")
    if int(target["dom_index"]) >= nodes.count():
        return False
    try:
        nodes.nth(int(target["dom_index"])).click(timeout=10000)
        page.wait_for_timeout(1800)
        return True
    except Exception:
        return False


def rewrite_fdate(current_url, target_date):
    parsed = urlparse(current_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params["FDate"] = target_date.isoformat()
    return urlunparse(parsed._replace(query=urlencode(params)))


def advance_week(page, initial_url, target_date):
    previous_slug = current_route_slug(page)
    selectors = (
        ".J-date-picker .next",
        ".J-date-picker .date-next",
        ".J-date-picker [class*='next']",
        ".J-date-picker [class*='Next']",
        ".J-date-picker .swiper-button-next",
    )
    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() == 0:
            continue
        try:
            locator.first.click(timeout=6000)
            page.wait_for_timeout(1800)
            if current_route_slug(page) == previous_slug:
                return True
        except Exception:
            continue

    page.goto(rewrite_fdate(page.url or initial_url, target_date), wait_until="domcontentloaded", timeout=120000)
    try:
        page.wait_for_selector("[data-route-index], .flight-list, .J-date-picker", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(1800)
    return True


def launch_browser(playwright):
    launch_kwargs = {
        "headless": bool(browser_options.get("headless", True)),
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    executable_path = browser_options.get("executable_path") or None
    channel = browser_options.get("channel") or None
    if executable_path:
        launch_kwargs["executable_path"] = executable_path
    elif channel:
        launch_kwargs["channel"] = channel
    return playwright.chromium.launch(**launch_kwargs)


with sync_playwright() as p:
    browser = launch_browser(p)
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.set_extra_http_headers({"Accept-Language": "en-US,en;q=0.9"})

    expected_slug = re.search(r"/flights/([A-Z]{3}-[A-Z]{3})\.html", url)
    expected_slug = expected_slug.group(1) if expected_slug else ""

    preview_days = []
    day_results = []
    fail_samples = []
    blocked = False
    blocked_after_progress = False
    route_mismatch = False
    weeks_scanned = 0
    empty_weeks = 0
    consecutive_empty_weeks = 0

    current_week_start = start_date
    current_url = url
    while current_week_start <= end_date:
        page.goto(current_url, wait_until="domcontentloaded", timeout=120000)
        try:
            page.wait_for_selector("[data-route-index], .flight-list, .J-date-picker", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(2200)

        is_blocked, block_reason = detect_block(page)
        if is_blocked:
            blocked = True
            blocked_after_progress = weeks_scanned > 0 or len(day_results) > 0
            fail_samples.append(f"{current_week_start.isoformat()}: blocked={block_reason}")
            break

        current_slug = current_route_slug(page)
        if expected_slug and current_slug and current_slug != expected_slug:
            route_mismatch = True
            fail_samples.append(
                f"{current_week_start.isoformat()}: route mismatch expected {expected_slug} got {current_slug}"
            )
            break

        week_preview = collect_preview_items(page, current_week_start)
        if not week_preview:
            empty_weeks += 1
            consecutive_empty_weeks += 1
            fail_samples.append(f"{current_week_start.isoformat()}: no preview days rendered")
            if consecutive_empty_weeks > max_empty_weeks:
                break
            next_week_start = current_week_start + timedelta(days=window_days)
            if next_week_start > end_date:
                break
            advance_week(page, url, next_week_start)
            current_url = rewrite_fdate(page.url or url, next_week_start)
            current_week_start = next_week_start
            continue

        weeks_scanned += 1
        consecutive_empty_weeks = 0
        preview_days.extend(week_preview)
        for target in week_preview:
            actual_date = date.fromisoformat(target["date"])
            if actual_date < start_date or actual_date > end_date:
                continue
            if float(target["price_total_cny"]) >= threshold:
                continue
            clicked = click_preview(page, target)
            flights = collect_flights(page) if clicked else []
            error = ""
            if not clicked:
                error = "preview click failed"
                fail_samples.append(f"{actual_date.isoformat()}: click failed")
            elif not flights:
                error = "no flight rows after click"
            day_results.append(
                {
                    "date": actual_date.isoformat(),
                    "label": target["label"],
                    "page_url": page.url,
                    "clicked": clicked,
                    "preview_price_total_cny": target["price_total_cny"],
                    "flights": flights,
                    "error": error,
                }
            )

        next_week_start = current_week_start + timedelta(days=window_days)
        if next_week_start > end_date:
            break
        advance_week(page, url, next_week_start)
        current_url = rewrite_fdate(page.url or url, next_week_start)
        current_week_start = next_week_start

    payload = {
        "title": page.title(),
        "url": url,
        "final_url": page.url,
        "total_text": "",
        "body_text": clean(page.locator("body").inner_text()) if page.locator("body").count() else "",
        "flights": collect_flights(page),
        "blocked": blocked,
        "blocked_after_progress": blocked_after_progress,
        "route_mismatch": route_mismatch,
        "weeks_scanned": weeks_scanned,
        "empty_weeks": empty_weeks,
        "consecutive_empty_weeks": consecutive_empty_weeks,
        "preview_days": preview_days,
        "day_results": day_results,
        "fail_samples": fail_samples[:5],
        "route_key": route_key,
    }
    print(json.dumps(payload, ensure_ascii=False))
    context.close()
    browser.close()
'''


class AgentBrowserClient:
    def __init__(
        self,
        base_dir: Path,
        *,
        headless: bool,
        executable_path: str = "",
        channel: str = "",
    ) -> None:
        self.base_dir = base_dir
        self.browser_headless = headless
        self.browser_executable_path = executable_path
        self.browser_channel = channel

    def extract_spring_page(self, url: str) -> BrowserSpringPage:
        payload = self._run_script(PLAYWRIGHT_EXTRACT_SCRIPT, [url])
        return self._payload_to_page(payload)

    def extract_spring_window(
        self,
        url: str,
        click_threshold_cny: float,
        fx_to_cny: dict[str, float],
    ) -> BrowserSpringPage:
        payload = self._run_script(
            PLAYWRIGHT_WINDOW_SCRIPT,
            [url, str(click_threshold_cny), json.dumps(fx_to_cny, ensure_ascii=False)],
            timeout=240,
        )
        return self._payload_to_page(payload)

    def scan_spring_route(
        self,
        url: str,
        route_key: str,
        start_date: str,
        end_date: str,
        click_threshold_cny: float,
        window_days: int,
        max_empty_weeks: int,
        fx_to_cny: dict[str, float],
    ) -> BrowserSpringPage:
        payload = self._run_script(
            PLAYWRIGHT_ROUTE_SCAN_SCRIPT,
            [
                url,
                route_key,
                start_date,
                end_date,
                str(click_threshold_cny),
                str(window_days),
                str(max_empty_weeks),
                json.dumps(fx_to_cny, ensure_ascii=False),
            ],
            timeout=900,
        )
        return self._payload_to_page(payload)

    def _run_script(self, script: str, extra_args: list[str], timeout: int = 180) -> dict[str, object]:
        browser_options = self._browser_launch_options()
        mode = self._mode_for_script(script)
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        result = subprocess.run(
            [sys.executable, "-m", "fare_monitor", "--browser-worker", mode, json.dumps(browser_options), *extra_args],
            cwd=self.base_dir,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
        )
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if result.returncode != 0:
            raise BrowserUnavailableError(stderr or stdout or "Playwright browser extraction failed.")
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise BrowserUnavailableError(f"Playwright returned invalid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise BrowserUnavailableError("Browser extraction returned an unexpected payload.")
        return payload

    def _mode_for_script(self, script: str) -> str:
        mapping = {
            PLAYWRIGHT_EXTRACT_SCRIPT: "extract",
            PLAYWRIGHT_WINDOW_SCRIPT: "window",
            PLAYWRIGHT_ROUTE_SCAN_SCRIPT: "route-scan",
        }
        mode = mapping.get(script)
        if mode is None:
            raise BrowserUnavailableError("Unknown browser worker script mode.")
        return mode

    def _browser_launch_options(self) -> dict[str, object]:
        explicit_path = self.browser_executable_path.strip()
        if explicit_path:
            browser_path = Path(explicit_path)
            if not browser_path.is_absolute():
                browser_path = (self.base_dir / browser_path).resolve()
            if not browser_path.exists():
                raise BrowserUnavailableError(
                    f"Configured browser executable does not exist: {self.browser_executable_path}"
                )
            return {
                "executable_path": str(browser_path),
                "channel": "",
                "headless": self.browser_headless,
            }

        executable = self._find_browser_executable()
        if executable is not None:
            return {
                "executable_path": executable,
                "channel": "",
                "headless": self.browser_headless,
            }

        return {
            "executable_path": "",
            "channel": self.browser_channel.strip(),
            "headless": self.browser_headless,
        }

    def _payload_to_page(self, payload: dict[str, object]) -> BrowserSpringPage:
        return BrowserSpringPage(
            title=str(payload.get("title", "")),
            url=str(payload.get("url", "")),
            final_url=str(payload.get("final_url", payload.get("url", ""))),
            total_text=str(payload.get("total_text", "")),
            body_text=str(payload.get("body_text", "")),
            flights=[
                {str(key): str(value) for key, value in item.items()}
                for item in payload.get("flights", [])
                if isinstance(item, dict)
            ],
            raw_payload=json.dumps(payload, ensure_ascii=False),
            blocked=bool(payload.get("blocked", False)),
            blocked_after_progress=bool(payload.get("blocked_after_progress", False)),
            route_mismatch=bool(payload.get("route_mismatch", False)),
            weeks_scanned=int(payload.get("weeks_scanned", 0) or 0),
            empty_weeks=int(payload.get("empty_weeks", 0) or 0),
            consecutive_empty_weeks=int(payload.get("consecutive_empty_weeks", 0) or 0),
            fail_samples=[
                str(item)
                for item in payload.get("fail_samples", [])
                if isinstance(item, (str, int, float))
            ],
            preview_days=[
                {
                    str(key): value
                    for key, value in item.items()
                }
                for item in payload.get("preview_days", [])
                if isinstance(item, dict)
            ],
            day_results=[
                {
                    str(key): value
                    for key, value in item.items()
                }
                for item in payload.get("day_results", [])
                if isinstance(item, dict)
            ],
        )

    def _find_browser_executable(self) -> str | None:
        candidates = [
            Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
            Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
            Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
            Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
        ]
        for path in candidates:
            if path.exists():
                return str(path)
        for binary in (
            "google-chrome",
            "google-chrome-stable",
            "chromium",
            "chromium-browser",
            "microsoft-edge",
            "msedge",
        ):
            resolved = shutil.which(binary)
            if resolved:
                return resolved
        return None
