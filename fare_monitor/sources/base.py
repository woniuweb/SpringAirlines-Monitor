from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from fare_monitor.config import AppConfig
from fare_monitor.models import RawPayload, SearchQuery, SourceFetchResult
from fare_monitor.sample_data import build_sample_result, sample_route_keys


class SourceAdapter(ABC):
    source_name: str
    carrier_name: str
    live_enabled_by_default: bool = True
    live_skip_reason: str = ""

    def __init__(self, config: AppConfig, sample_mode: bool = False, session: requests.Session | None = None) -> None:
        self.config = config
        self.sample_mode = sample_mode
        self.session = session or requests.Session()
        self.session.headers.update(self.config.headers)
        self._text_cache: dict[str, tuple[str, str] | requests.RequestException] = {}
        self._supported_route_keys: set[str] | None = None

    def search(self, query: SearchQuery, collection_id: str) -> SourceFetchResult:
        if not self.supports_query(query):
            return SourceFetchResult(source=self.source_name, status="skipped", message="")
        if self.sample_mode:
            return build_sample_result(
                source=self.source_name,
                query=query,
                collection_id=collection_id,
                booking_url=self.build_booking_url(query),
                qualified_threshold=self.config.qualified_threshold,
            )
        return self._search_live(query, collection_id)

    def filter_queries(self, queries: list[SearchQuery]) -> list[SearchQuery]:
        return [query for query in queries if self.supports_query(query)]

    def is_live_enabled(self) -> bool:
        return self.sample_mode or self.config.is_source_enabled(self.source_name)

    def supports_query(self, query: SearchQuery) -> bool:
        route_keys = self.supported_route_keys()
        return query.route_key in route_keys

    def supported_route_keys(self) -> set[str]:
        if self.sample_mode:
            return sample_route_keys(self.source_name)
        if self._supported_route_keys is None:
            self._supported_route_keys = self.discover_route_keys()
        return self._supported_route_keys

    def discover_route_keys(self) -> set[str]:
        return set()

    @abstractmethod
    def build_booking_url(self, query: SearchQuery) -> str:
        raise NotImplementedError

    @abstractmethod
    def _search_live(self, query: SearchQuery, collection_id: str) -> SourceFetchResult:
        raise NotImplementedError

    def fetch_text(self, url: str) -> tuple[str, str]:
        cached = self._text_cache.get(url)
        if cached is not None:
            if isinstance(cached, requests.RequestException):
                raise cached
            return cached
        try:
            response = self.session.get(url, timeout=self.config.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            self._text_cache[url] = exc
            raise
        payload = (response.text, response.url)
        self._text_cache[url] = payload
        return payload

    def default_payload(self, name: str, content: str, extension: str = "html") -> RawPayload:
        return RawPayload(name=name, content=content, extension=extension)

    def no_fare_result(self, source: str, message: str, payloads: list[RawPayload] | None = None) -> SourceFetchResult:
        return SourceFetchResult(source=source, fares=[], status="empty", message=message, payloads=payloads or [])

    def extract_hidden_inputs(self, html: str) -> dict[str, str]:
        soup = BeautifulSoup(html, "lxml")
        values: dict[str, str] = {}
        for node in soup.select("input[name]"):
            name = node.get("name")
            if not name:
                continue
            values[name] = node.get("value", "")
        return values

    def extract_price_placeholder(self, html: str) -> str | None:
        match = re.search(r'<div class="price J-total">(?:&yen;|¥)<em>([^<]+)</em>', html)
        if match:
            return match.group(1).replace(",", "").strip()
        return None


def save_payloads(base_dir: Path, source: str, collection_id: str, query: SearchQuery, payloads: list[RawPayload]) -> None:
    target_dir = base_dir / source / collection_id
    target_dir.mkdir(parents=True, exist_ok=True)
    for payload in payloads:
        safe_name = re.sub(r"[^a-zA-Z0-9._-]+", "-", payload.name)
        path = target_dir / f"{query.origin}-{query.destination}-{query.depart_date.isoformat()}-{safe_name}.{payload.extension}"
        path.write_text(payload.content, encoding="utf-8")
