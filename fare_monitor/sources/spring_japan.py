from __future__ import annotations

import io
import re

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

from fare_monitor.sources.spring_airlines import SpringAirlinesAdapter


class SpringJapanAdapter(SpringAirlinesAdapter):
    source_name = "spring_japan"
    carrier_name = "SPRING JAPAN"
    live_enabled_by_default = False
    live_skip_reason = "Stable live mode uses the Spring main booking site for IJ flights as well, so this duplicate source is skipped."
    timetable_url = "https://en.ch.com/pages/IJ/int/time-table"

    def supported_route_keys(self) -> set[str]:
        if self.sample_mode:
            return super().supported_route_keys()
        if self._supported_route_keys is None:
            self._supported_route_keys = self.discover_route_keys()
        return self._supported_route_keys

    def discover_route_keys(self) -> set[str]:
        try:
            html, _ = self.fetch_text(self.timetable_url)
        except requests.RequestException:
            return set()

        soup = BeautifulSoup(html, "lxml")
        pdf_links = []
        for node in soup.find_all("a", href=True):
            href = node["href"]
            if href.lower().endswith(".pdf"):
                pdf_links.append(requests.compat.urljoin(self.timetable_url, href))

        route_keys: set[str] = set()
        for pdf_url in pdf_links:
            try:
                response = self.session.get(pdf_url, timeout=self.config.request_timeout)
                response.raise_for_status()
            except requests.RequestException:
                continue
            reader = PdfReader(io.BytesIO(response.content))
            text = "\n".join(page.extract_text() or "" for page in reader.pages)
            for line in text.splitlines():
                matches = re.findall(r"([A-Za-z][A-Za-z() /-]+)\uff08([A-Z]{3})\uff09", line)
                if len(matches) < 2:
                    continue
                for index in range(0, len(matches) - 1, 2):
                    origin = matches[index][1]
                    destination = matches[index + 1][1]
                    if origin in self.config.origins and destination in self.config.destinations:
                        route_keys.add(f"{origin}->{destination}")
        return route_keys
