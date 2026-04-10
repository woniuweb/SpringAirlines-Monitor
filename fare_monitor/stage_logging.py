from __future__ import annotations

from datetime import datetime
from pathlib import Path


class StageLogger:
    def __init__(self, enabled: bool, log_path: Path | None = None) -> None:
        self.enabled = enabled
        self.log_path = log_path
        if self.enabled and self.log_path is not None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self.log_path.write_text("", encoding="utf-8")

    def log(self, stage: str, message: str) -> None:
        if not self.enabled:
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{timestamp}] {stage} {message}"
        print(line, flush=True)
        if self.log_path is not None:
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
