from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class Job:
    company: str
    title: str
    location: str
    url: str
    posted_at: Optional[datetime]  # tz-aware UTC when known
    source: str  # which scraper produced this row

    def to_dict(self) -> dict:
        d = asdict(self)
        d["posted_at"] = self.posted_at.isoformat() if self.posted_at else None
        return d
