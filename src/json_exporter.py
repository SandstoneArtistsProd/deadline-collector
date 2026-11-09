"""
JSON Exporter with monthly storage and a rolling recent window.

Cloudflare Pages rejects any single file over 25 MiB. A full year of articles
with full text runs to ~55 MB, so the data is stored as:

  data/articles_YYYY-MM.json  one file per month (~6 MB each), the master record
  data/articles.json          the last WINDOW_DAYS days, same shape as before,
                              so anything reading that URL keeps working
  data/index.json             list of monthly files with article counts
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

WINDOW_DAYS = 60
# Stay well under Cloudflare Pages' 25 MiB per-file limit.
MAX_FILE_BYTES = 24 * 1024 * 1024


class JSONExporter:
    """Writes monthly article files plus a rolling articles.json window."""

    def __init__(self, output_path: str = "data/articles.json", window_days: int = WINDOW_DAYS):
        self.output_path = Path(output_path)
        self.data_dir = self.output_path.parent
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.window_days = window_days
        self.logger = logging.getLogger(__name__)

    # ---------- helpers ----------

    @staticmethod
    def _date_key(article: Dict) -> str:
        return article.get("date_collected") or article.get("publication_date") or ""

    def _month_path(self, month: str) -> Path:
        return self.data_dir / f"articles_{month}.json"

    def _read_articles(self, path: Path) -> List[Dict]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f).get("articles", [])
        except (OSError, json.JSONDecodeError, AttributeError) as e:
            self.logger.warning(f"Could not read {path}: {e}")
            return []

    def _write(self, path: Path, payload: Dict) -> None:
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        size = tmp.stat().st_size
        if size > MAX_FILE_BYTES:
            self.logger.warning(
                f"{path.name} is {size / 1e6:.1f} MB, close to Cloudflare's 25 MiB limit"
            )
        tmp.replace(path)

    def _load_existing(self) -> Dict[str, Dict]:
        """Load every stored article (monthly files plus legacy/current articles.json), keyed by URL."""
        by_url: Dict[str, Dict] = {}
        sources = sorted(self.data_dir.glob("articles_????-??.json"))
        if self.output_path.exists():
            sources.append(self.output_path)
        for path in sources:
            for a in self._read_articles(path):
                url = a.get("url")
                if url and url not in by_url:
                    by_url[url] = a
        return by_url

    # ---------- main ----------

    def export(self, articles: List[Dict]) -> str:
        """
        Merge new articles into monthly files, then rebuild articles.json as the
        last WINDOW_DAYS days and refresh index.json.
        """
        now = datetime.utcnow()
        stored = self._load_existing()

        new_count = 0
        touched_months = set()
        for article in articles:
            url = article.get("url", "")
            if not url or url in stored:
                continue
            entry = {
                "title": article.get("title", ""),
                "url": url,
                "date_collected": article.get("created_at", ""),
                "publication_date": article.get("publication_date", ""),
                "source": article.get("source", ""),
                "full_text": article.get("full_text", ""),
            }
            stored[url] = entry
            new_count += 1
            touched_months.add(self._date_key(entry)[:7] or "unknown")

        # Group everything by month.
        months: Dict[str, List[Dict]] = {}
        for a in stored.values():
            months.setdefault(self._date_key(a)[:7] or "unknown", []).append(a)
        for items in months.values():
            items.sort(key=self._date_key, reverse=True)

        # Write a monthly file when it is new, missing, or received articles this run.
        stamp = now.isoformat() + "Z"
        for month, items in months.items():
            path = self._month_path(month)
            if month in touched_months or not path.exists():
                self._write(path, {
                    "meta": {"month": month, "total_articles": len(items), "last_updated": stamp},
                    "articles": items,
                })

        # Rolling window, same structure as the old articles.json.
        cutoff = (now - timedelta(days=self.window_days)).strftime("%Y-%m-%d")
        recent = sorted(
            (a for a in stored.values() if self._date_key(a)[:10] >= cutoff),
            key=self._date_key,
            reverse=True,
        )
        self._write(self.output_path, {
            "meta": {
                "year": now.year,
                "total_articles": len(recent),
                "last_updated": stamp,
                "new_this_run": new_count,
                "window_days": self.window_days,
                "window_start": cutoff,
                "all_articles": len(stored),
                "monthly_files": "index.json",
            },
            "articles": recent,
        })

        # Index of monthly files.
        self._write(self.data_dir / "index.json", {
            "last_updated": stamp,
            "total_articles": len(stored),
            "recent_window": {"file": self.output_path.name, "days": self.window_days,
                              "articles": len(recent)},
            "months": [
                {"month": m, "file": self._month_path(m).name, "articles": len(months[m])}
                for m in sorted(months, reverse=True)
            ],
        })

        self.logger.info(
            f"JSON export complete: {len(recent)} articles in {self.window_days}-day window, "
            f"{len(stored)} total across {len(months)} monthly files ({new_count} new)"
        )
        return str(self.output_path)
