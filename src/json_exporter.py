"""
JSON Exporter for cumulative calendar-year article storage.
Exports articles to a single articles.json file containing the full current year.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional


class JSONExporter:
    """Exports articles to a cumulative JSON file, scoped to the current calendar year."""

    def __init__(self, output_path: str = "data/articles.json"):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def _load_existing(self) -> List[Dict]:
        """Load existing articles from JSON file if it exists."""
        if self.output_path.exists():
            try:
                with open(self.output_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('articles', [])
            except (json.JSONDecodeError, KeyError) as e:
                self.logger.warning(f"Could not load existing JSON: {e}. Starting fresh.")
        return []

    def export(self, articles: List[Dict]) -> str:
        """
        Merge new articles into the cumulative JSON file.
        Only includes articles from the current calendar year.
        Deduplicates by URL.

        Args:
            articles: List of article dicts from the database

        Returns:
            Path to the output file
        """
        current_year = datetime.utcnow().year

        # Load what's already saved
        existing = self._load_existing()
        existing_urls = {a['url'] for a in existing}

        # Filter and format new articles
        new_count = 0
        for article in articles:
            url = article.get('url', '')
            if not url or url in existing_urls:
                continue

            # Parse date - use created_at (date collected) as primary
            date_collected = article.get('created_at', '')
            pub_date = article.get('publication_date', '')

            # Determine the year to filter on
            date_to_check = date_collected or pub_date
            if date_to_check:
                try:
                    year = datetime.fromisoformat(date_to_check[:10]).year
                    if year != current_year:
                        continue
                except ValueError:
                    pass  # If we can't parse, include it

            entry = {
                "title": article.get('title', ''),
                "url": url,
                "date_collected": date_collected,
                "publication_date": pub_date,
                "source": article.get('source', ''),
                "full_text": article.get('full_text', ''),
            }

            existing.append(entry)
            existing_urls.add(url)
            new_count += 1

        # Sort by date_collected descending (newest first)
        existing.sort(
            key=lambda x: x.get('date_collected') or x.get('publication_date') or '',
            reverse=True
        )

        # Write output
        output = {
            "meta": {
                "year": current_year,
                "total_articles": len(existing),
                "last_updated": datetime.utcnow().isoformat() + "Z",
                "new_this_run": new_count
            },
            "articles": existing
        }

        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        self.logger.info(
            f"JSON export complete: {len(existing)} total articles "
            f"({new_count} new) → {self.output_path}"
        )
        return str(self.output_path)

    def archive_year(self, year: int, archive_path: Optional[str] = None) -> str:
        """
        Copy current articles.json to a year archive file and clear the main file.

        Args:
            year: The year being archived
            archive_path: Optional custom path for archive

        Returns:
            Path to the archive file
        """
        if archive_path is None:
            archive_path = self.output_path.parent / f"articles_{year}.json"

        archive_path = Path(archive_path)

        if self.output_path.exists():
            with open(self.output_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            with open(archive_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"Archived {year} articles to {archive_path}")

            # Clear main file for new year
            empty = {
                "meta": {
                    "year": year + 1,
                    "total_articles": 0,
                    "last_updated": datetime.utcnow().isoformat() + "Z",
                    "new_this_run": 0
                },
                "articles": []
            }
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(empty, f, ensure_ascii=False, indent=2)

            self.logger.info(f"Cleared articles.json for {year + 1}")

        return str(archive_path)
