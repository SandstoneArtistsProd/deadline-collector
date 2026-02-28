#!/usr/bin/env python3
"""
Deadline Article Collector - Main Entry Point

Run modes:
  - collector.py              : Run once and exit
  - collector.py --schedule   : Run on schedule (continuous)
  - collector.py --export     : Export articles to CSV
  - collector.py --stats      : Show database statistics
"""

import sys
import argparse
import logging
from pathlib import Path
import yaml
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from database import ArticleDatabase
from content_extractor import ContentExtractor
from deadline_collector import DeadlineCollector
from csv_exporter import CSVExporter
from json_exporter import JSONExporter


def setup_logging(config: dict) -> logging.Logger:
    """
    Setup logging configuration.

    Args:
        config: Configuration dictionary

    Returns:
        Logger instance
    """
    log_config = config.get('logging', {})
    log_level = log_config.get('level', 'INFO')
    log_dir = Path(log_config.get('directory', 'logs'))
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create formatters and handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler
    log_file = log_dir / 'collector.log'
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return root_logger


def load_config(config_path: str = 'config/config.yaml') -> dict:
    """
    Load configuration file.

    Args:
        config_path: Path to config file

    Returns:
        Configuration dictionary
    """
    config_file = Path(config_path)

    if not config_file.exists():
        # Try JSON
        config_file = Path(config_path.replace('.yaml', '.json'))

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, 'r') as f:
        if config_file.suffix == '.yaml' or config_file.suffix == '.yml':
            config = yaml.safe_load(f)
        else:
            config = json.load(f)

    return config


def run_collection(config: dict):
    """
    Run article collection once.

    Args:
        config: Configuration dictionary
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting article collection...")

    db_path = config.get('database', {}).get('path', 'articles.db')

    with ArticleDatabase(db_path) as db:
        extractor = ContentExtractor(config.get('extraction', {}))
        collector = DeadlineCollector(config.get('collection', {}), db, extractor)

        try:
            collector.collect()
            logger.info("Collection completed successfully")
        finally:
            extractor.close()


def run_scheduled_collection(config: dict):
    """
    Run collection on a schedule.

    Args:
        config: Configuration dictionary
    """
    import schedule
    import time

    logger = logging.getLogger(__name__)
    schedule_config = config.get('schedule', {})
    cron_expression = schedule_config.get('cron', '0 * * * *')  # Default: hourly

    logger.info("Starting scheduled collector...")
    logger.info(f"Schedule: {cron_expression}")

    # Parse cron-like expression (simplified)
    # Format: minute hour day month day_of_week
    # For simplicity, we'll support common patterns
    if cron_expression == '0 * * * *':
        # Every hour
        schedule.every().hour.at(":00").do(lambda: run_collection(config))
        logger.info("Scheduled: Every hour at :00")
    elif cron_expression == '*/30 * * * *':
        # Every 30 minutes
        schedule.every(30).minutes.do(lambda: run_collection(config))
        logger.info("Scheduled: Every 30 minutes")
    elif cron_expression.startswith('0 */'):
        # Every N hours
        hours = int(cron_expression.split()[1].replace('*/', ''))
        schedule.every(hours).hours.do(lambda: run_collection(config))
        logger.info(f"Scheduled: Every {hours} hours")
    else:
        # Default to hourly
        schedule.every().hour.do(lambda: run_collection(config))
        logger.info("Scheduled: Every hour (default)")

    # Run initial collection
    logger.info("Running initial collection...")
    run_collection(config)

    # Keep running
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


def export_articles(config: dict, source_filter: str = None, summary_only: bool = False):
    """
    Export articles to CSV.

    Args:
        config: Configuration dictionary
        source_filter: Filter by source name
        summary_only: Export summary only (no full text)
    """
    logger = logging.getLogger(__name__)
    logger.info("Exporting articles...")

    db_path = config.get('database', {}).get('path', 'articles.db')
    export_dir = config.get('export', {}).get('directory', 'exports')

    with ArticleDatabase(db_path) as db:
        # Get articles
        articles = db.get_all_articles(source=source_filter, include_duplicates=False)

        if not articles:
            logger.warning("No articles found to export")
            return

        logger.info(f"Found {len(articles)} articles")

        # Export
        exporter = CSVExporter(export_dir)

        if summary_only:
            filepath = exporter.export_summary(articles)
        else:
            filepath = exporter.export_articles(articles, source_filter=source_filter)

        logger.info(f"Export complete: {filepath}")


def show_statistics(config: dict):
    """
    Display database statistics.

    Args:
        config: Configuration dictionary
    """
    logger = logging.getLogger(__name__)
    db_path = config.get('database', {}).get('path', 'articles.db')

    with ArticleDatabase(db_path) as db:
        stats = db.get_statistics()

        print("\n" + "=" * 60)
        print("DEADLINE COLLECTOR STATISTICS")
        print("=" * 60)

        overall = stats['overall']
        print(f"\nTotal Unique Articles: {overall['total_articles']:,}")
        print(f"Duplicates Filtered: {stats['duplicates']:,}")
        print(f"Total Characters: {overall.get('total_chars', 0):,}")
        print(f"Average Article Length: {overall.get('avg_length', 0):,.0f} chars")
        print(f"Date Range: {overall.get('earliest', 'N/A')} to {overall.get('latest', 'N/A')}")

        if stats['by_source']:
            print("\nBY SOURCE:")
            print("-" * 60)
            for source in stats['by_source']:
                print(f"  {source['source']:25} {source['count']:6,} articles  "
                      f"(avg: {source.get('avg_length', 0):,.0f} chars)")

        print("=" * 60 + "\n")


def export_json(config: dict):
    """
    Export articles to cumulative calendar-year JSON file.

    Args:
        config: Configuration dictionary
    """
    logger = logging.getLogger(__name__)
    logger.info("Exporting articles to JSON...")

    db_path = config.get('database', {}).get('path', 'articles.db')
    output_path = config.get('export', {}).get('json_path', 'data/articles.json')

    with ArticleDatabase(db_path) as db:
        articles = db.get_all_articles(source=None, include_duplicates=False)

        if not articles:
            logger.warning("No articles found to export")
            return

        logger.info(f"Found {len(articles)} articles in database")
        exporter = JSONExporter(output_path)
        filepath = exporter.export(articles)
        logger.info(f"JSON export complete: {filepath}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Deadline Article Collector - Collect and export entertainment news articles'
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to configuration file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--export-json',
        action='store_true',
        help='Export articles to cumulative calendar-year JSON file'
    )
    parser.add_argument(
        '--schedule',
        action='store_true',
        help='Run on schedule (continuous mode)'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Export articles to CSV'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show database statistics'
    )
    parser.add_argument(
        '--source',
        help='Filter by source (e.g., Deadline, Variety)'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Export summary only (no full text)'
    )

    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config)

        # Setup logging
        setup_logging(config)
        logger = logging.getLogger(__name__)

        # Determine mode
        if args.stats:
            show_statistics(config)
        elif args.export:
            export_articles(config, source_filter=args.source, summary_only=args.summary)
        elif getattr(args, 'export_json', False):
            export_json(config)
        elif args.schedule:
            run_scheduled_collection(config)
        else:
            # Default: run once
            run_collection(config)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        logging.exception("Fatal error")
        sys.exit(1)


if __name__ == '__main__':
    main()
