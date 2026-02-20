# Deadline Article Collector

This is a Python-based RSS article collector that continuously fetches entertainment news from Deadline, Variety, and Hollywood Reporter. Features full-text extraction, smart deduplication, and CSV export capabilities.

## Features

- **RSS Feed Monitoring**: Automatically collects articles from multiple entertainment news sources
- **Full-Text Extraction**: Scrapes and stores complete article content
- **Smart Deduplication**: Detects duplicate articles across different sources using content hashing and similarity analysis
- **SQLite Database**: Efficient local storage with indexing for fast queries
- **CSV Export**: Export articles to CSV format with or without full text
- **Scheduled Collection**: Run on a schedule (hourly, every 30 mins, etc.)
- **Archive Collection**: Backfill from Deadline's monthly archives
- **Logging**: Comprehensive logging for monitoring and debugging

## Installation

### Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

### Setup

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd deadline-collector
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Run Once (Single Collection)

Collect articles once and exit:

```bash
python collector.py
```

### Scheduled Collection

Run continuously on a schedule (default: every hour):

```bash
python collector.py --schedule
```

The schedule can be configured in `config/config.yaml`.

### Export to CSV

Export all articles to CSV:

```bash
python collector.py --export
```

Export from specific source only:

```bash
python collector.py --export --source Deadline
```

Export summary (without full text):

```bash
python collector.py --export --summary
```

### View Statistics

Show database statistics:

```bash
python collector.py --stats
```

### Custom Configuration

Use a different config file:

```bash
python collector.py --config /path/to/config.yaml
```

## Configuration

Edit `config/config.yaml` to customize:

- **RSS Feeds**: Add/remove news sources
- **Collection Schedule**: Change frequency of collection
- **Deduplication**: Adjust similarity threshold
- **Rate Limiting**: Control request delays
- **Content Extraction**: Minimum content length, timeout, retries

Example configuration:

```yaml
collection:
  feeds:
    - name: "Deadline"
      url: "https://deadline.com/feed/"
      domain: "deadline.com"

  min_year: 2025
  similarity_threshold: 0.7
  delay_between_requests: 1.0

schedule:
  cron: "0 * * * *"  # Every hour
```

## Running as a Cron Job

### Linux/macOS

Add to crontab:

```bash
crontab -e
```

Add this line to run every hour:

```
0 * * * * cd /path/to/deadline-collector && /path/to/venv/bin/python collector.py >> logs/cron.log 2>&1
```

### Using systemd (Linux)

Create a service file `/etc/systemd/system/deadline-collector.service`:

```ini
[Unit]
Description=Deadline Article Collector
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/path/to/deadline-collector
ExecStart=/path/to/venv/bin/python collector.py --schedule
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable deadline-collector
sudo systemctl start deadline-collector
sudo systemctl status deadline-collector
```

### Using PM2 (Alternative)

```bash
# Install PM2
npm install -g pm2

# Start collector
pm2 start collector.py --name deadline-collector --interpreter python3 -- --schedule

# Save configuration
pm2 save

# Setup to start on boot
pm2 startup
```

## Project Structure

```
deadline-collector/
├── collector.py           # Main entry point
├── config/
│   └── config.yaml       # Configuration file
├── src/
│   ├── database.py       # Database operations
│   ├── content_extractor.py  # Web scraping & content extraction
│   ├── deadline_collector.py # Main collector logic
│   └── csv_exporter.py   # CSV export functionality
├── exports/              # CSV exports (created automatically)
├── logs/                 # Log files (created automatically)
├── articles.db          # SQLite database (created automatically)
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Database Schema

The SQLite database contains:

- **articles**: Main table storing articles with full text
  - Indexed on: url, content_hash, normalized_title, publication_date, source
  - Tracks duplicates with foreign key to original article

- **system_tracking**: Tracks collection runs and timing

## Deduplication Logic

The collector uses multi-level deduplication:

1. **URL Check**: Skip if URL already exists
2. **Content Hash**: Fast comparison using MD5 hash of content
3. **Title Normalization**: Remove punctuation, normalize whitespace
4. **Similarity Analysis**: Jaccard similarity for content comparison

## Output Files

- **Database**: `articles.db` - SQLite database with all articles
- **CSV Exports**: `exports/deadline_articles_TIMESTAMP.csv`
- **Logs**: `logs/collector.log` - Application logs
- **Statistics**: `exports/statistics_TIMESTAMP.txt`

## Troubleshooting

### No articles collected

- Check internet connection
- Verify RSS feed URLs are accessible
- Check `min_year` setting in config
- Review logs in `logs/collector.log`

### Content extraction fails

- Some sites may block scrapers
- Increase `request_timeout` in config
- Check if site structure has changed
- Review User-Agent string

### Database locked errors

- Don't run multiple instances simultaneously
- Check file permissions on `articles.db`

## Development

### Running Tests

```bash
# TODO: Add tests
pytest tests/
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License - See LICENSE file for details

## Author

Created for entertainment news collection and analysis.

## Changelog

### Version 1.0.0
- Initial release
- RSS feed collection
- Full-text extraction
- Deduplication system
- CSV export
- Scheduled collection
