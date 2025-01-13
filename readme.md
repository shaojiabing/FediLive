This project allows you to crawl all live feeds from Mastodon instances within a specified time range. It consists of two main steps:
1. **Fetch Mastodon instance information**.
2. **Crawl live feeds for the specified time range**.

## Features
- **Instance Data Fetching**: Fetches Mastodon instance metadata and stores it in a local SQLite database.
- **Live Feed Crawling**: Crawls live feeds from all instances for the specified time range and stores them in the database.
- **Multiprocessing**: Supports parallel crawling using multiple processes.
- **JSON Export**: Exports all live feeds data to a JSON file.

---

## File Structure

```plaintext
project/
│
├── database_manager.py   # Manages database initialization and connections.
├── fetch_mastolist.py    # Fetches Mastodon instances and populates the database.
├── instance_manager.py   # Handles operations related to the `instances` table.
├── livefeeds_manager.py  # Manages live feed operations for the `livefeeds` table.
├── main.py               # Main script to crawl live feeds.
├── utils.py              # Utility functions for time handling and other helper methods.
├── README.md             # Project documentation.
└── requirements.txt      # Python dependencies (if applicable).
```

---

## Requirements

- Python 3.8 or later
- SQLite3
- instance.social token and Mastodon API token (required for authentication)

---

## Installation

1. Clone the repository:
   ```bash
   git clone -b Single git@github.com:shaojiabing/FediLive.git
   cd FediLive
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

### Step 1: Fetch Mastodon Instances
Use `fetch_mastolist.py` to fetch the list of Mastodon instances and populate the database.

```bash
python fetch_mastolist.py --token YOUR_INSTANCESOCIAL_API_TOKEN
```

- **Arguments**:
  - `--token` (required): Your instance.social API token.

This step will:
1. Fetch the list of Mastodon instances.
2. Store the instance data in the `instances` table of the SQLite database.

---

### Step 2: Crawl Live Feeds
Use `main.py` to crawl live feeds for the specified time range.

```bash
python main.py --token YOUR_MASTODON_API_TOKEN --start "YYYY-MM-DD HH:MM:SS" --end "YYYY-MM-DD HH:MM:SS" --processnum NUM_PROCESSES --output OUTPUT_FILE
```

- **Arguments**:
  - `--token` (required): Your Mastodon API token.
  - `--start` (required): Start time for the crawl in the format `YYYY-MM-DD HH:MM:SS`.
  - `--end` (optional): End time for the crawl in the format `YYYY-MM-DD HH:MM:SS` (default: current time).
  - `--processnum` (optional): Number of parallel processes to use for crawling (default: 1).
  - `--output` (optional): Output JSON file to save live feeds (default: `livefeeds.json`).

This step will:
1. Fetch live feeds from all instances within the specified time range.
2. Store live feed data in the `livefeeds` table.
3. Export all live feed data to the specified JSON file.

---

## Example

### 1. Fetch Instances
```bash
python fetch_mastolist.py --token ABC123
```

### 2. Crawl Live Feeds
```bash
python main.py --token ABC123 --start "2025-01-01 00:00:00" --end "2025-01-02 00:00:00" --processnum 4 --output livefeeds.json
```

---

## Notes

- Ensure your Mastodon API token has sufficient permissions to access the necessary endpoints.
- Use the `--processnum` argument to optimize crawling speed by using multiple processes.
