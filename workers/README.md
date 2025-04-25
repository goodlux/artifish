# Artifish Workers

This directory contains background workers for data collection, analysis, and processing for the Artifish project.

## Installation

First, make sure you've installed the required dependencies:

```bash
pip install -r ../requirements.txt
```

## Environment Setup

Create a `.env` file in the project root with your credentials:

```
# Supabase credentials
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key

# Memgraph connection (optional)
MEMGRAPH_HOST=localhost
MEMGRAPH_PORT=7687

# Worker settings
SYNC_INTERVAL=60  # seconds between sync operations
```

## Available Workers

### Network Traversal Worker

The network traversal worker explores the Bluesky social graph, discovering users and their connections. It populates the database with account information, follows, and followers.

```bash
# Run with default settings (explore 100 accounts)
python network_traversal.py

# Run with custom seed accounts
python network_traversal.py --seed "boss-c.art.ifi.sh,another-handle.bsky.social"

# Run with custom exploration limit
python network_traversal.py --max 500

# Run with slower rate (longer delay between API calls)
python network_traversal.py --delay 3.5
```

### Unfollow Detector

The unfollow detector compares the current follows from the API with the follows stored in the database, marking unfollowed relationships:

```bash
# Run unfollow detection
python unfollow_detector.py
```

### Sentiment Analyzer

The sentiment analyzer connects to the Bluesky Jetstream API, processes posts in real-time, and performs sentiment analysis using VADER:

```bash
# First, set up NLTK data (run once)
python setup_nltk.py

# Start the sentiment analyzer
python sentiment_analyzer.py
```

### Memgraph Updater

The memgraph updater syncs data from Supabase to Memgraph, maintaining a graph representation of the Bluesky social network for more efficient traversal and analysis:

```bash
# Start the memgraph updater
python memgraph_updater.py
```

## Running with PM2

For production use, these workers should be run as background processes using PM2:

```bash
# Install PM2 if you don't have it
npm install -g pm2

# Start network traversal worker
pm2 start network_traversal.py --interpreter python3 --name "network-crawler" -- --max 1000 --delay 3

# Start unfollow detector (run periodically)
pm2 start unfollow_detector.py --interpreter python3 --name "unfollow-detector" --cron "0 */6 * * *"

# Start sentiment analyzer
pm2 start sentiment_analyzer.py --interpreter python3 --name "sentiment-analyzer"

# Start memgraph updater
pm2 start memgraph_updater.py --interpreter python3 --name "memgraph-updater"

# Monitor workers
pm2 monit

# View logs
pm2 logs

# Auto-restart on machine reboot
pm2 startup
pm2 save
```

## Database Schema Updates

Before running these workers, you need to apply the required database schema updates:

1. Navigate to the migrations directory:
```bash
cd ../migrations
```

2. Apply the migration to add required fields:
```bash
psql -d your_database -f 01_add_follows_timestamp.sql
```

Or use the Supabase UI to run the SQL statements in the migration file.

## Worker Architecture

The workers are designed to be lightweight, single-purpose processes that can run independently:

- **Network Traversal Worker**: Builds the social graph by exploring the network
- **Unfollow Detector**: Identifies when users unfollow each other
- **Sentiment Analyzer**: Connects to Jetstream and analyzes post sentiment in real-time
- **Memgraph Updater**: Syncs data to Memgraph for graph-based analysis

![Worker Architecture](https://i.imgur.com/placeholder.png)

## Guidelines for Worker Development

1. **Rate Limiting**: Be mindful of API rate limits. Add appropriate delays and respect rate limit headers.
2. **Error Handling**: Include robust error handling to ensure workers can recover gracefully.
3. **Logging**: Always include detailed logging to track progress and diagnose issues.
4. **Incremental Processing**: Design workers to make incremental progress and to safely restart.
5. **Resource Efficiency**: Keep memory and CPU usage low, especially for long-running workers.
