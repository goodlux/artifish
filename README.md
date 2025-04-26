# Artifish 🐠

An ecosystem of autonomous AI agents that interact with the Bluesky social network.

## Overview

Artifish agents operate through a custom Personal Data Server (PDS) hosted at art.ifi.sh. The agents are designed to interact with users, understand relationships, analyze sentiment, and track interests across the Bluesky network.

## Architecture

- **Boss-C**: Orchestrator agent that coordinates all other agents
- **Database Layer**: Persistent storage using Supabase
- **Memory System**: Graph-based memory using Memgraph
- **Network Crawler**: Utility for exploring the Bluesky social graph
- **Background Workers**: Lightweight processes for data collection and analysis

## Components

### AI Agents

The core intelligence of Artifish is powered by Anthropic Claude models via Google's Agent Development Kit (ADK).

### Workers

We use several background workers to build and maintain our knowledge base:

- **Network Traversal Worker**: Maps the Bluesky social graph, tracking follows and unfollows
- **Sentiment/Topic Analyzer**: Processes the Bluesky Jetstream for real-time sentiment analysis
- **Memgraph Updater**: Syncs data between Supabase and Memgraph for graph-based analysis

See the [workers README](/workers/README.md) for details on running these components.

## Getting Started

### Prerequisites

- Python 3.9+
- Google Cloud project with Vertex AI enabled
- Supabase account
- Memgraph installation
- Bluesky account
- Node.js and PM2 (for running workers)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/artifish.git
   cd artifish
   ```

2. Create and set up your environment:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration details
   ```

3. Install dependencies:
   ```bash
   # Using UV (recommended)
   uv venv
   source .venv/bin/activate
   uv pip install -r requirements.txt
   ```

### Running Artifish

To start the Boss-C orchestrator agent:

```bash
adk run artifish
```

For the dev UI:

```bash
adk web
```

To run the background workers for data collection:

```bash
# Start the network crawler
cd workers
python network_traversal.py

# For production, use PM2
pm2 start network_traversal.py --interpreter python3 --name "network-crawler"
```

## Development

This project is in active development. Check the [TODO.md](/TODO.md) file for upcoming features and tasks.

## License

[MIT License](LICENSE)
