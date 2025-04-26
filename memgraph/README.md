# Artifish Memgraph Integration

This directory contains scripts for integrating Memgraph with the Artifish project, specifically for analyzing the Bluesky social network.

## Setup

1. Start the Memgraph Docker container using the docker-compose file in the project root:

```bash
docker-compose up -d
```

2. Ensure you have the necessary environment variables set in your `.env` file:

```
# Supabase PostgreSQL connection details
SUPABASE_DB_HOST=db.uqdfoqccbjfpftpvqwam.supabase.co
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=your_password_here

# Memgraph connection details (usually not needed for default setup)
MEMGRAPH_URI=bolt://localhost:7687
MEMGRAPH_USER=
MEMGRAPH_PASSWORD=
```

3. Install the required Python packages:

```bash
pip install neo4j python-dotenv
```

## Usage

### Initial Sync

For the first sync, run:

```bash
python mage_sync.py --initial
```

This will:
1. Set up the schema with constraints and indexes
2. Import all Bluesky accounts from Supabase
3. Import all follow relationships

### Incremental Updates

For regular updates, run:

```bash
python mage_sync.py
```

This will only sync data that has changed since the last sync. The script checks the last sync timestamp and only imports new or modified data.

### Force Sync

If you want to force a sync even if one was performed recently:

```bash
python mage_sync.py --force
```

## Schema

The current schema includes:

- **User nodes** with properties:
  - `did` (unique identifier)
  - `handle`
  - `display_name`
  - `bio`
  - `avatar_url`
  - `updated_at`

- **FOLLOWS relationships** with properties:
  - `created_at`
  - `last_verified_at`

## Visualization

You can visualize the graph using Memgraph Lab, which is available at:

http://localhost:3000

Try these example queries:

```cypher
// View all users
MATCH (u:User)
RETURN u
LIMIT 100;

// Find users with the most followers
MATCH (follower:User)-[:FOLLOWS]->(u:User)
RETURN u.handle as handle, count(follower) as followers
ORDER BY followers DESC
LIMIT 10;

// Find users with the most follows
MATCH (u:User)-[:FOLLOWS]->(following:User)
RETURN u.handle as handle, count(following) as following
ORDER BY following DESC
LIMIT 10;

// Find mutuals (users who follow each other)
MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1)
RETURN u1.handle as user1, u2.handle as user2
LIMIT 100;
```

## Extending

As the project evolves, this integration can be extended to include:
- Post data
- Topic/interest tracking
- Sentiment analysis
- More complex relationships and patterns

To add new data types, simply create new sync methods in the `MemgraphSync` class that use the MAGE migration module to import the data from Supabase.
