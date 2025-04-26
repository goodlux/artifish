# Artifish Project TODO List

## Core Workers

1. **Network Traversal Worker** ✅
   - Crawl Bluesky network to build social graph
   - Track follows/followers with timestamps
   - Handle follow/unfollow detection
   - Rate limiting and throttling mechanisms
   - Status: ✅ Implemented

2. **Memgraph Updater** ✅
   - Pull data from Supabase into Memgraph
   - Sync graph structure (users, follows)
   - Sync content (posts, topics)
   - Implement incremental updates
   - Status: ✅ Implemented

3. **Sentiment/Topic Analyzer** ✅
   - Connect to Bluesky Jetstream
   - Apply VADER sentiment analysis on incoming posts
   - Basic topic extraction and classification
   - Update database with analysis results
   - Status: ✅ Implemented

## Next Steps

1. **Testing & Deployment**
   - Test network traversal with small account limits
   - Verify sentiment analysis with sample posts
   - Set up PM2 for production deployment
   - Status: 📝 Planned

2. **Agent Integration**
   - Develop Google ADK agent integration
   - Create agent query interfaces for DB access
   - Build agent personalities and behaviors
   - Status: 📝 Planned

3. **Data Visualization**
   - Build simple dashboard for monitoring
   - Network graph visualization
   - Sentiment and topic trends
   - Status: 📝 Planned

## Future Enhancements

- Advanced topic modeling with more sophisticated NLP
- Interest inference from post content
- Cross-reference detection between users/topics
- Temporal analysis of sentiment shifts
- API endpoints for agent queries

## Network Traversal Optimizations

1. **Supabase Queue Implementation**
   - Add `last_crawled_at` timestamp column to `bluesky_accounts` table
   - Add `crawl_priority` numeric column for exploration ordering
   - Refactor code to use database for queue instead of in-memory list
   - Enable graceful restarts of incomplete crawls

2. **PostgreSQL Stored Procedures**
   - Create procedures for exploration queue management
   - Optimize follow relationship tracking
   - Implement efficient unfollow detection logic
   - Use batch operations to reduce network roundtrips

3. **Crawler Improvements**
   - Add better error recovery mechanisms
   - Implement configurable crawl strategies
   - Add metrics collection for crawl performance
   - Create admin dashboard for monitoring crawl progress

## Development Guidelines

- Keep workers simple and focused on specific tasks
- Implement robust error handling and recovery
- Use incremental approaches to avoid API rate limits
- Log activities thoroughly for debugging
- Design for eventual scale, but implement for current needs
