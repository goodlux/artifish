"""
Memgraph Updater Worker

This worker syncs data from Supabase to Memgraph, maintaining a graph representation
of the Bluesky social network for more efficient traversal and analysis.

Features:
- Incremental sync of users, follows, and posts
- Graph-based representation of social relationships
- Topic and sentiment mapping in the graph
"""

import os
import logging
import time
from datetime import datetime, timedelta
import json
from typing import Dict, Any, List, Set, Optional
import threading
import signal
from dotenv import load_dotenv  # Added dotenv import
from supabase import create_client, Client
import neo4j
from gqlalchemy import Memgraph

# Load environment variables from .env file
load_dotenv()  # Added this line to load .env file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("memgraph_updater.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MemgraphUpdater")

class MemgraphUpdater:
    """Worker for syncing data from Supabase to Memgraph."""
    
    def __init__(self, 
                 supabase_url: str, 
                 supabase_key: str, 
                 memgraph_host: str = "localhost", 
                 memgraph_port: int = 7687):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.memgraph = Memgraph(host=memgraph_host, port=memgraph_port)
        self.running = False
        self.sync_interval = 60  # Default sync interval in seconds
    
    def start(self, sync_interval: int = 60):
        """Start the Memgraph updater."""
        logger.info("Starting Memgraph updater")
        self.running = True
        self.sync_interval = sync_interval
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            # Ensure indexes and constraints
            self._ensure_indexes()
            
            # Main sync loop
            while self.running:
                start_time = time.time()
                
                # Perform sync operations
                users_count = self._sync_users()
                follows_count = self._sync_follows()
                posts_count = self._sync_posts()
                
                # Calculate time spent and sleep for remaining interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.sync_interval - elapsed)
                
                logger.info(f"Sync completed. Users: {users_count}, Follows: {follows_count}, Posts: {posts_count}. Next sync in {sleep_time:.1f}s")
                
                if self.running and sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except Exception as e:
            logger.error(f"Error in main sync loop: {e}")
            
        finally:
            logger.info("Memgraph updater stopped")
    
    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {sig}, shutting down...")
        self.running = False
    
    def _ensure_indexes(self):
        """Ensure Memgraph has necessary indexes and constraints."""
        try:
            # Create unique constraint on User did
            self.memgraph.execute("""
                CREATE CONSTRAINT ON (u:User)
                ASSERT u.did IS UNIQUE
            """)
            
            # Create index on Post uri
            self.memgraph.execute("""
                CREATE INDEX ON :Post(uri)
            """)
            
            # Create index on Topic name
            self.memgraph.execute("""
                CREATE INDEX ON :Topic(name)
            """)
            
            logger.info("Memgraph indexes and constraints set up")
            
        except Exception as e:
            logger.warning(f"Error setting up indexes (may already exist): {e}")
    
    def _sync_users(self) -> int:
        """Sync users from Supabase to Memgraph."""
        try:
            # Get last sync timestamp from Memgraph
            result = self.memgraph.execute_and_fetch("""
                MATCH (m:Metadata {key: 'last_user_sync'})
                RETURN m.timestamp AS timestamp
            """)
            
            last_sync = None
            for record in result:
                last_sync = record.get('timestamp')
            
            # Initialize if this is the first sync
            if not last_sync:
                # Create metadata node
                self.memgraph.execute("""
                    CREATE (m:Metadata {key: 'last_user_sync', timestamp: $timestamp})
                """, {'timestamp': datetime.now().isoformat()})
                
                # Get all users (limited for first sync)
                query = self.supabase.table('bluesky_accounts') \
                               .select('*') \
                               .order('indexed_at', desc=True) \
                               .limit(1000)
            else:
                # Get users updated since last sync
                query = self.supabase.table('bluesky_accounts') \
                               .select('*') \
                               .gt('last_updated_at', last_sync) \
                               .order('last_updated_at')
            
            result = query.execute()
            users = result.data
            
            # Create Cypher query to insert/update users in batches
            batch_size = 100
            for i in range(0, len(users), batch_size):
                batch = users[i:i+batch_size]
                params = {'users': [self._prepare_user_data(user) for user in batch]}
                
                self.memgraph.execute("""
                    UNWIND $users AS user
                    MERGE (u:User {did: user.did})
                    SET 
                        u.handle = user.handle,
                        u.display_name = user.display_name,
                        u.bio = user.bio,
                        u.avatar_url = user.avatar_url,
                        u.updated_at = user.updated_at
                """, params)
            
            # Update last sync timestamp
            if users:
                self.memgraph.execute("""
                    MATCH (m:Metadata {key: 'last_user_sync'})
                    SET m.timestamp = $timestamp
                """, {'timestamp': datetime.now().isoformat()})
            
            return len(users)
            
        except Exception as e:
            logger.error(f"Error syncing users: {e}")
            return 0
    
    def _prepare_user_data(self, user: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare user data for Memgraph."""
        return {
            'did': user.get('did'),
            'handle': user.get('handle'),
            'display_name': user.get('display_name') or '',
            'bio': user.get('bio') or '',
            'avatar_url': user.get('avatar_url') or '',
            'updated_at': datetime.now().isoformat()
        }
    
    def _sync_follows(self) -> int:
        """Sync follows from Supabase to Memgraph."""
        try:
            # Get last sync timestamp from Memgraph
            result = self.memgraph.execute_and_fetch("""
                MATCH (m:Metadata {key: 'last_follows_sync'})
                RETURN m.timestamp AS timestamp
            """)
            
            last_sync = None
            for record in result:
                last_sync = record.get('timestamp')
            
            # Initialize if this is the first sync
            if not last_sync:
                # Create metadata node
                self.memgraph.execute("""
                    CREATE (m:Metadata {key: 'last_follows_sync', timestamp: $timestamp})
                """, {'timestamp': datetime.now().isoformat()})
                
                # Get active follows (limited for first sync)
                query = self.supabase.table('follows') \
                               .select('*') \
                               .eq('follow_status', 'active') \
                               .order('created_at', desc=True) \
                               .limit(5000)
            else:
                # Get follows updated since last sync
                query = self.supabase.table('follows') \
                               .select('*') \
                               .gt('last_verified_at', last_sync) \
                               .order('last_verified_at')
            
            result = query.execute()
            follows = result.data
            
            # Process follows in batches
            batch_size = 500
            active_follows = [f for f in follows if f.get('follow_status') == 'active']
            unfollows = [f for f in follows if f.get('follow_status') == 'unfollowed']
            
            # Create FOLLOWS relationships for active follows
            for i in range(0, len(active_follows), batch_size):
                batch = active_follows[i:i+batch_size]
                params = {'follows': [self._prepare_follow_data(follow) for follow in batch]}
                
                self.memgraph.execute("""
                    UNWIND $follows AS follow
                    MATCH (follower:User {did: follow.follower_did})
                    MATCH (following:User {did: follow.following_did})
                    MERGE (follower)-[r:FOLLOWS]->(following)
                    SET 
                        r.created_at = follow.created_at,
                        r.last_verified_at = follow.last_verified_at
                """, params)
            
            # Remove FOLLOWS relationships for unfollows
            for i in range(0, len(unfollows), batch_size):
                batch = unfollows[i:i+batch_size]
                params = {'unfollows': [self._prepare_follow_data(follow) for follow in batch]}
                
                self.memgraph.execute("""
                    UNWIND $unfollows AS unfollow
                    MATCH (follower:User {did: unfollow.follower_did})
                    MATCH (following:User {did: unfollow.following_did})
                    MATCH (follower)-[r:FOLLOWS]->(following)
                    DELETE r
                """, params)
            
            # Update last sync timestamp
            if follows:
                self.memgraph.execute("""
                    MATCH (m:Metadata {key: 'last_follows_sync'})
                    SET m.timestamp = $timestamp
                """, {'timestamp': datetime.now().isoformat()})
            
            return len(follows)
            
        except Exception as e:
            logger.error(f"Error syncing follows: {e}")
            return 0
    
    def _prepare_follow_data(self, follow: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare follow data for Memgraph."""
        return {
            'follower_did': follow.get('follower_did'),
            'following_did': follow.get('following_did'),
            'created_at': follow.get('created_at') or datetime.now().isoformat(),
            'last_verified_at': follow.get('last_verified_at') or datetime.now().isoformat()
        }
    
    def _sync_posts(self) -> int:
        """Sync posts from Supabase to Memgraph."""
        try:
            # Get last sync timestamp from Memgraph
            result = self.memgraph.execute_and_fetch("""
                MATCH (m:Metadata {key: 'last_posts_sync'})
                RETURN m.timestamp AS timestamp
            """)
            
            last_sync = None
            for record in result:
                last_sync = record.get('timestamp')
            
            # Initialize if this is the first sync
            if not last_sync:
                # Create metadata node
                self.memgraph.execute("""
                    CREATE (m:Metadata {key: 'last_posts_sync', timestamp: $timestamp})
                """, {'timestamp': datetime.now().isoformat()})
                
                # Get recent posts (limited for first sync)
                query = self.supabase.table('post_references') \
                               .select('*') \
                               .order('indexed_at', desc=True) \
                               .limit(1000)
            else:
                # Get posts updated since last sync
                query = self.supabase.table('post_references') \
                               .select('*') \
                               .gt('indexed_at', last_sync) \
                               .order('indexed_at')
            
            result = query.execute()
            posts = result.data
            
            # Process posts in batches
            batch_size = 100
            for i in range(0, len(posts), batch_size):
                batch = posts[i:i+batch_size]
                params = {'posts': [self._prepare_post_data(post) for post in batch]}
                
                # Create post nodes
                self.memgraph.execute("""
                    UNWIND $posts AS post
                    MATCH (author:User {did: post.author_did})
                    MERGE (p:Post {uri: post.uri})
                    SET 
                        p.rkey = post.rkey,
                        p.created_at = post.created_at,
                        p.indexed_at = post.indexed_at,
                        p.sentiment_compound = post.sentiment_compound,
                        p.sentiment_positive = post.sentiment_positive,
                        p.sentiment_negative = post.sentiment_negative,
                        p.sentiment_neutral = post.sentiment_neutral
                    MERGE (author)-[:AUTHORED]->(p)
                """, params)
                
                # Process topics for each post
                for post in batch:
                    if post.get('topics'):
                        try:
                            topics = json.loads(post.get('topics'))
                            if topics:
                                topic_params = {
                                    'uri': post.get('uri'),
                                    'topics': topics
                                }
                                
                                # Create topic nodes and relationships
                                self.memgraph.execute("""
                                    MATCH (p:Post {uri: $uri})
                                    UNWIND $topics AS topic_name
                                    MERGE (t:Topic {name: topic_name})
                                    MERGE (p)-[:MENTIONS]->(t)
                                """, topic_params)
                        except Exception as e:
                            logger.error(f"Error processing topics for post {post.get('uri')}: {e}")
            
            # Update last sync timestamp
            if posts:
                self.memgraph.execute("""
                    MATCH (m:Metadata {key: 'last_posts_sync'})
                    SET m.timestamp = $timestamp
                """, {'timestamp': datetime.now().isoformat()})
            
            return len(posts)
            
        except Exception as e:
            logger.error(f"Error syncing posts: {e}")
            return 0
    
    def _prepare_post_data(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare post data for Memgraph."""
        return {
            'uri': post.get('uri'),
            'rkey': post.get('rkey'),
            'author_did': post.get('author_did'),
            'created_at': post.get('created_at') or datetime.now().isoformat(),
            'indexed_at': post.get('indexed_at') or datetime.now().isoformat(),
            'sentiment_compound': float(post.get('sentiment_compound') or 0),
            'sentiment_positive': float(post.get('sentiment_positive') or 0),
            'sentiment_negative': float(post.get('sentiment_negative') or 0),
            'sentiment_neutral': float(post.get('sentiment_neutral') or 0)
        }
    
    def analyze_graph(self):
        """Run graph analytics and store results."""
        try:
            logger.info("Running graph analytics")
            
            # Calculate PageRank for users
            self.memgraph.execute("""
                CALL pagerank.get()
                YIELD node, rank
                WHERE node:User
                SET node.pagerank = rank
            """)
            
            # Calculate community detection
            self.memgraph.execute("""
                CALL louvain.get()
                YIELD node, community
                WHERE node:User
                SET node.community = community
            """)
            
            # Calculate weighted sentiment per user
            self.memgraph.execute("""
                MATCH (u:User)-[:AUTHORED]->(p:Post)
                WITH 
                    u, 
                    AVG(p.sentiment_compound) AS avg_sentiment,
                    COUNT(p) AS post_count
                SET 
                    u.avg_sentiment = avg_sentiment,
                    u.post_count = post_count
            """)
            
            # Find and store topic affinities
            self.memgraph.execute("""
                MATCH (u:User)-[:AUTHORED]->(p:Post)-[:MENTIONS]->(t:Topic)
                WITH u, t, COUNT(p) AS mentions
                ORDER BY mentions DESC
                WITH u, COLLECT({topic: t.name, count: mentions})[0..5] AS top_topics
                SET u.top_topics = top_topics
            """)
            
            logger.info("Graph analytics completed")
            
        except Exception as e:
            logger.error(f"Error running graph analytics: {e}")


def main():
    """Main entry point for the Memgraph updater."""
    # Get configuration from environment
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    memgraph_host = os.environ.get('MEMGRAPH_HOST', 'localhost')
    memgraph_port = int(os.environ.get('MEMGRAPH_PORT', '7687'))
    sync_interval = int(os.environ.get('SYNC_INTERVAL', '60'))
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        return
    
    # Run updater
    updater = MemgraphUpdater(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        memgraph_host=memgraph_host,
        memgraph_port=memgraph_port
    )
    updater.start(sync_interval=sync_interval)


if __name__ == "__main__":
    main()
