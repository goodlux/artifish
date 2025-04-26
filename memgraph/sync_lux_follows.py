#!/usr/bin/env python3
"""
Special script to sync Lux's follow relationships to Memgraph.

This script specifically focuses on creating follow relationships for
lux.bsky.social that weren't properly created in the main sync.
"""

import os
import sys
import time
import logging
from datetime import datetime

import neo4j
from neo4j import GraphDatabase
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("lux_follows_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LuxFollowsSync")

class LuxFollowsSync:
    """Sync Lux's follow relationships to Memgraph."""
    
    def __init__(self):
        """Initialize connections to Supabase and Memgraph."""
        # Supabase connection
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            logger.error("SUPABASE_URL and SUPABASE_KEY must be set in the environment")
            sys.exit(1)
            
        self.supabase = create_client(supabase_url, supabase_key)
        logger.info(f"Connected to Supabase at {supabase_url}")
        
        # Memgraph connection
        memgraph_uri = os.getenv('MEMGRAPH_URI', 'bolt://localhost:7687')
        memgraph_user = os.getenv('MEMGRAPH_USER', '')
        memgraph_password = os.getenv('MEMGRAPH_PASSWORD', '')
        
        try:
            auth = None
            if memgraph_user and memgraph_password:
                auth = (memgraph_user, memgraph_password)
                
            self.driver = GraphDatabase.driver(memgraph_uri, auth=auth)
            logger.info(f"Connected to Memgraph at {memgraph_uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Memgraph: {e}")
            sys.exit(1)
        
        # Configuration
        self.batch_size = 100
        self.lux_did = "did:plc:hhtah7oh3r4vq3jrn5iuy7hm"
        self.lux_handle = "lux.bsky.social"
    
    def close(self):
        """Close connections."""
        if hasattr(self, 'driver'):
            self.driver.close()
    
    def sync_lux_follows(self):
        """Sync Lux's follow relationships."""
        logger.info(f"Starting sync for {self.lux_handle} follow relationships")
        
        try:
            # First, ensure Lux's node exists
            with self.driver.session() as session:
                # Get Lux's account data
                lux_query = self.supabase.table('bluesky_accounts').select('*').eq('did', self.lux_did)
                lux_response = lux_query.execute()
                
                if not lux_response.data:
                    logger.error(f"Could not find {self.lux_handle} in Supabase")
                    return
                
                lux_account = lux_response.data[0]
                
                # Ensure Lux's node exists
                session.run(
                    """
                    MERGE (lux:User {did: $did})
                    SET lux.handle = $handle,
                        lux.display_name = $display_name,
                        lux.bio = $bio,
                        lux.avatar_url = $avatar_url,
                        lux.updated_at = $updated_at
                    """,
                    {
                        'did': lux_account.get('did'),
                        'handle': lux_account.get('handle'),
                        'display_name': lux_account.get('display_name') or '',
                        'bio': lux_account.get('bio') or '',
                        'avatar_url': lux_account.get('avatar_url') or '',
                        'updated_at': lux_account.get('last_updated_at')
                    }
                )
                logger.info(f"Ensured {self.lux_handle} node exists")
            
            # Get all of Lux's follows
            follows_query = self.supabase.from_('follow_activity').select('*') \
                .or_(f"follower_did.eq.{self.lux_did},following_did.eq.{self.lux_did}") \
                .eq('follow_status', 'active')
            follows_response = follows_query.execute()
            follows = follows_response.data
            
            logger.info(f"Found {len(follows)} active follow relationships for {self.lux_handle}")
            
            # Process in batches
            for i in range(0, len(follows), self.batch_size):
                batch = follows[i:i+self.batch_size]
                self._process_follow_batch(batch)
                logger.info(f"Processed batch {i//self.batch_size + 1}/{(len(follows) + self.batch_size - 1)//self.batch_size}")
                time.sleep(0.1)
            
            # Check results
            with self.driver.session() as session:
                result = session.run(
                    f"""
                    MATCH (lux:User {{did: '{self.lux_did}'}})
                    MATCH (lux)<-[r1:FOLLOWS]-(follower:User)
                    MATCH (lux)-[r2:FOLLOWS]->(following:User)
                    RETURN count(DISTINCT follower) as follower_count, 
                           count(DISTINCT following) as following_count
                    """
                )
                counts = result.single()
                logger.info(f"Successfully synced: {counts['follower_count']} followers, {counts['following_count']} following")
            
        except Exception as e:
            logger.error(f"Error syncing {self.lux_handle} follows: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _process_follow_batch(self, follows):
        """Process a batch of follows and create relationships."""
        # Create followers
        outgoing_follows = []  # Lux follows these
        incoming_follows = []  # These follow Lux
        
        for follow in follows:
            follower_did = follow.get('follower_did')
            following_did = follow.get('following_did')
            
            if follower_did == self.lux_did:
                # Lux follows someone
                outgoing_follows.append({
                    'did': following_did,
                    'handle': follow.get('following_handle'),
                    'created_at': follow.get('created_at'),
                    'last_verified_at': follow.get('last_verified_at'),
                    'activity_type': follow.get('activity_type')
                })
            elif following_did == self.lux_did:
                # Someone follows Lux
                incoming_follows.append({
                    'did': follower_did,
                    'handle': follow.get('follower_handle'),
                    'created_at': follow.get('created_at'),
                    'last_verified_at': follow.get('last_verified_at'),
                    'activity_type': follow.get('activity_type')
                })
        
        # Create relationships in Memgraph
        with self.driver.session() as session:
            if incoming_follows:
                result = session.run(
                    f"""
                    UNWIND $followers AS f
                    MERGE (follower:User {{did: f.did}})
                    SET follower.handle = f.handle
                    MERGE (lux:User {{did: '{self.lux_did}'}})
                    MERGE (follower)-[r:FOLLOWS]->(lux)
                    SET r.created_at = f.created_at,
                        r.last_verified_at = f.last_verified_at,
                        r.activity_type = f.activity_type
                    RETURN count(*) as created
                    """,
                    {"followers": incoming_follows}
                )
                created = result.single()["created"]
                logger.info(f"Created {created} incoming follow relationships")
            
            if outgoing_follows:
                result = session.run(
                    f"""
                    UNWIND $followings AS f
                    MERGE (following:User {{did: f.did}})
                    SET following.handle = f.handle
                    MERGE (lux:User {{did: '{self.lux_did}'}})
                    MERGE (lux)-[r:FOLLOWS]->(following)
                    SET r.created_at = f.created_at,
                        r.last_verified_at = f.last_verified_at,
                        r.activity_type = f.activity_type
                    RETURN count(*) as created
                    """,
                    {"followings": outgoing_follows}
                )
                created = result.single()["created"]
                logger.info(f"Created {created} outgoing follow relationships")

def main():
    """Main entry point."""
    sync = LuxFollowsSync()
    
    try:
        sync.sync_lux_follows()
    finally:
        sync.close()

if __name__ == "__main__":
    main()
