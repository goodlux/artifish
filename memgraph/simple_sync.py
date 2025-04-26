#!/usr/bin/env python3
"""
Simple Memgraph Synchronization Script

This script syncs bluesky_accounts and follows from Supabase to Memgraph
using the existing Supabase API client. It's designed to be run periodically
to keep the graph database updated with the latest social network data.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import neo4j
from neo4j import GraphDatabase
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("memgraph_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MemgraphSync")

class MemgraphSyncSimple:
    """Simple synchronization from Supabase to Memgraph for Bluesky data."""
    
    def __init__(self, force_full_sync=True):
        """Initialize the sync with Supabase and Memgraph connections."""
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
        self.batch_size = 500
        self.min_sync_interval = timedelta(hours=1)
        self.force_full_sync = force_full_sync
    
    def close(self):
        """Close the Memgraph connection."""
        if hasattr(self, 'driver'):
            self.driver.close()
    
    def setup_schema(self):
        """Set up the initial schema with constraints and indexes."""
        logger.info("Setting up Memgraph schema")
        
        with self.driver.session() as session:
            # Switch to analytical mode for better import performance
            session.run("STORAGE MODE IN_MEMORY_ANALYTICAL")
            
            # Create constraints for main entity types
            constraints = [
                "CREATE CONSTRAINT ON (u:User) ASSERT u.did IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(f"Constraint already exists: {constraint}")
                    else:
                        logger.warning(f"Error creating constraint: {e}")
            
            # Create indexes for better query performance
            indexes = [
                "CREATE INDEX ON :User(handle)"
            ]
            
            for index in indexes:
                try:
                    session.run(index)
                    logger.info(f"Created index: {index}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(f"Index already exists: {index}")
                    else:
                        logger.warning(f"Error creating index: {e}")
    
    def should_sync(self, sync_type: str) -> bool:
        """Check if we should sync based on last sync time."""
        # Always sync if force_full_sync is set
        if self.force_full_sync:
            logger.info(f"Forcing full sync for {sync_type}")
            return True
            
        with self.driver.session() as session:
            result = session.run(
                f"MATCH (m:Metadata {{key: 'last_{sync_type}_sync'}}) RETURN m.timestamp as timestamp"
            )
            
            record = result.single()
            if not record:
                return True
                
            try:
                last_sync = datetime.fromisoformat(record["timestamp"])
                now = datetime.now()
                
                if now - last_sync < self.min_sync_interval:
                    logger.info(f"Last {sync_type} sync was {now - last_sync} ago, skipping")
                    return False
                    
                return True
            except Exception as e:
                logger.warning(f"Error checking last sync time: {e}")
                return True
    
    def update_sync_timestamp(self, sync_type: str):
        """Update the sync timestamp for a specific type."""
        timestamp = datetime.now().isoformat()
        
        with self.driver.session() as session:
            session.run(
                f"""
                MERGE (m:Metadata {{key: 'last_{sync_type}_sync'}})
                SET m.timestamp = $timestamp
                """,
                {"timestamp": timestamp}
            )
            
        logger.info(f"Updated {sync_type} sync timestamp to {timestamp}")
    
    def reset_sync_timestamps(self):
        """Reset all sync timestamps to force a full sync."""
        logger.info("Resetting all sync timestamps")
        
        with self.driver.session() as session:
            session.run("MATCH (m:Metadata) DETACH DELETE m")
    
    def get_node_count(self):
        """Get current node count from Memgraph."""
        with self.driver.session() as session:
            result = session.run("MATCH (u:User) RETURN count(u) as user_count")
            user_count = result.single()["user_count"]
            
            result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as follow_count")
            follow_count = result.single()["follow_count"]
            
            return user_count, follow_count
    
    def sync_accounts(self):
        """Sync Bluesky accounts from Supabase to Memgraph."""
        if not self.should_sync("accounts"):
            return
            
        logger.info("Starting accounts sync")
        initial_count, _ = self.get_node_count()
        logger.info(f"Starting with {initial_count} user nodes")
        
        try:
            # Get the timestamp of the last sync
            last_sync = None
            if not self.force_full_sync:
                with self.driver.session() as session:
                    result = session.run(
                        "MATCH (m:Metadata {key: 'last_accounts_sync'}) RETURN m.timestamp as timestamp"
                    )
                    
                    record = result.single()
                    if record and record["timestamp"]:
                        last_sync = record["timestamp"]
                        logger.info(f"Syncing accounts updated since {last_sync}")
            
            # Full sync approach - paginate through all accounts
            total_accounts = 0
            offset = 0
            has_more = True
            total_batches_processed = 0
            
            # First, get a count of all accounts
            count_query = self.supabase.table('bluesky_accounts').select('count', count='exact')
            if last_sync:
                count_query = count_query.gt('last_updated_at', last_sync)
            count_response = count_query.execute()
            total_expected = count_response.count
            logger.info(f"Found {total_expected} total accounts to sync")
            
            # Process accounts in batches
            while has_more:
                start_time = time.time()
                
                # Fetch a batch of accounts with pagination
                query = self.supabase.table('bluesky_accounts').select('*')
                if last_sync:
                    query = query.gt('last_updated_at', last_sync)
                query = query.range(offset, offset + self.batch_size - 1)
                
                # Execute the query
                response = query.execute()
                accounts_batch = response.data
                batch_size = len(accounts_batch)
                
                logger.info(f"Fetched {batch_size} accounts (offset: {offset})")
                total_accounts += batch_size
                
                # Process this batch
                if batch_size > 0:
                    batch_result = self._process_accounts_batch(accounts_batch)
                    total_batches_processed += 1
                    
                    # Logging for monitoring progress
                    elapsed = time.time() - start_time
                    current_count, _ = self.get_node_count()
                    logger.info(f"Batch {total_batches_processed}: {batch_result} accounts processed in {elapsed:.2f}s. Total nodes: {current_count}")
                    
                    # Calculate progress percentage
                    progress = (total_accounts / total_expected) * 100 if total_expected > 0 else 0
                    logger.info(f"Progress: {progress:.1f}% ({total_accounts}/{total_expected})")
                
                # Check if there are more accounts to fetch
                has_more = batch_size == self.batch_size
                offset += self.batch_size
                
                # Add a small delay to avoid overwhelming the Supabase API
                if has_more:
                    time.sleep(0.2)
            
            # Final count check
            final_count, _ = self.get_node_count()
            logger.info(f"Completed full sync of {total_accounts} accounts. Nodes before: {initial_count}, after: {final_count}")
            
            # Update the sync timestamp
            self.update_sync_timestamp("accounts")
            
        except Exception as e:
            logger.error(f"Error syncing accounts: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _process_accounts_batch(self, accounts):
        """Process a batch of accounts and update Memgraph."""
        if not accounts:
            return 0
            
        batch_accounts = []
        
        # Prepare batch data
        for account in accounts:
            batch_accounts.append({
                'did': account.get('did'),
                'handle': account.get('handle'),
                'display_name': account.get('display_name') or '',
                'bio': account.get('bio') or '',
                'avatar_url': account.get('avatar_url') or '',
                'updated_at': account.get('last_updated_at')
            })
        
        # Update Memgraph - Use MERGE instead of MATCH to handle both creation and updates
        try:
            with self.driver.session() as session:
                result = session.run(
                    """
                    UNWIND $accounts AS account
                    MERGE (u:User {did: account.did})
                    SET u.handle = account.handle,
                        u.display_name = account.display_name,
                        u.bio = account.bio,
                        u.avatar_url = account.avatar_url,
                        u.updated_at = account.updated_at
                    RETURN count(*) as updated
                    """,
                    {"accounts": batch_accounts}
                )
                
                updated = result.single()["updated"]
                logger.info(f"Updated {updated} accounts in Memgraph")
                return updated
        except Exception as e:
            logger.error(f"Error processing accounts batch: {e}")
            logger.error(f"First account in batch: {batch_accounts[0] if batch_accounts else 'None'}")
            import traceback
            logger.error(traceback.format_exc())
            return 0
    
    def sync_follows(self):
        """Sync follow relationships from Supabase to Memgraph."""
        if not self.should_sync("follows"):
            return
            
        logger.info("Starting follows sync")
        _, initial_follows = self.get_node_count()
        logger.info(f"Starting with {initial_follows} follow relationships")
        
        try:
            # Create an index to store all DIDs we've already processed
            # This helps us track which users exist in the database
            user_dids = set()
            with self.driver.session() as session:
                # Get all existing DIDs from the database
                result = session.run(
                    """
                    MATCH (u:User) 
                    RETURN collect(u.did) as dids
                    """
                )
                dids_from_db = result.single()["dids"]
                user_dids.update(dids_from_db)
                logger.info(f"Found {len(user_dids)} existing users in Memgraph")
                
            # Get the timestamp of the last sync
            last_sync = None
            if not self.force_full_sync:
                with self.driver.session() as session:
                    result = session.run(
                        "MATCH (m:Metadata {key: 'last_follows_sync'}) RETURN m.timestamp as timestamp"
                    )
                    
                    record = result.single()
                    if record and record["timestamp"]:
                        last_sync = record["timestamp"]
                        logger.info(f"Syncing follows updated since {last_sync}")
            
            # Process all the follow activity
            total_follows = 0
            offset = 0
            has_more = True
            total_batches_processed = 0
            
            # First, get a count of all follows
            count_query = self.supabase.from_('follow_activity').select('count', count='exact')
            if last_sync:
                count_query = count_query.or_(f"follow_status.eq.active,unfollowed_at.gt.{last_sync}")
            count_response = count_query.execute()
            total_expected = count_response.count
            logger.info(f"Found {total_expected} total follows to sync")

            # Process follows in batches
            while has_more:
                start_time = time.time()
                
                # Fetch a batch of follows from the view
                query = self.supabase.from_('follow_activity').select('*')
                
                # If we have a last sync, only get active follows and unfollows after that time
                if last_sync:
                    query = query.or_(f"follow_status.eq.active,unfollowed_at.gt.{last_sync}")
                    
                # Add pagination
                query = query.range(offset, offset + self.batch_size - 1)
                
                # Execute the query
                response = query.execute()
                follows_batch = response.data
                batch_size = len(follows_batch)
                
                logger.info(f"Fetched {batch_size} follow activities (offset: {offset})")
                total_follows += batch_size
                
                # Process this batch
                if batch_size > 0:
                    # First ensure all users exist
                    missing_users = self._ensure_users_exist(follows_batch, user_dids)
                    # Then create the relationships
                    batch_result = self._process_follow_activity_batch(follows_batch)
                    total_batches_processed += 1
                    
                    # Logging for monitoring progress
                    elapsed = time.time() - start_time
                    _, current_follows = self.get_node_count()
                    logger.info(f"Batch {total_batches_processed}: Created {missing_users} missing users, {batch_result} follows processed in {elapsed:.2f}s. Total relationships: {current_follows}")
                    
                    # Calculate progress percentage
                    progress = (total_follows / total_expected) * 100 if total_expected > 0 else 0
                    logger.info(f"Progress: {progress:.1f}% ({total_follows}/{total_expected})")
                
                # Check if there are more follows to fetch
                has_more = batch_size == self.batch_size
                offset += self.batch_size
                
                # Add a small delay to avoid overwhelming the Supabase API
                if has_more:
                    time.sleep(0.2)
            
            # Final count check
            _, final_follows = self.get_node_count()
            logger.info(f"Completed sync of {total_follows} follow activities. Relationships before: {initial_follows}, after: {final_follows}")
            
            # Update the sync timestamp
            self.update_sync_timestamp("follows")
            
        except Exception as e:
            logger.error(f"Error syncing follows: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _ensure_users_exist(self, follows, existing_dids):
        """Ensure all users from the follows batch exist in Memgraph."""
        # Extract user information
        users_to_create = []
        for follow in follows:
            # Extract follower
            follower_did = follow.get('follower_did')
            if follower_did and follower_did not in existing_dids:
                users_to_create.append({
                    'did': follower_did,
                    'handle': follow.get('follower_handle')
                })
                existing_dids.add(follower_did)
            
            # Extract following
            following_did = follow.get('following_did')
            if following_did and following_did not in existing_dids:
                users_to_create.append({
                    'did': following_did,
                    'handle': follow.get('following_handle')
                })
                existing_dids.add(following_did)
        
        # Create any missing users
        if users_to_create:
            logger.info(f"Creating {len(users_to_create)} missing users")
            
            try:
                with self.driver.session() as session:
                    result = session.run(
                        """
                        UNWIND $users AS user
                        MERGE (u:User {did: user.did})
                        SET u.handle = user.handle
                        RETURN count(*) as created
                        """,
                        {"users": users_to_create}
                    )
                    created = result.single()["created"]
                    return created
            except Exception as e:
                logger.error(f"Error creating missing users: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return 0
        
        return 0
    
    def _process_follow_activity_batch(self, follows):
        """Process a batch of follow activities and update Memgraph."""
        if not follows:
            return 0
            
        # Group follows by status
        active_follows = []
        unfollows = []
        
        # Prepare data for active follows and unfollows
        for follow in follows:
            follow_status = follow.get('follow_status')
            
            # Get common properties
            follow_data = {
                'follower_did': follow.get('follower_did'),
                'following_did': follow.get('following_did'),
                'follower_handle': follow.get('follower_handle', ''),
                'following_handle': follow.get('following_handle', ''),
                'created_at': follow.get('created_at'),
                'last_verified_at': follow.get('last_verified_at'),
                'activity_type': follow.get('activity_type')
            }
            
            if follow_status == 'active':
                active_follows.append(follow_data)
            else:
                unfollows.append(follow_data)
        
        # Process active follows - using MERGE instead of MATCH to ensure relationships are created
        active_count = 0
        if active_follows:
            try:
                with self.driver.session() as session:
                    result = session.run(
                        """
                        UNWIND $follows AS follow
                        MATCH (follower:User {did: follow.follower_did})
                        MATCH (following:User {did: follow.following_did})
                        MERGE (follower)-[r:FOLLOWS]->(following)
                        SET r.created_at = follow.created_at,
                            r.last_verified_at = follow.last_verified_at,
                            r.activity_type = follow.activity_type,
                            r.follower_handle = follow.follower_handle,
                            r.following_handle = follow.following_handle
                        RETURN count(*) as updated
                        """,
                        {"follows": active_follows}
                    )
                    
                    active_count = result.single()["updated"]
                    logger.info(f"Updated {active_count} active follows in Memgraph")
            except Exception as e:
                logger.error(f"Error processing active follows: {e}")
                logger.error(f"First follow in batch: {active_follows[0] if active_follows else 'None'}")
                import traceback
                logger.error(traceback.format_exc())
        
        # Process unfollows
        unfollow_count = 0
        if unfollows:
            try:
                with self.driver.session() as session:
                    result = session.run(
                        """
                        UNWIND $unfollows AS unfollow
                        MATCH (follower:User {did: unfollow.follower_did})
                        -[r:FOLLOWS]->
                        (following:User {did: unfollow.following_did})
                        DELETE r
                        RETURN count(*) as deleted
                        """,
                        {"unfollows": unfollows}
                    )
                    
                    unfollow_count = result.single()["deleted"] 
                    logger.info(f"Deleted {unfollow_count} unfollows in Memgraph")
            except Exception as e:
                logger.error(f"Error processing unfollows: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        return active_count + unfollow_count
    
    def run(self):
        """Run the synchronization process."""
        try:
            start_time = time.time()
            logger.info("Starting Memgraph sync process")
            
            if self.force_full_sync:
                self.reset_sync_timestamps()
            
            # Set up schema
            self.setup_schema()
            
            # Switch to analytical mode for better import performance
            with self.driver.session() as session:
                session.run("STORAGE MODE IN_MEMORY_ANALYTICAL")
            
            # First sync all user accounts
            account_sync_start = time.time()
            self.sync_accounts()
            account_sync_time = time.time() - account_sync_start
            logger.info(f"Account sync completed in {account_sync_time:.2f} seconds")
            
            # Then sync all follow relationships
            follow_sync_start = time.time()
            self.sync_follows()
            follow_sync_time = time.time() - follow_sync_start
            logger.info(f"Follow sync completed in {follow_sync_time:.2f} seconds")
            
            # Switch back to transactional mode
            with self.driver.session() as session:
                session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
            
            # Print some stats
            with self.driver.session() as session:
                # Count users
                result = session.run("MATCH (u:User) RETURN count(u) as count")
                user_count = result.single()["count"]
                
                # Count follow relationships
                result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
                follow_count = result.single()["count"]
                
                # Total execution time
                total_time = time.time() - start_time
                
                logger.info(f"Sync completed in {total_time:.2f} seconds: {user_count} users, {follow_count} follow relationships")
            
        except Exception as e:
            logger.error(f"Error during sync: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        finally:
            # Make sure we're back in transactional mode
            try:
                with self.driver.session() as session:
                    session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
            except:
                pass

def main():
    """Main entry point."""
    sync = MemgraphSyncSimple(force_full_sync=True)
    
    try:
        sync.run()
    finally:
        sync.close()

if __name__ == "__main__":
    main()
