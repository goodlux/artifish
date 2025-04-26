#!/usr/bin/env python3
"""
Supabase to Memgraph Migration Tool

This script uses the Supabase Python client and the Neo4j Python driver (compatible with Memgraph)
to efficiently migrate data from a Supabase PostgreSQL database to a Memgraph graph database.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Tuple

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
        logging.FileHandler("supabase_to_memgraph.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SupabaseToMemgraph")

class SupabaseToMemgraph:
    """Tool to migrate data from Supabase to Memgraph."""
    
    def __init__(self, force_full_sync=True):
        """Initialize connections to Supabase and Memgraph."""
        # Supabase connection
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            logger.error("SUPABASE_URL and SUPABASE_KEY must be set in the environment")
            sys.exit(1)
            
        self.supabase = create_client(self.supabase_url, self.supabase_key)
        logger.info(f"Connected to Supabase at {self.supabase_url}")
        
        # Memgraph connection
        self.memgraph_uri = os.getenv('MEMGRAPH_URI', 'bolt://localhost:7687')
        self.memgraph_user = os.getenv('MEMGRAPH_USER', '')
        self.memgraph_password = os.getenv('MEMGRAPH_PASSWORD', '')
        
        try:
            auth = None
            if self.memgraph_user and self.memgraph_password:
                auth = (self.memgraph_user, self.memgraph_password)
                
            self.driver = GraphDatabase.driver(self.memgraph_uri, auth=auth)
            logger.info(f"Connected to Memgraph at {self.memgraph_uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Memgraph: {e}")
            sys.exit(1)
        
        # Configuration
        self.batch_size = 500
        self.force_full_sync = force_full_sync
        self.timeout = 300  # Timeout for Memgraph operations in seconds
    
    def close(self):
        """Close connections."""
        if hasattr(self, 'driver'):
            self.driver.close()
    
    def setup_schema(self):
        """Set up the Memgraph schema with constraints and indexes."""
        logger.info("Setting up Memgraph schema")
        
        with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
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
    
    def reset_sync_timestamps(self):
        """Reset all sync timestamps in Memgraph."""
        logger.info("Resetting sync timestamps")
        
        with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
            session.run("MATCH (m:Metadata) DETACH DELETE m")
    
    def update_sync_timestamp(self, sync_type: str):
        """Update the sync timestamp for a specific type."""
        timestamp = datetime.now().isoformat()
        
        with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
            session.run(
                f"""
                MERGE (m:Metadata {{key: 'last_{sync_type}_sync'}})
                SET m.timestamp = $timestamp
                """,
                {"timestamp": timestamp}
            )
            
        logger.info(f"Updated {sync_type} sync timestamp to {timestamp}")
    
    def get_node_count(self) -> Tuple[int, int]:
        """Get current node and relationship counts from Memgraph."""
        with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
            result = session.run("MATCH (u:User) RETURN count(u) as user_count")
            user_count = result.single()["user_count"]
            
            result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as follow_count")
            follow_count = result.single()["follow_count"]
            
            return user_count, follow_count
    
    def migrate_accounts(self):
        """Migrate user accounts from Supabase to Memgraph."""
        logger.info("Starting account migration")
        initial_count, _ = self.get_node_count()
        logger.info(f"Starting with {initial_count} user nodes")
        
        try:
            # Get total number of accounts for progress tracking
            count_query = self.supabase.table('bluesky_accounts').select('count', count='exact')
            count_response = count_query.execute()
            total_expected = count_response.count
            logger.info(f"Found {total_expected} total accounts to migrate")
            
            # Process accounts in batches
            total_accounts = 0
            offset = 0
            has_more = True
            total_batches_processed = 0
            
            while has_more:
                start_time = time.time()
                
                # Fetch a batch of accounts with pagination
                query = self.supabase.table('bluesky_accounts').select('*').range(offset, offset + self.batch_size - 1)
                response = query.execute()
                accounts_batch = response.data
                batch_size = len(accounts_batch)
                
                logger.info(f"Fetched {batch_size} accounts (offset: {offset})")
                total_accounts += batch_size
                
                # Process this batch
                if batch_size > 0:
                    processed = self._process_accounts_batch(accounts_batch)
                    total_batches_processed += 1
                    
                    # Logging for monitoring progress
                    elapsed = time.time() - start_time
                    current_count, _ = self.get_node_count()
                    logger.info(f"Batch {total_batches_processed}: {processed} accounts processed in {elapsed:.2f}s. Total nodes: {current_count}")
                    
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
            logger.info(f"Completed migration of {total_accounts} accounts. Nodes before: {initial_count}, after: {final_count}")
            
            # Update the sync timestamp
            self.update_sync_timestamp("accounts")
            
        except Exception as e:
            logger.error(f"Error migrating accounts: {e}")
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
        
        # Update Memgraph with batched user creation
        try:
            with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
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
    
    def migrate_follows(self):
        """Migrate follow relationships from Supabase to Memgraph."""
        logger.info("Starting follows migration")
        _, initial_follows = self.get_node_count()
        logger.info(f"Starting with {initial_follows} follow relationships")
        
        try:
            # Create an efficient in-memory tracking set for existing users
            user_dids = set()
            with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
                result = session.run("MATCH (u:User) RETURN collect(u.did) as dids")
                dids_from_db = result.single()["dids"]
                user_dids.update(dids_from_db)
                logger.info(f"Found {len(user_dids)} existing users in Memgraph")
            
            # Get total number of follows for progress tracking
            count_query = self.supabase.from_('follow_activity').select('count', count='exact').eq('follow_status', 'active')
            count_response = count_query.execute()
            total_expected = count_response.count
            logger.info(f"Found {total_expected} total active follows to migrate")
            
            # Process follows in batches
            total_follows = 0
            offset = 0
            has_more = True
            total_batches_processed = 0
            
            while has_more:
                start_time = time.time()
                
                # Fetch a batch of follows from the view
                query = self.supabase.from_('follow_activity').select('*').eq('follow_status', 'active').range(offset, offset + self.batch_size - 1)
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
                    processed = self._process_follow_batch(follows_batch)
                    total_batches_processed += 1
                    
                    # Logging for monitoring progress
                    elapsed = time.time() - start_time
                    _, current_follows = self.get_node_count()
                    logger.info(f"Batch {total_batches_processed}: Created {missing_users} missing users, {processed} follows processed in {elapsed:.2f}s. Total relationships: {current_follows}")
                    
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
            logger.info(f"Completed migration of {total_follows} follows. Relationships before: {initial_follows}, after: {final_follows}")
            
            # Update the sync timestamp
            self.update_sync_timestamp("follows")
            
        except Exception as e:
            logger.error(f"Error migrating follows: {e}")
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
                with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
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
    
    def _process_follow_batch(self, follows):
        """Process a batch of follow relationships and update Memgraph."""
        if not follows:
            return 0
            
        # Prepare active follows data
        active_follows = []
        
        for follow in follows:
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
            
            active_follows.append(follow_data)
        
        # Process active follows
        try:
            with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
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
                
                updated = result.single()["updated"]
                logger.info(f"Updated {updated} active follows in Memgraph")
                return updated
        except Exception as e:
            logger.error(f"Error processing active follows: {e}")
            logger.error(f"First follow in batch: {active_follows[0] if active_follows else 'None'}")
            import traceback
            logger.error(traceback.format_exc())
            return 0
    
    def run(self):
        """Run the migration process."""
        try:
            start_time = time.time()
            logger.info("Starting Supabase to Memgraph migration")
            
            if self.force_full_sync:
                self.reset_sync_timestamps()
            
            # Set up schema
            self.setup_schema()
            
            # Switch to analytical mode for better import performance
            with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
                session.run("STORAGE MODE IN_MEMORY_ANALYTICAL")
            
            # First migrate all user accounts
            account_sync_start = time.time()
            self.migrate_accounts()
            account_sync_time = time.time() - account_sync_start
            logger.info(f"Account migration completed in {account_sync_time:.2f} seconds")
            
            # Then migrate all follow relationships
            follow_sync_start = time.time()
            self.migrate_follows()
            follow_sync_time = time.time() - follow_sync_start
            logger.info(f"Follow migration completed in {follow_sync_time:.2f} seconds")
            
            # Switch back to transactional mode
            with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
                session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
            
            # Print some stats
            with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
                # Count users
                result = session.run("MATCH (u:User) RETURN count(u) as count")
                user_count = result.single()["count"]
                
                # Count follow relationships
                result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
                follow_count = result.single()["count"]
                
                # Total execution time
                total_time = time.time() - start_time
                
                logger.info(f"Migration completed in {total_time:.2f} seconds: {user_count} users, {follow_count} follow relationships")
            
        except Exception as e:
            logger.error(f"Error during migration: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        finally:
            # Make sure we're back in transactional mode
            try:
                with self.driver.session(database="", max_transaction_retry_time=self.timeout) as session:
                    session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
            except:
                pass

def main():
    """Main entry point."""
    migrator = SupabaseToMemgraph(force_full_sync=True)
    
    try:
        migrator.run()
    finally:
        migrator.close()

if __name__ == "__main__":
    main()
