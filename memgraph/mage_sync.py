#!/usr/bin/env python3
"""
Artifish Memgraph Sync using MAGE Migration Module

This script synchronizes data from Supabase to Memgraph using the MAGE migration module.
It supports both initial data loading and periodic updates.

Current scope:
- Bluesky accounts (users)
- Follow relationships

Usage:
    python mage_sync.py [--initial] [--force]

Options:
    --initial   Perform a full sync of all data (default: incremental)
    --force     Force sync even if recent sync was performed
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import neo4j
from neo4j import GraphDatabase

# Load environment variables from .env file
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

class MemgraphSync:
    """Synchronizes data from Supabase to Memgraph using MAGE migration."""
    
    def __init__(self):
        """Initialize the Memgraph sync."""
        # Memgraph connection details
        self.memgraph_uri = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
        self.memgraph_user = os.getenv("MEMGRAPH_USER", "")
        self.memgraph_password = os.getenv("MEMGRAPH_PASSWORD", "")
        
        # Supabase connection details
        self.supabase_db_host = os.getenv("SUPABASE_DB_HOST", "db.uqdfoqccbjfpftpvqwam.supabase.co")
        self.supabase_db_name = os.getenv("SUPABASE_DB_NAME", "postgres")
        self.supabase_db_user = os.getenv("SUPABASE_DB_USER", "postgres")
        self.supabase_db_password = os.getenv("SUPABASE_DB_PASSWORD", "")
        
        # Check if required environment variables are set
        if not self.supabase_db_password:
            logger.error("SUPABASE_DB_PASSWORD environment variable must be set")
            sys.exit(1)
            
        # Configuration
        self.batch_size = 1000
        self.sync_interval = timedelta(hours=1)  # Minimum time between syncs
        
        # Connect to Memgraph
        try:
            self.driver = GraphDatabase.driver(
                self.memgraph_uri,
                auth=(self.memgraph_user, self.memgraph_password) if self.memgraph_user else None
            )
            logger.info(f"Connected to Memgraph at {self.memgraph_uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Memgraph: {e}")
            sys.exit(1)
    
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
            for constraint in [
                "CREATE CONSTRAINT ON (u:User) ASSERT u.did IS UNIQUE",
                "CREATE CONSTRAINT ON (a:Artifish) ASSERT a.artifish_id IS UNIQUE"
            ]:
                try:
                    session.run(constraint)
                    logger.info(f"Created constraint: {constraint}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(f"Constraint already exists: {constraint}")
                    else:
                        logger.warning(f"Error creating constraint: {e}")
            
            # Create indexes for better query performance
            for index in [
                "CREATE INDEX ON :User(handle)",
                "CREATE INDEX ON :User(display_name)"
            ]:
                try:
                    session.run(index)
                    logger.info(f"Created index: {index}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(f"Index already exists: {index}")
                    else:
                        logger.warning(f"Error creating index: {e}")
    
    def check_last_sync(self, force=False):
        """
        Check when the last sync was performed.
        
        Args:
            force: Force sync even if recent sync was performed
            
        Returns:
            bool: True if sync should proceed, False otherwise
        """
        if force:
            return True
            
        with self.driver.session() as session:
            result = session.run("""
                MATCH (m:Metadata {key: 'last_sync'})
                RETURN m.timestamp AS timestamp
            """)
            
            record = result.single()
            if not record:
                return True
                
            try:
                last_sync = datetime.fromisoformat(record["timestamp"])
                now = datetime.now()
                
                if now - last_sync < self.sync_interval:
                    logger.info(f"Last sync was {now - last_sync} ago, skipping (use --force to override)")
                    return False
                    
                return True
            except Exception as e:
                logger.warning(f"Error parsing last sync timestamp: {e}")
                return True
    
    def update_sync_timestamp(self):
        """Update the sync timestamp metadata."""
        timestamp = datetime.now().isoformat()
        
        with self.driver.session() as session:
            # Check if metadata node exists
            result = session.run("""
                MATCH (m:Metadata {key: 'last_sync'})
                RETURN count(m) as count
            """)
            
            count = result.single()["count"]
            
            if count > 0:
                # Update existing node
                session.run("""
                    MATCH (m:Metadata {key: 'last_sync'})
                    SET m.timestamp = $timestamp
                """, {"timestamp": timestamp})
            else:
                # Create new metadata node
                session.run("""
                    CREATE (m:Metadata {
                        key: 'last_sync',
                        timestamp: $timestamp
                    })
                """, {"timestamp": timestamp})
                
            logger.info(f"Updated sync timestamp to {timestamp}")
    
    def sync_accounts(self, initial=False):
        """
        Sync Bluesky accounts from Supabase to Memgraph.
        
        Args:
            initial: Whether this is an initial full sync
        """
        logger.info(f"{'Initial' if initial else 'Incremental'} sync of Bluesky accounts")
        
        with self.driver.session() as session:
            # Get last sync timestamp if this is an incremental sync
            where_clause = ""
            if not initial:
                # Get the last sync timestamp
                result = session.run("""
                    MATCH (m:Metadata {key: 'last_accounts_sync'})
                    RETURN m.timestamp AS timestamp
                """)
                
                record = result.single()
                if record and record["timestamp"]:
                    last_sync = record["timestamp"]
                    where_clause = f"WHERE last_updated_at > '{last_sync}'"
                    logger.info(f"Syncing accounts updated since {last_sync}")
            
            # Use MAGE migration to import accounts
            try:
                query = f"""
                    CALL migrate.postgresql(
                        'SELECT did, handle, display_name, bio, avatar_url, last_updated_at 
                        FROM bluesky_accounts
                        {where_clause}
                        ORDER BY last_updated_at 
                        LIMIT {self.batch_size}',
                        {{
                            user: $user,
                            password: $password,
                            host: $host,
                            database: $database
                        }}
                    ) 
                    YIELD row
                    MERGE (u:User {{did: row.did}})
                    SET 
                        u.handle = row.handle,
                        u.display_name = row.display_name,
                        u.bio = row.bio,
                        u.avatar_url = row.avatar_url,
                        u.updated_at = row.last_updated_at
                    RETURN count(*) as count
                """
                
                result = session.run(
                    query,
                    {
                        "user": self.supabase_db_user,
                        "password": self.supabase_db_password,
                        "host": self.supabase_db_host,
                        "database": self.supabase_db_name
                    }
                )
                
                count = result.single()["count"]
                logger.info(f"Imported {count} Bluesky accounts")
                
                # Update the accounts sync timestamp
                timestamp = datetime.now().isoformat()
                session.run("""
                    MERGE (m:Metadata {key: 'last_accounts_sync'})
                    SET m.timestamp = $timestamp
                """, {"timestamp": timestamp})
                
                # Continue importing in batches if needed
                while count > 0 and count == self.batch_size:
                    # If we imported a full batch, there might be more to import
                    # Update the where clause to get the next batch
                    if not initial:
                        where_clause = f"WHERE last_updated_at > '{last_sync}'"
                    
                    query = f"""
                        CALL migrate.postgresql(
                            'SELECT did, handle, display_name, bio, avatar_url, last_updated_at 
                            FROM bluesky_accounts
                            {where_clause}
                            ORDER BY last_updated_at 
                            OFFSET {self.batch_size} 
                            LIMIT {self.batch_size}',
                            {{
                                user: $user,
                                password: $password,
                                host: $host,
                                database: $database
                            }}
                        ) 
                        YIELD row
                        MERGE (u:User {{did: row.did}})
                        SET 
                            u.handle = row.handle,
                            u.display_name = row.display_name,
                            u.bio = row.bio,
                            u.avatar_url = row.avatar_url,
                            u.updated_at = row.last_updated_at
                        RETURN count(*) as count
                    """
                    
                    result = session.run(
                        query,
                        {
                            "user": self.supabase_db_user,
                            "password": self.supabase_db_password,
                            "host": self.supabase_db_host,
                            "database": self.supabase_db_name
                        }
                    )
                    
                    count = result.single()["count"]
                    logger.info(f"Imported {count} additional Bluesky accounts")
                
            except Exception as e:
                logger.error(f"Error importing accounts: {e}")
    
    def sync_follows(self, initial=False):
        """
        Sync follow relationships from Supabase to Memgraph.
        
        Args:
            initial: Whether this is an initial full sync
        """
        logger.info(f"{'Initial' if initial else 'Incremental'} sync of follow relationships")
        
        with self.driver.session() as session:
            # Get last sync timestamp if this is an incremental sync
            where_clause = "WHERE follow_status = 'active'"
            if not initial:
                # Get the last sync timestamp
                result = session.run("""
                    MATCH (m:Metadata {key: 'last_follows_sync'})
                    RETURN m.timestamp AS timestamp
                """)
                
                record = result.single()
                if record and record["timestamp"]:
                    last_sync = record["timestamp"]
                    where_clause = f"WHERE follow_status = 'active' AND last_verified_at > '{last_sync}'"
                    logger.info(f"Syncing follows updated since {last_sync}")
            
            # Import active follows
            try:
                query = f"""
                    CALL migrate.postgresql(
                        'SELECT follower_did, following_did, created_at, last_verified_at
                        FROM follows
                        {where_clause}
                        ORDER BY last_verified_at 
                        LIMIT {self.batch_size}',
                        {{
                            user: $user,
                            password: $password,
                            host: $host,
                            database: $database
                        }}
                    ) 
                    YIELD row
                    MATCH (follower:User {{did: row.follower_did}})
                    MATCH (following:User {{did: row.following_did}})
                    MERGE (follower)-[r:FOLLOWS]->(following)
                    SET 
                        r.created_at = row.created_at,
                        r.last_verified_at = row.last_verified_at
                    RETURN count(*) as count
                """
                
                result = session.run(
                    query,
                    {
                        "user": self.supabase_db_user,
                        "password": self.supabase_db_password,
                        "host": self.supabase_db_host,
                        "database": self.supabase_db_name
                    }
                )
                
                count = result.single()["count"]
                logger.info(f"Imported {count} follow relationships")
                
                # Update the follows sync timestamp
                timestamp = datetime.now().isoformat()
                session.run("""
                    MERGE (m:Metadata {key: 'last_follows_sync'})
                    SET m.timestamp = $timestamp
                """, {"timestamp": timestamp})
                
                # Continue importing in batches if needed
                while count > 0 and count == self.batch_size:
                    # If we imported a full batch, there might be more to import
                    # Update the offset to get the next batch
                    query = f"""
                        CALL migrate.postgresql(
                            'SELECT follower_did, following_did, created_at, last_verified_at
                            FROM follows
                            {where_clause}
                            ORDER BY last_verified_at 
                            OFFSET {self.batch_size} 
                            LIMIT {self.batch_size}',
                            {{
                                user: $user,
                                password: $password,
                                host: $host,
                                database: $database
                            }}
                        ) 
                        YIELD row
                        MATCH (follower:User {{did: row.follower_did}})
                        MATCH (following:User {{did: row.following_did}})
                        MERGE (follower)-[r:FOLLOWS]->(following)
                        SET 
                            r.created_at = row.created_at,
                            r.last_verified_at = row.last_verified_at
                        RETURN count(*) as count
                    """
                    
                    result = session.run(
                        query,
                        {
                            "user": self.supabase_db_user,
                            "password": self.supabase_db_password,
                            "host": self.supabase_db_host,
                            "database": self.supabase_db_name
                        }
                    )
                    
                    count = result.single()["count"]
                    logger.info(f"Imported {count} additional follow relationships")
                
            except Exception as e:
                logger.error(f"Error importing follows: {e}")
            
            # Also handle unfollows if this is an incremental sync
            if not initial:
                try:
                    # Get unfollowed relationships
                    query = f"""
                        CALL migrate.postgresql(
                            'SELECT follower_did, following_did
                            FROM follows
                            WHERE follow_status = ''unfollowed'' AND unfollowed_at > ''{last_sync}''
                            LIMIT {self.batch_size}',
                            {{
                                user: $user,
                                password: $password,
                                host: $host,
                                database: $database
                            }}
                        ) 
                        YIELD row
                        MATCH (follower:User {{did: row.follower_did}})-[r:FOLLOWS]->(following:User {{did: row.following_did}})
                        DELETE r
                        RETURN count(*) as count
                    """
                    
                    result = session.run(
                        query,
                        {
                            "user": self.supabase_db_user,
                            "password": self.supabase_db_password,
                            "host": self.supabase_db_host,
                            "database": self.supabase_db_name
                        }
                    )
                    
                    count = result.single()["count"]
                    logger.info(f"Removed {count} unfollowed relationships")
                    
                except Exception as e:
                    logger.error(f"Error processing unfollows: {e}")
    
    def run(self, initial=False, force=False):
        """
        Run the synchronization process.
        
        Args:
            initial: Whether to perform an initial full sync
            force: Force sync even if recent sync was performed
        """
        # Check if we should proceed with sync
        if not self.check_last_sync(force):
            return
            
        try:
            # Set up schema
            self.setup_schema()
            
            # Switch to analytical mode for better import performance
            with self.driver.session() as session:
                session.run("STORAGE MODE IN_MEMORY_ANALYTICAL")
            
            # Sync accounts
            self.sync_accounts(initial)
            
            # Sync follows
            self.sync_follows(initial)
            
            # Switch back to transactional mode
            with self.driver.session() as session:
                session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
            
            # Update the overall sync timestamp
            self.update_sync_timestamp()
            
            # Print some stats
            with self.driver.session() as session:
                # Count users
                result = session.run("MATCH (u:User) RETURN count(u) as count")
                user_count = result.single()["count"]
                
                # Count follow relationships
                result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as count")
                follow_count = result.single()["count"]
                
                logger.info(f"Sync completed: {user_count} users, {follow_count} follow relationships")
            
        except Exception as e:
            logger.error(f"Error during sync: {e}")
        
        finally:
            # Make sure we're back in transactional mode
            try:
                with self.driver.session() as session:
                    session.run("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
            except:
                pass


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Memgraph Sync using MAGE Migration')
    parser.add_argument('--initial', action='store_true', help='Perform initial full sync')
    parser.add_argument('--force', action='store_true', help='Force sync even if recent sync was performed')
    args = parser.parse_args()
    
    sync = MemgraphSync()
    
    try:
        sync.run(initial=args.initial, force=args.force)
    finally:
        sync.close()


if __name__ == "__main__":
    main()
