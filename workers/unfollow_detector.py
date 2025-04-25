"""
Bluesky Unfollow Detector

This script detects unfollows by comparing the current follows from the API
with the follows stored in the database. It marks unfollowed relationships
in the database for tracking and analysis.
"""

import asyncio
import logging
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any, Set
from dotenv import load_dotenv  # Added dotenv import
from supabase import create_client, Client
import aiohttp

# Load environment variables from .env file
load_dotenv()  # Added this line to load .env file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("unfollow_detector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("UnfollowDetector")

class BlueskyAPIClient:
    """Simple client for interacting with the Bluesky API."""
    
    def __init__(self, pds_host="bsky.social"):
        self.pds_host = pds_host
        self.base_url = f"https://{pds_host}/xrpc"
        self.session = None
        
    async def initialize(self):
        """Initialize the API client session."""
        if self.session is None:
            self.session = aiohttp.ClientSession()
    
    async def close(self):
        """Close the API client session."""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def get_all_follows(self, did: str) -> Set[str]:
        """Get all DIDs that a user follows."""
        await self.initialize()
        
        follows = set()
        cursor = None
        
        while True:
            try:
                url = f"{self.base_url}/app.bsky.graph.getFollows"
                params = {"actor": did, "limit": 100}
                if cursor:
                    params["cursor"] = cursor
                    
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        # Extract DIDs of followed accounts
                        for follow in result.get("follows", []):
                            follow_did = follow.get("did")
                            if follow_did:
                                follows.add(follow_did)
                        
                        # Check if there are more follows
                        cursor = result.get("cursor")
                        if not cursor:
                            break
                    else:
                        logger.warning(f"Failed to get follows for {did}: {await response.text()}")
                        break
                        
                # Add delay between paginated requests
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error getting follows for {did}: {str(e)}")
                break
        
        return follows


class UnfollowDetector:
    """Detects unfollows in the Bluesky network."""
    
    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.api_client = BlueskyAPIClient()
    
    async def detect_unfollows(self, days_threshold: int = 7):
        """
        Detect unfollows by comparing current follows with database records.
        
        This script works by:
        1. Finding accounts that were recently checked for follows
        2. For each account, getting all their follows from the database
        3. Comparing with the current follows from the API
        4. Marking unfollowed relationships in the database
        """
        logger.info("Starting unfollow detection")
        
        try:
            # Get accounts that were recently checked for follows
            check_date = (datetime.now() - timedelta(days=days_threshold)).isoformat()
            result = self.supabase.table('bluesky_accounts') \
                          .select('did') \
                          .gt('follows_last_updated_at', check_date) \
                          .limit(100) \
                          .execute()
            
            accounts = result.data
            logger.info(f"Found {len(accounts)} recently checked accounts")
            
            for account in accounts:
                await self._process_account_unfollows(account['did'])
                
        except Exception as e:
            logger.error(f"Error detecting unfollows: {str(e)}")
        finally:
            await self.api_client.close()
    
    async def _process_account_unfollows(self, did: str):
        """Process unfollows for a single account."""
        logger.info(f"Processing unfollows for account: {did}")
        
        try:
            # Get all follows from the database
            result = self.supabase.table('follows') \
                          .select('id, following_did') \
                          .eq('follower_did', did) \
                          .eq('follow_status', 'active') \
                          .execute()
            
            db_follows = {follow['following_did']: follow['id'] for follow in result.data}
            logger.info(f"Found {len(db_follows)} follows in database for {did}")
            
            # Get current follows from the API
            current_follows = await self.api_client.get_all_follows(did)
            logger.info(f"Found {len(current_follows)} follows from API for {did}")
            
            # Find unfollows (in db but not in current)
            unfollowed_dids = set(db_follows.keys()) - current_follows
            
            if unfollowed_dids:
                logger.info(f"Detected {len(unfollowed_dids)} unfollows for {did}")
                
                # Mark unfollows in the database
                for unfollowed_did in unfollowed_dids:
                    follow_id = db_follows[unfollowed_did]
                    
                    # Update the follow record
                    self.supabase.table('follows') \
                              .update({
                                  "follow_status": "unfollowed",
                                  "unfollowed_at": datetime.now().isoformat()
                              }) \
                              .eq('id', follow_id) \
                              .execute()
                              
                    logger.info(f"Marked unfollow: {did} -> {unfollowed_did}")
            else:
                logger.info(f"No unfollows detected for {did}")
                
        except Exception as e:
            logger.error(f"Error processing unfollows for {did}: {str(e)}")


async def main():
    """Main entry point for the unfollow detector."""
    # Get configuration from environment or use defaults
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        return
    
    # Run unfollow detection
    detector = UnfollowDetector(supabase_url, supabase_key)
    await detector.detect_unfollows()


if __name__ == "__main__":
    asyncio.run(main())
