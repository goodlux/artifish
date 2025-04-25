"""
Bluesky Network Traversal Worker

This worker crawls the Bluesky social graph, discovering users and their connections.
It populates the database with account information, follows, and followers.

Features:
- Incremental exploration starting from seed accounts
- Detection of new follows and unfollows
- Rate limiting to avoid API restrictions
- Prioritization of accounts for exploration
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any, Optional, Set
from dotenv import load_dotenv  # Added dotenv import
from supabase import create_client, Client
import argparse
import json
import aiohttp
import random

# Load environment variables from .env file
load_dotenv()  # Added this line to load .env file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("network_traversal.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("NetworkTraversal")

class BlueskyAPIClient:
    """Client for interacting with the Bluesky API."""
    
    def __init__(self, pds_host="bsky.social"):
        self.pds_host = pds_host
        self.base_url = f"https://{pds_host}/xrpc"
        self.session = None
        self.auth_token = None
        self.rate_limit_remaining = 100
        self.rate_limit_reset = 0
    
    async def initialize(self):
        """Initialize the API client session."""
        if self.session is None:
            self.session = aiohttp.ClientSession()
    
    async def close(self):
        """Close the API client session."""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def get_profile(self, handle: str) -> Optional[Dict[str, Any]]:
        """Get a user's profile information."""
        await self.initialize()
        
        # Respect rate limits
        await self._check_rate_limit()
        
        try:
            url = f"{self.base_url}/com.atproto.repo.describeRepo"
            params = {"repo": handle}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    # Get more profile details
                    profile_details = await self._get_profile_details(result.get("did"))
                    if profile_details:
                        result.update(profile_details)
                    
                    self._update_rate_limit(response)
                    return result
                else:
                    logger.warning(f"Failed to get profile for {handle}: {await response.text()}")
        except Exception as e:
            logger.error(f"Error getting profile for {handle}: {str(e)}")
        
        return None
    
    async def _get_profile_details(self, did: str) -> Optional[Dict[str, Any]]:
        """Get more detailed profile information."""
        if not did:
            return None
            
        await self._check_rate_limit()
        
        try:
            url = f"{self.base_url}/app.bsky.actor.getProfile"
            params = {"actor": did}
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    self._update_rate_limit(response)
                    return result
                else:
                    logger.warning(f"Failed to get profile details for {did}: {await response.text()}")
        except Exception as e:
            logger.error(f"Error getting profile details for {did}: {str(e)}")
        
        return None
    
    async def get_follows(self, did: str, limit: int = 100, cursor: str = None) -> Dict[str, Any]:
        """Get accounts that a user follows."""
        await self.initialize()
        await self._check_rate_limit()
        
        try:
            url = f"{self.base_url}/app.bsky.graph.getFollows"
            params = {"actor": did, "limit": limit}
            if cursor:
                params["cursor"] = cursor
                
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    self._update_rate_limit(response)
                    return result
                else:
                    logger.warning(f"Failed to get follows for {did}: {await response.text()}")
        except Exception as e:
            logger.error(f"Error getting follows for {did}: {str(e)}")
        
        return {"follows": []}
    
    async def get_followers(self, did: str, limit: int = 100, cursor: str = None) -> Dict[str, Any]:
        """Get accounts that follow a user."""
        await self.initialize()
        await self._check_rate_limit()
        
        try:
            url = f"{self.base_url}/app.bsky.graph.getFollowers"
            params = {"actor": did, "limit": limit}
            if cursor:
                params["cursor"] = cursor
                
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    self._update_rate_limit(response)
                    return result
                else:
                    logger.warning(f"Failed to get followers for {did}: {await response.text()}")
        except Exception as e:
            logger.error(f"Error getting followers for {did}: {str(e)}")
        
        return {"followers": []}
    
    async def _check_rate_limit(self):
        """Check and respect rate limits."""
        if self.rate_limit_remaining < 10:
            delay = max(0, self.rate_limit_reset - time.time())
            if delay > 0:
                logger.info(f"Rate limit low, sleeping for {delay:.2f} seconds")
                await asyncio.sleep(delay + 1)  # Add a little buffer
    
    def _update_rate_limit(self, response):
        """Update rate limit information from response headers."""
        try:
            if "ratelimit-limit" in response.headers:
                self.rate_limit_remaining = int(response.headers.get("ratelimit-remaining", 100))
                self.rate_limit_reset = time.time() + int(response.headers.get("ratelimit-reset", 0))
        except (ValueError, TypeError):
            # If headers are missing or invalid, use conservative defaults
            self.rate_limit_remaining = 10
            self.rate_limit_reset = time.time() + 60


class NetworkTraversal:
    """Worker for traversing the Bluesky social network."""
    
    def __init__(self, supabase_url: str, supabase_key: str, pds_host: str = "bsky.social"):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.api_client = BlueskyAPIClient(pds_host)
        self.processed_accounts: Set[str] = set()
        self.exploration_queue: List[str] = []
        self.exploration_delay = 2.0  # seconds between API calls
        
    async def start(self, seed_handles: List[str] = None, max_accounts: int = 1000):
        """Start the network traversal process."""
        logger.info("Starting network traversal")
        
        # Get seed accounts
        if not seed_handles:
            seed_handles = await self._get_seed_accounts()
        
        if not seed_handles:
            logger.error("No seed accounts available, exiting")
            return
        
        # Add seed accounts to queue
        self.exploration_queue.extend(seed_handles)
        
        try:
            count = 0
            # Process queue until empty or max_accounts reached
            while self.exploration_queue and count < max_accounts:
                handle = self.exploration_queue.pop(0)
                
                # Skip if already processed
                if handle in self.processed_accounts:
                    continue
                
                # Process the account
                await self._process_account(handle)
                self.processed_accounts.add(handle)
                count += 1
                
                # Add some randomized delay to avoid rate limiting
                delay = self.exploration_delay * (0.8 + 0.4 * random.random())
                await asyncio.sleep(delay)
                
                if count % 10 == 0:
                    logger.info(f"Processed {count} accounts, queue size: {len(self.exploration_queue)}")
        
        finally:
            await self.api_client.close()
            logger.info(f"Network traversal completed. Processed {len(self.processed_accounts)} accounts")
    
    async def _get_seed_accounts(self) -> List[str]:
        """Get seed accounts from the database."""
        try:
            # Try to get accounts that haven't been updated recently
            check_date = (datetime.now() - timedelta(days=7)).isoformat()
            result = self.supabase.table('bluesky_accounts') \
                          .select('handle') \
                          .or_(f"follows_last_updated_at.is.null,follows_last_updated_at.lt.{check_date}") \
                          .limit(10) \
                          .execute()
            
            handles = [account['handle'] for account in result.data]
            
            # If no accounts need updating, get any accounts
            if not handles:
                result = self.supabase.table('bluesky_accounts') \
                              .select('handle') \
                              .limit(10) \
                              .execute()
                handles = [account['handle'] for account in result.data]
            
            # If still no accounts, use a default
            if not handles:
                handles = ["boss-c.art.ifi.sh"]
            
            return handles
            
        except Exception as e:
            logger.error(f"Error getting seed accounts: {e}")
            return ["boss-c.art.ifi.sh"]  # Default fallback
    
    async def _process_account(self, handle: str):
        """Process a single account - get profile, follows, followers."""
        logger.info(f"Processing account: {handle}")
        
        try:
            # Get profile
            profile = await self.api_client.get_profile(handle)
            if not profile:
                logger.warning(f"Could not fetch profile for {handle}")
                return
            
            # Store or update user
            did = profile.get("did")
            if did:
                self._store_user(did, handle, profile)
                
                # Process follows
                await self._process_follows(did)
                
                # Process followers
                await self._process_followers(did)
                
                # Update the follows_last_updated_at timestamp
                self._update_follows_timestamp(did)
                
                # Add some accounts to the exploration queue
                self._add_accounts_to_queue()
        
        except Exception as e:
            logger.error(f"Error processing account {handle}: {e}")
    
    async def _process_follows(self, did: str):
        """Process all accounts that a user follows."""
        cursor = None
        follows_count = 0
        
        while True:
            follows_data = await self.api_client.get_follows(did, limit=100, cursor=cursor)
            follows = follows_data.get("follows", [])
            
            if not follows:
                break
            
            # Process each follow
            for follow in follows:
                following_did = follow.get("did")
                following_handle = follow.get("handle")
                
                if following_did and following_handle:
                    # Store the user
                    self._store_user(following_did, following_handle, follow)
                    
                    # Store the follow relationship
                    self._store_follow(did, following_did)
                    
                    # Add to exploration queue with low probability
                    if random.random() < 0.1:  # 10% chance
                        self.exploration_queue.append(following_handle)
                
                follows_count += 1
            
            # Check if there are more follows
            cursor = follows_data.get("cursor")
            if not cursor:
                break
                
            # Add delay between paginated requests
            await asyncio.sleep(self.exploration_delay)
        
        logger.info(f"Processed {follows_count} follows for {did}")
    
    async def _process_followers(self, did: str):
        """Process accounts that follow a user."""
        cursor = None
        followers_count = 0
        
        while True:
            followers_data = await self.api_client.get_followers(did, limit=100, cursor=cursor)
            followers = followers_data.get("followers", [])
            
            if not followers:
                break
            
            # Process each follower
            for follower in followers:
                follower_did = follower.get("did")
                follower_handle = follower.get("handle")
                
                if follower_did and follower_handle:
                    # Store the user
                    self._store_user(follower_did, follower_handle, follower)
                    
                    # Store the follow relationship (follower follows did)
                    self._store_follow(follower_did, did)
                    
                    # Add to exploration queue with low probability
                    if random.random() < 0.05:  # 5% chance
                        self.exploration_queue.append(follower_handle)
                
                followers_count += 1
            
            # Check if there are more followers
            cursor = followers_data.get("cursor")
            if not cursor:
                break
                
            # Add delay between paginated requests
            await asyncio.sleep(self.exploration_delay)
        
        logger.info(f"Processed {followers_count} followers for {did}")
    
    def _store_user(self, did: str, handle: str, profile_data: Dict[str, Any]):
        """Store or update a user in the database."""
        try:
            # Extract profile data
            display_name = profile_data.get("displayName")
            description = profile_data.get("description")
            avatar_url = None
            
            # Try to get avatar URL from different profile response formats
            if "avatar" in profile_data:
                avatar_url = profile_data.get("avatar")
            elif "avatar" in profile_data.get("profile", {}):
                avatar_url = profile_data.get("profile", {}).get("avatar")
                
            # Prepare user data
            user_data = {
                "did": did,
                "handle": handle,
                "last_updated_at": datetime.now().isoformat()
            }
            
            # Add optional fields if available
            if display_name:
                user_data["display_name"] = display_name
            if description:
                user_data["bio"] = description
            if avatar_url:
                user_data["avatar_url"] = avatar_url
                
            # Upsert into database
            self.supabase.table('bluesky_accounts').upsert(user_data).execute()
            
        except Exception as e:
            logger.error(f"Error storing user {handle} ({did}): {e}")
    
    def _store_follow(self, follower_did: str, following_did: str):
        """Store a follow relationship in the database."""
        try:
            # Check if follow relationship already exists
            result = self.supabase.table('follows') \
                          .select('id') \
                          .eq('follower_did', follower_did) \
                          .eq('following_did', following_did) \
                          .execute()
            
            now = datetime.now().isoformat()
            
            if not result.data:
                # Insert new follow relationship
                follow_data = {
                    "follower_did": follower_did,
                    "following_did": following_did,
                    "created_at": now,
                    "last_verified_at": now
                }
                self.supabase.table('follows').insert(follow_data).execute()
            else:
                # Update existing follow relationship
                follow_id = result.data[0]['id']
                self.supabase.table('follows') \
                          .update({"last_verified_at": now}) \
                          .eq('id', follow_id) \
                          .execute()
                          
        except Exception as e:
            logger.error(f"Error storing follow relationship {follower_did} -> {following_did}: {e}")
    
    def _update_follows_timestamp(self, did: str):
        """Update the follows_last_updated_at timestamp for an account."""
        try:
            self.supabase.table('bluesky_accounts') \
                      .update({"follows_last_updated_at": datetime.now().isoformat()}) \
                      .eq('did', did) \
                      .execute()
        except Exception as e:
            logger.error(f"Error updating follows_last_updated_at for {did}: {e}")
    
    def _add_accounts_to_queue(self):
        """Add more accounts to the exploration queue to ensure continued exploration."""
        # If queue is getting short, add more accounts
        if len(self.exploration_queue) < 10:
            try:
                # Get some random accounts we haven't processed yet
                result = self.supabase.table('bluesky_accounts') \
                              .select('handle') \
                              .order('random()') \
                              .limit(20) \
                              .execute()
                
                for account in result.data:
                    handle = account.get('handle')
                    if handle and handle not in self.processed_accounts and handle not in self.exploration_queue:
                        self.exploration_queue.append(handle)
                        
            except Exception as e:
                logger.error(f"Error adding accounts to queue: {e}")


async def main():
    parser = argparse.ArgumentParser(description='Bluesky Network Traversal Worker')
    parser.add_argument('--seed', type=str, help='Comma-separated list of seed handles')
    parser.add_argument('--max', type=int, default=100, help='Maximum number of accounts to process')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between API calls in seconds')
    args = parser.parse_args()
    
    # Get configuration from environment or use defaults
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        return
    
    # Initialize traversal
    traversal = NetworkTraversal(supabase_url, supabase_key)
    traversal.exploration_delay = args.delay
    
    # Parse seed handles if provided
    seed_handles = None
    if args.seed:
        seed_handles = [handle.strip() for handle in args.seed.split(',')]
    
    # Start traversal
    await traversal.start(seed_handles=seed_handles, max_accounts=args.max)


if __name__ == "__main__":
    asyncio.run(main())
