"""
Bluesky Network Traversal Worker with Database Queue

This worker crawls the Bluesky social graph using a database-backed queue system.
It populates the database with account information, follows, and followers.

Features:
- Database-backed queue for exploration with priorities
- Efficient follow/unfollow detection using stored procedures
- Rate limiting to avoid API restrictions
- Prioritization of accounts based on connectivity
"""

import asyncio
import logging
import time
import os
from datetime import datetime
import random
from typing import List, Dict, Any, Optional, Set, Tuple
from dotenv import load_dotenv
from supabase import create_client, Client
import argparse
import aiohttp

# Load environment variables from .env file
load_dotenv()

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
    
    def __init__(self, pds_host="bsky.social", username=None, password=None):
        self.pds_host = pds_host
        self.base_url = f"https://{pds_host}/xrpc"
        self.session = None
        self.auth_token = None
        self.username = username
        self.password = password
        self.rate_limit_remaining = 100
        self.rate_limit_reset = 0
        self.token_created_at = None
        self.token_expires_in = 3600  # Default token lifetime in seconds (1 hour)
    
    async def initialize(self):
        """Initialize the API client session and authenticate if credentials are provided."""
        if self.session is None:
            self.session = aiohttp.ClientSession()
            
        # Authenticate if credentials are provided and not already authenticated
        if self.username and self.password and not self.auth_token:
            await self._authenticate()
    
    async def close(self):
        """Close the API client session."""
        if self.session:
            await self.session.close()
            self.session = None
            
    def is_token_expired(self):
        """Check if the authentication token is expired or close to expiring."""
        if not self.auth_token or not self.token_created_at:
            return True
            
        # Consider token expired if it's within 5 minutes of expiration
        buffer_time = 300  # 5 minutes in seconds
        current_time = time.time()
        expiry_time = self.token_created_at + self.token_expires_in - buffer_time
        
        return current_time >= expiry_time
    
    async def get_profile(self, handle: str) -> Optional[Dict[str, Any]]:
        """Get a user's profile information."""
        await self.initialize()
        
        # Respect rate limits
        await self._check_rate_limit()
        
        try:
            url = f"{self.base_url}/com.atproto.repo.describeRepo"
            params = {"repo": handle}
            
            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
                
            async with self.session.get(url, params=params, headers=headers) as response:
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
            
            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
                
            async with self.session.get(url, params=params, headers=headers) as response:
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
            
            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
                
            async with self.session.get(url, params=params, headers=headers) as response:
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
            
            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
                
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    result = await response.json()
                    self._update_rate_limit(response)
                    return result
                else:
                    logger.warning(f"Failed to get followers for {did}: {await response.text()}")
        except Exception as e:
            logger.error(f"Error getting followers for {did}: {str(e)}")
        
        return {"followers": []}
    
    async def _authenticate(self):
        """Authenticate with the Bluesky API using provided credentials."""
        try:
            url = f"{self.base_url}/com.atproto.server.createSession"
            data = {
                "identifier": self.username,
                "password": self.password
            }
            
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    self.auth_token = result.get("accessJwt")
                    self.token_created_at = time.time()
                    
                    # Extract token expiry if available, or use default (1 hour)
                    # Note: Bluesky tokens typically last for 2 weeks, but we'll refresh more frequently
                    refresh_in = "1 hour"  # Human-readable for logging
                    self.token_expires_in = 3600  # 1 hour in seconds
                    
                    logger.info(f"Successfully authenticated as {self.username} (token refreshes in {refresh_in})")
                else:
                    logger.error(f"Authentication failed: {await response.text()}")
        except Exception as e:
            logger.error(f"Error during authentication: {str(e)}")
    
    async def _check_rate_limit(self):
        """Check and respect rate limits and token expiration."""
        # First check if token needs to be refreshed
        if self.username and self.password and self.is_token_expired():
            logger.info("Auth token is expired or will expire soon, refreshing...")
            await self._authenticate()
            
        # Then check rate limits
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
    """Worker for traversing the Bluesky social network using database queue."""
    
    def __init__(self, supabase_url: str, supabase_key: str, pds_host: str = "bsky.social", 
                 bsky_username: str = None, bsky_password: str = None):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.api_client = BlueskyAPIClient(pds_host, username=bsky_username, password=bsky_password)
        self.exploration_delay = 2.0  # seconds between API calls
        
    async def start(self, seed_handles: List[str] = None, max_accounts: int = 1000, min_interval_days: int = 7):
        """Start the network traversal process using database queue."""
        logger.info("Starting network traversal with database queue")
        
        # Initialize the queue with seed accounts if provided
        if seed_handles:
            await self._add_seed_accounts(seed_handles)
        
        try:
            count = 0
            total_processed = 0
            batch_size = min(10, max_accounts)
            
            # Main processing loop
            while total_processed < max_accounts:
                # Get next batch of accounts to process from the database queue
                accounts = await self._get_next_accounts(batch_size, min_interval_days)
                
                if not accounts:
                    logger.info("No more accounts to process in queue, adding recommended accounts")
                    # Try to add some recommended accounts to the queue
                    added = await self._add_recommended_accounts()
                    if added:
                        continue
                    else:
                        logger.info("No more accounts available, finishing")
                        break
                
                # Process each account in the batch
                for account in accounts:
                    # Process the account
                    await self._process_account(account['did'], account['handle'])
                    count += 1
                    total_processed += 1
                    
                    # Add some randomized delay to avoid rate limiting
                    delay = self.exploration_delay * (0.8 + 0.4 * random.random())
                    await asyncio.sleep(delay)
                
                logger.info(f"Processed batch of {count} accounts, total: {total_processed}/{max_accounts}")
                count = 0
                
                # Update priorities periodically
                if total_processed % 50 == 0:
                    await self._update_account_priorities()
        
        finally:
            await self.api_client.close()
            logger.info(f"Network traversal completed. Processed {total_processed} accounts")
    
    async def _get_next_accounts(self, limit: int, min_interval_days: int) -> List[Dict]:
        """Get next batch of accounts to process from database queue."""
        try:
            result = self.supabase.rpc(
                'get_accounts_to_crawl', 
                {'limit_count': limit, 'min_interval': f'{min_interval_days} days'}
            ).execute()
            
            return result.data
            
        except Exception as e:
            logger.error(f"Error getting next accounts from queue: {e}")
            return []
    
    async def _add_seed_accounts(self, handles: List[str]):
        """Add seed accounts to the database."""
        for handle in handles:
            try:
                # Ensure handle is in the correct format
                if '.' not in handle:
                    logger.warning(f"Invalid handle format for {handle}, skipping")
                    continue
                    
                # Get profile
                profile = await self.api_client.get_profile(handle)
                if not profile:
                    logger.warning(f"Could not fetch profile for seed account {handle}")
                    continue
                
                # Store or update user with high priority
                did = profile.get("did")
                if did:
                    self._store_user(did, handle, profile)
                    
                    # Set high priority
                    self.supabase.table('bluesky_accounts').update({
                        "crawl_priority": 100,
                        "crawl_status": "pending"
                    }).eq('did', did).execute()
                    
                    logger.info(f"Added seed account {handle} ({did}) to queue with high priority")
                    
            except Exception as e:
                logger.error(f"Error adding seed account {handle}: {e}")
    
    async def _add_recommended_accounts(self) -> bool:
        """Add recommended accounts to the queue based on network connectivity."""
        try:
            result = self.supabase.rpc(
                'get_recommended_accounts', 
                {'limit_count': 20, 'max_priority': 80}
            ).execute()
            
            if not result.data:
                return False
                
            # Update these accounts to be pending with higher priority
            for account in result.data:
                self.supabase.table('bluesky_accounts').update({
                    "crawl_status": "pending",
                    "crawl_priority": 70  # Set a good priority but not as high as seed accounts
                }).eq('did', account['did']).execute()
                
            logger.info(f"Added {len(result.data)} recommended accounts to queue")
            return True
            
        except Exception as e:
            logger.error(f"Error adding recommended accounts: {e}")
            return False
    
    async def _update_account_priorities(self):
        """Update account priorities based on connectivity."""
        try:
            self.supabase.rpc('update_crawl_priorities').execute()
            logger.info("Updated account crawl priorities")
        except Exception as e:
            logger.error(f"Error updating account priorities: {e}")
    
    async def _process_account(self, did: str, handle: str):
        """Process a single account - get profile, follows, followers."""
        logger.info(f"Processing account: {handle} ({did})")
        
        try:
            # Get profile
            profile = await self.api_client.get_profile(handle)
            if not profile:
                logger.warning(f"Could not fetch profile for {handle}")
                # Mark as failed
                self.supabase.rpc('mark_account_crawled', {
                    'account_did': did, 
                    'status': 'failed'
                }).execute()
                return
                
            # If we reach this point after token expiration, we've successfully refreshed the token
            
            # Store or update user
            self._store_user(did, handle, profile)
                
            # Process follows
            follows_dids = await self._collect_all_follows(did)
            
            # Process followers with lower probability
            if random.random() < 0.5:  # 50% chance to process followers
                await self._collect_some_followers(did)
            
            # Use stored procedure to efficiently process follows
            result = self.supabase.rpc('process_account_follows', {
                'account_did': did,
                'current_follows': follows_dids
            }).execute()
            
            if result.data:
                stats = result.data[0]
                logger.info(f"Follows for {handle}: {stats['new_follows_count']} new, "
                           f"{stats['maintained_follows_count']} maintained, "
                           f"{stats['unfollowed_count']} unfollowed")
            
            # Mark account as processed
            self.supabase.rpc('mark_account_crawled', {
                'account_did': did,
                'status': 'completed'
            }).execute()
            
        except Exception as e:
            logger.error(f"Error processing account {handle} ({did}): {e}")
            # Mark as error
            try:
                self.supabase.rpc('mark_account_crawled', {
                    'account_did': did, 
                    'status': 'error'
                }).execute()
            except:
                pass
    
    async def _collect_all_follows(self, did: str) -> List[str]:
        """Collect all DIDs that a user follows."""
        follows_dids = []
        cursor = None
        
        while True:
            follows_data = await self.api_client.get_follows(did, limit=100, cursor=cursor)
            follows = follows_data.get("follows", [])
            
            if not follows:
                break
            
            # Collect DIDs of followed accounts
            for follow in follows:
                following_did = follow.get("did")
                following_handle = follow.get("handle")
                
                if following_did and following_handle:
                    # Store the user
                    self._store_user(following_did, following_handle, follow)
                    follows_dids.append(following_did)
            
            # Check if there are more follows
            cursor = follows_data.get("cursor")
            if not cursor:
                break
                
            # Add delay between paginated requests
            await asyncio.sleep(self.exploration_delay)
        
        logger.info(f"Collected {len(follows_dids)} follows for {did}")
        return follows_dids
    
    async def _collect_some_followers(self, did: str, max_pages: int = 3) -> None:
        """Collect some followers of a user (limited to save API calls)."""
        cursor = None
        followers_count = 0
        pages = 0
        
        while pages < max_pages:
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
                    
                    # Add to queue with lower priority 
                    if random.random() < 0.2:  # 20% chance
                        self.supabase.table('bluesky_accounts').update({
                            "crawl_priority": 50,
                            "crawl_status": "pending"
                        }).eq('did', follower_did).execute()
                
                followers_count += 1
            
            # Check if there are more followers
            cursor = followers_data.get("cursor")
            if not cursor:
                break
                
            # Add delay between paginated requests
            await asyncio.sleep(self.exploration_delay)
            pages += 1
        
        logger.info(f"Collected {followers_count} followers for {did} (limited to {max_pages} pages)")
    
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
            
            try:
                # Try update first
                update_result = self.supabase.table('bluesky_accounts').update(user_data).eq('did', did).execute()
                
                # If no rows updated, insert it
                if not update_result.data or len(update_result.data) == 0:
                    insert_result = self.supabase.table('bluesky_accounts').insert(user_data).execute()
                    logger.info(f"Created new account record for {handle}")
            except Exception as supabase_error:
                # If update/insert approach fails, fall back to upsert
                logger.warning(f"Error with update/insert for {handle}, falling back to upsert: {supabase_error}")
                self.supabase.table('bluesky_accounts').upsert(user_data).execute()
            
        except Exception as e:
            logger.error(f"Error storing user {handle} ({did}): {e}")


async def main():
    parser = argparse.ArgumentParser(description='Bluesky Network Traversal Worker (DB Queue)')
    parser.add_argument('--seed', type=str, help='Comma-separated list of seed handles')
    parser.add_argument('--max', type=int, default=100, help='Maximum number of accounts to process')
    parser.add_argument('--interval', type=int, default=7, help='Minimum interval in days before recrawling')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between API calls in seconds')
    args = parser.parse_args()
    
    # Get configuration from environment or use defaults
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        return
    
    # Get Bluesky credentials from environment
    bsky_username = os.environ.get('BSKY_USERNAME')
    bsky_password = os.environ.get('BSKY_PASSWORD')
    
    # Initialize traversal
    traversal = NetworkTraversal(
        supabase_url, 
        supabase_key, 
        bsky_username=bsky_username, 
        bsky_password=bsky_password
    )
    traversal.exploration_delay = args.delay
    
    # Parse seed handles if provided
    seed_handles = None
    if args.seed:
        seed_handles = [handle.strip() for handle in args.seed.split(',')]
    
    # Start traversal
    await traversal.start(
        seed_handles=seed_handles, 
        max_accounts=args.max,
        min_interval_days=args.interval
    )


if __name__ == "__main__":
    asyncio.run(main())