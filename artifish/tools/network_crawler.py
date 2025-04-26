"""
Network crawler tool for Bluesky profiles.

This module provides an ADK tool for crawling Bluesky profiles,
fetching followers/following relationships, and storing this information.
"""

import logging
from typing import Dict, List, Any, Optional, Callable
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.function_tool import FunctionTool
from pydantic import BaseModel, Field
from atproto import Client as AtprotoClient
from ..db.db_service import db_service

logger = logging.getLogger(__name__)


class CrawlRequest(BaseModel):
    """Request model for the crawler tool."""
    handle: str = Field(description="The Bluesky handle to crawl")
    max_depth: int = Field(default=1, description="Maximum depth to crawl")
    max_per_level: int = Field(default=10, description="Maximum accounts per level")


class CrawlResponse(BaseModel):
    """Response model for the crawler tool."""
    success: bool
    accounts_found: int
    follows_found: int
    handle: str
    did: Optional[str] = None
    error: Optional[str] = None


class NetworkCrawlerService:
    """Service for crawling Bluesky social networks."""
    
    def __init__(self):
        """Initialize the network crawler service."""
        self.name = "network_crawler"
    
    async def crawl(self, handle: str, max_depth: int = 1, max_per_level: int = 10) -> Dict[str, Any]:
        """
        Crawl the Bluesky network starting from a given handle.
        
        Args:
            handle (str): The Bluesky handle to start crawling from.
            max_depth (int): Maximum depth to crawl (default: 1).
            max_per_level (int): Maximum accounts to process per level (default: 10).
            
        Returns:
            Dict[str, Any]: Results of the crawl operation.
        """
        logger.info(f"Starting network crawl from handle: {handle}")
        
        try:
            # Initialize Bluesky client
            client = AtprotoClient()
            
            # Resolve the handle to a DID
            profile = client.get_profile({"actor": handle})
            if not profile:
                return {"error": f"Could not find profile for handle: {handle}"}
            
            did = profile.did
            
            # Add the account to the database
            db_service.add_bluesky_account(did, handle, profile)
            
            # Start the crawl process
            accounts_found = 1  # Count the initial account
            follows_found = 0
            
            # Process follows (people the user follows)
            follows = await self._get_follows(client, did, max_per_level)
            follows_found += len(follows)
            
            for follow in follows:
                # Add the followed account to the database
                db_service.add_bluesky_account(
                    follow["did"], 
                    follow["handle"],
                    follow.get("profile")
                )
                
                # Add the follow relationship
                db_service.add_follow_relationship(did, follow["did"])
                
                accounts_found += 1
            
            # Process followers
            followers = await self._get_followers(client, did, max_per_level)
            follows_found += len(followers)
            
            for follower in followers:
                # Add the follower account to the database
                db_service.add_bluesky_account(
                    follower["did"], 
                    follower["handle"],
                    follower.get("profile")
                )
                
                # Add the follow relationship
                db_service.add_follow_relationship(follower["did"], did)
                
                accounts_found += 1
            
            # Additional recursive crawling would be implemented here if max_depth > 1
            # For now, we're keeping it simple
            
            return {
                "success": True,
                "accounts_found": accounts_found,
                "follows_found": follows_found,
                "handle": handle,
                "did": did
            }
            
        except Exception as e:
            logger.error(f"Error crawling network: {e}")
            return {"error": str(e)}
    
    async def _get_follows(self, client, did, limit) -> List[Dict[str, Any]]:
        """
        Get the accounts that a user follows.
        
        Args:
            client: The Bluesky client
            did (str): The DID of the user
            limit (int): Maximum number of follows to retrieve
            
        Returns:
            List[Dict]: List of follow data
        """
        follows = []
        try:
            response = client.get_follows({"actor": did, "limit": limit})
            
            if not response or not hasattr(response, "follows"):
                return follows
            
            for follow in response.follows:
                follows.append({
                    "did": follow.did,
                    "handle": follow.handle,
                    "profile": {
                        "displayName": getattr(follow, "display_name", None),
                        "description": getattr(follow, "description", None),
                        "avatar": getattr(follow, "avatar", None)
                    }
                })
                
        except Exception as e:
            logger.error(f"Error getting follows: {e}")
            
        return follows
    
    async def _get_followers(self, client, did, limit) -> List[Dict[str, Any]]:
        """
        Get the followers of a user.
        
        Args:
            client: The Bluesky client
            did (str): The DID of the user
            limit (int): Maximum number of followers to retrieve
            
        Returns:
            List[Dict]: List of follower data
        """
        followers = []
        try:
            response = client.get_followers({"actor": did, "limit": limit})
            
            if not response or not hasattr(response, "followers"):
                return followers
            
            for follower in response.followers:
                followers.append({
                    "did": follower.did,
                    "handle": follower.handle,
                    "profile": {
                        "displayName": getattr(follower, "display_name", None),
                        "description": getattr(follower, "description", None),
                        "avatar": getattr(follower, "avatar", None)
                    }
                })
                
        except Exception as e:
            logger.error(f"Error getting followers: {e}")
            
        return followers
    
    def get_function_tool(self) -> Callable:
        """Get the function tool for this service."""
        async def crawl_tool(handle: str, max_depth: int = 1, max_per_level: int = 10) -> Dict[str, Any]:
            return await self.crawl(handle, max_depth, max_per_level)
        
        return crawl_tool