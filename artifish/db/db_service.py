"""
Database service for Artifish.

This module provides methods for interacting with the Supabase database,
specifically for storing Bluesky user data and relationships.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables from .env file
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


class DatabaseService:
    """Service for database operations related to Bluesky network crawling."""
    
    def __init__(self):
        """Initialize the database service with Supabase client."""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
            
        self.supabase = create_client(supabase_url, supabase_key)
    
    def add_bluesky_account(self, did, handle, profile_data=None):
        """
        Add a Bluesky account to the database if it doesn't exist.
        
        Args:
            did (str): The decentralized identifier for the Bluesky account
            handle (str): The handle for the Bluesky account
            profile_data (dict, optional): Additional profile data
            
        Returns:
            dict: The added or existing account data
        """
        # Check if account already exists
        response = self.supabase.table("bluesky_accounts").select("*").eq("did", did).execute()
        
        if response.data:
            # Account exists, update if needed
            account = response.data[0]
            
            # Update handle if it changed
            if account["handle"] != handle:
                update_response = self.supabase.table("bluesky_accounts").update(
                    {"handle": handle}
                ).eq("did", did).execute()
                
            return account
        
        # Account doesn't exist, create it
        account_data = {
            "did": did,
            "handle": handle,
        }
        
        if profile_data:
            # Add additional profile data
            account_data.update({
                "display_name": profile_data.get("displayName"),
                "description": profile_data.get("description"),
                "avatar_url": profile_data.get("avatar"),
            })
        
        response = self.supabase.table("bluesky_accounts").insert(account_data).execute()
        
        if response.data:
            return response.data[0]
        
        return None
    
    def add_follow_relationship(self, follower_did, following_did):
        """
        Add a follow relationship between two Bluesky accounts.
        
        Args:
            follower_did (str): DID of the follower account
            following_did (str): DID of the account being followed
            
        Returns:
            dict: The created relationship data or None if it failed
        """
        # Check if relationship already exists
        response = self.supabase.table("follows").select("*").eq(
            "follower_did", follower_did
        ).eq("following_did", following_did).execute()
        
        if response.data:
            # Relationship already exists
            return response.data[0]
        
        # Create the relationship
        relationship_data = {
            "follower_did": follower_did,
            "following_did": following_did,
        }
        
        response = self.supabase.table("follows").insert(relationship_data).execute()
        
        if response.data:
            return response.data[0]
        
        return None


# Create a singleton instance
db_service = DatabaseService()
