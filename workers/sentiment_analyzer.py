"""
Bluesky Sentiment Analyzer Worker

This worker connects to the Bluesky Jetstream API, processes posts in real-time,
and performs sentiment analysis using VADER. It populates the database with
post references and sentiment data.

Features:
- Real-time processing of the Bluesky firehose via Jetstream
- Lightweight sentiment analysis with VADER
- Basic topic extraction
- Database storage of posts with sentiment scores
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
import re
import websocket
import threading
from queue import Queue
import signal
import sys
from typing import Dict, Any, List, Optional, Set
from dotenv import load_dotenv  # Added dotenv import
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()  # Added this line to load .env file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sentiment_analyzer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SentimentAnalyzer")

# Make sure NLTK data is available
try:
    nltk.data.find('vader_lexicon')
    nltk.data.find('punkt')
    nltk.data.find('stopwords')
except LookupError:
    logger.warning("NLTK data not found. Please run setup_nltk.py first.")
    sys.exit(1)

class JetstreamClient:
    """Client for connecting to the Bluesky Jetstream API."""
    
    def __init__(self, host='wss://jetstream2.us-east.bsky.network'):
        self.host = host
        self.ws = None
        self.running = False
        self.message_queue = Queue()
        self.receive_thread = None
        self.connected = False
        self.reconnect_delay = 1  # Start with 1 second delay
        self.max_reconnect_delay = 60  # Maximum reconnect delay in seconds
    
    def connect(self, collections=None):
        """Connect to the Jetstream API."""
        if not collections:
            collections = ["app.bsky.feed.post"]
            
        url = f"{self.host}/subscribe?wantedCollections={','.join(collections)}"
        
        try:
            logger.info(f"Connecting to Jetstream: {url}")
            self.ws = websocket.create_connection(url)
            self.connected = True
            self.reconnect_delay = 1  # Reset reconnect delay after successful connection
            logger.info("Connected to Jetstream")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to Jetstream: {e}")
            self.connected = False
            return False
    
    def start(self, collections=None):
        """Start listening for messages."""
        if self.running:
            return
            
        self.running = True
        
        if not self.connect(collections):
            self.reconnect()
            
        # Start receive thread
        self.receive_thread = threading.Thread(target=self._receive_loop)
        self.receive_thread.daemon = True
        self.receive_thread.start()
        
        logger.info("Jetstream client started")
    
    def stop(self):
        """Stop listening for messages."""
        self.running = False
        if self.ws:
            self.ws.close()
        logger.info("Jetstream client stopped")
    
    def _receive_loop(self):
        """Loop to receive messages from the websocket."""
        while self.running:
            if not self.connected:
                time.sleep(0.1)
                continue
                
            try:
                message = self.ws.recv()
                self.message_queue.put(message)
                
            except websocket.WebSocketConnectionClosedException:
                logger.warning("Websocket connection closed")
                self.connected = False
                self.reconnect()
                
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                self.connected = False
                self.reconnect()
    
    def reconnect(self):
        """Attempt to reconnect with exponential backoff."""
        if not self.running:
            return
            
        logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
        time.sleep(self.reconnect_delay)
        
        # Exponential backoff with jitter
        self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
        
        if not self.connect():
            threading.Thread(target=self.reconnect).start()
    
    def get_message(self, timeout=None):
        """Get a message from the queue."""
        try:
            return self.message_queue.get(block=True, timeout=timeout)
        except Exception:
            return None


class SentimentAnalyzer:
    """Worker for analyzing sentiment of Bluesky posts."""
    
    def __init__(self, supabase_url: str, supabase_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.jetstream = JetstreamClient()
        self.sid = SentimentIntensityAnalyzer()
        self.stop_words = set(stopwords.words('english'))
        self.processed_count = 0
        self.running = False
        self.interesting_topics = self._load_interesting_topics()
    
    def _load_interesting_topics(self):
        """Load list of interesting topics to track."""
        # This could be loaded from database later
        return [
            'ai', 'artificial intelligence', 'machine learning', 'ml', 
            'llm', 'large language model', 'gpt', 'claude', 'bard',
            'bluesky', 'social network', 'twitter', 'facebook', 'instagram',
            'crypto', 'bitcoin', 'ethereum', 'blockchain',
            'politics', 'science', 'tech', 'technology',
            'art', 'music', 'film', 'movie'
        ]
    
    def start(self):
        """Start the sentiment analyzer."""
        logger.info("Starting sentiment analyzer")
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Connect to Jetstream
        self.jetstream.start(collections=["app.bsky.feed.post"])
        
        try:
            # Main processing loop
            while self.running:
                message = self.jetstream.get_message(timeout=1.0)
                if message:
                    self._process_message(message)
                    
        except Exception as e:
            logger.error(f"Error in main processing loop: {e}")
            
        finally:
            self.jetstream.stop()
            logger.info(f"Sentiment analyzer stopped. Processed {self.processed_count} posts")
    
    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {sig}, shutting down...")
        self.running = False
    
    def _process_message(self, message):
        """Process a single message from Jetstream."""
        try:
            data = json.loads(message)
            
            # Check if it's a post commit with record data
            if data.get('kind') == 'commit' and data.get('commit', {}).get('collection') == 'app.bsky.feed.post' and 'record' in data:
                # Extract text content
                text = data['record'].get('text', '')
                if not text:
                    return
                
                # Extract metadata
                did = data.get('did')
                rkey = data.get('commit', {}).get('rkey')
                
                if not did or not rkey:
                    return
                    
                uri = f"at://{did}/app.bsky.feed.post/{rkey}"
                created_at = data['record'].get('createdAt')
                
                # Analyze sentiment
                sentiment = self._analyze_sentiment(text)
                
                # Extract topics
                topics = self._extract_topics(text)
                
                # Store results
                self._store_results(did, uri, rkey, text, sentiment, topics, created_at)
                
                # Increment counter
                self.processed_count += 1
                if self.processed_count % 100 == 0:
                    logger.info(f"Processed {self.processed_count} posts")
                    
        except json.JSONDecodeError:
            logger.error("Invalid JSON received")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _analyze_sentiment(self, text):
        """Analyze sentiment of text using VADER."""
        try:
            return self.sid.polarity_scores(text)
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {'compound': 0, 'neg': 0, 'neu': 0, 'pos': 0}
    
    def _extract_topics(self, text):
        """Extract topics from text."""
        topics = []
        
        try:
            # Lowercase and tokenize
            tokens = word_tokenize(text.lower())
            
            # Remove stopwords and punctuation
            words = [word for word in tokens if word.isalpha() and word not in self.stop_words]
            
            # Create word pairs (bigrams)
            bigrams = [' '.join(words[i:i+2]) for i in range(len(words)-1)]
            
            # Check against interesting topics
            for topic in self.interesting_topics:
                if topic in text.lower() or topic in bigrams:
                    topics.append(topic)
            
        except Exception as e:
            logger.error(f"Error extracting topics: {e}")
            
        return topics
    
    def _store_results(self, did, uri, rkey, text, sentiment, topics, created_at):
        """Store results in database."""
        try:
            # Store user if not exists
            self._ensure_user_exists(did)
            
            # Store post reference
            post_data = {
                "uri": uri,
                "author_did": did,
                "rkey": rkey,
                "indexed_at": datetime.now().isoformat(),
                "created_at": created_at,
                "sentiment_compound": sentiment['compound'],
                "sentiment_positive": sentiment['pos'],
                "sentiment_negative": sentiment['neg'],
                "sentiment_neutral": sentiment['neu'],
                "topics": json.dumps(topics) if topics else None
            }
            
            self.supabase.table('post_references').upsert(post_data).execute()
            
            # Store full post content if it's relevant
            needs_full_storage = self._needs_full_storage(sentiment, topics, text)
            if needs_full_storage:
                content_data = {
                    "post_uri": uri,
                    "full_text": text,
                    "created_at": datetime.now().isoformat(),
                    "analyzed_at": datetime.now().isoformat()
                }
                
                self.supabase.table('post_content').upsert(content_data).execute()
                
        except Exception as e:
            logger.error(f"Error storing results for {uri}: {e}")
    
    def _ensure_user_exists(self, did):
        """Make sure user exists in database."""
        try:
            # Check if user exists
            result = self.supabase.table('bluesky_accounts').select('did').eq('did', did).execute()
            
            if not result.data:
                # Insert minimal user record
                user_data = {
                    "did": did,
                    "handle": f"unknown_{did[-8:]}",  # Temporary handle
                    "indexed_at": datetime.now().isoformat()
                }
                
                self.supabase.table('bluesky_accounts').insert(user_data).execute()
                
        except Exception as e:
            logger.error(f"Error ensuring user {did} exists: {e}")
    
    def _needs_full_storage(self, sentiment, topics, text):
        """Determine if a post needs full content storage."""
        # Store if sentiment is strongly positive or negative
        if abs(sentiment['compound']) > 0.5:
            return True
            
        # Store if it matches our topics of interest
        if topics:
            return True
            
        # Store longer posts with some engagement potential
        if len(text) > 100 and abs(sentiment['compound']) > 0.3:
            return True
            
        return False


def main():
    """Main entry point for the sentiment analyzer."""
    # Get configuration from environment
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        return
    
    # Run sentiment analyzer
    analyzer = SentimentAnalyzer(supabase_url, supabase_key)
    analyzer.start()


if __name__ == "__main__":
    main()
