"""
Boss-C: The primary agent for the Artifish ecosystem.

This agent will be our main interface with the Bluesky social network,
with a distinct personality and capabilities.
"""

import os
import asyncio
import logging
from typing import AsyncGenerator, Optional
from dotenv import load_dotenv
from google.adk import Agent
from google.adk.events.event import Event
from google.adk.agents.invocation_context import InvocationContext
from pydantic import BaseModel
from artifish.tools.network_crawler import NetworkCrawlerService

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class MessageContent(BaseModel):
    content: str


class BossCAgent(Agent):
    """The primary Artifish agent, Boss-C."""
    
    def __init__(self, model_name="deepseek"):
        """
        Initialize the Boss-C agent.
        
        Args:
            model_name (str): Name of the Ollama model to use
        """
        # Set up environment for liteLLM to use Ollama
        import os
        os.environ["LITELLM_MODEL"] = f"ollama/{model_name}"
        
        # Create the network crawler service before initialization
        self._network_crawler_service = NetworkCrawlerService()
        
        # Initialize the agent
        super().__init__(
            name="boss_c",
            model="litellm/ollama",  # Using the LiteLLM adapter for Ollama
            instruction="""
            You are Boss-C, an AI agent that can analyze and crawl Bluesky social networks.
            Your capabilities include:
            1. Crawling Bluesky profiles to analyze connections
            2. Identifying key accounts in a user's network
            
            When a user asks you to crawl a Bluesky account, extract the handle from their message
            and use the network_crawler tool to analyze their connections.
            """,
            tools=[self._network_crawler_service.get_function_tool()]
        )
        
        logger.info(f"Boss-C agent initialized with Ollama model: {model_name}")
    
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        """
        Handle incoming messages to the agent.
        Overrides the base implementation to implement custom logic.
        
        Args:
            ctx (InvocationContext): The invocation context
            
        Yields:
            Event: Events generated during processing
        """
        # This is the newer implementation based on the ADK version
        message_content = ctx.user_request.text
        
        # Basic message handling logic
        if "crawl" in message_content.lower() and "bluesky" in message_content.lower():
            # Extract handle from message if possible
            words = message_content.split()
            handle = None
            
            for word in words:
                if "." in word and not word.startswith(("http://", "https://")):
                    handle = word.strip(",.?!;:")
                    break
            
            if handle:
                # Use the tool directly
                try:
                    result = await self._network_crawler_service.crawl(handle)
                    response = f"Successfully crawled the network for {handle}!\n\nFound {result.get('accounts_found', 0)} accounts and {result.get('follows_found', 0)} follow relationships."
                except Exception as e:
                    logger.error(f"Error in crawl request: {e}")
                    response = f"Sorry, I encountered an error while trying to crawl {handle}: {str(e)}"
            else:
                response = "I need a Bluesky handle to crawl. Please provide one."
        else:
            # Default response for other messages
            response = ("Hello! I'm Boss-C, your Bluesky network crawler. "
                      "You can ask me to crawl a Bluesky profile to analyze "
                      "their network. Just say something like 'crawl bluesky.bsky.social'.")
        
        # Create an event with the response
        event = Event.create_content_event(response)
        yield event


async def run_agent():
    """Run the Boss-C agent interactively."""
    agent = BossCAgent(model_name="deepseek")  # Use Ollama model
    logger.info("Boss-C agent is ready")
    
    # Interactive console
    print("Welcome to Boss-C Bluesky Network Crawler!")
    print("Type 'exit' or 'quit' to end the session.")
    print("Example: 'crawl bluesky.bsky.social'")
    
    while True:
        try:
            user_input = input("\n> ")
            if user_input.lower() in ["exit", "quit"]:
                break
                
            # Create a simple invocation context with the user's input
            from google.adk.agents.invocation_context import InvocationContext
            from google.adk.agents.user_request import UserRequest
            
            ctx = InvocationContext(
                user_request=UserRequest(text=user_input),
                session_state={}
            )
            
            # Process the user input
            response_text = ""
            async for event in agent._run_async_impl(ctx):
                if event.content:
                    response_text = "".join([part.text for part in event.content.parts if part.text])
            
            # Print the response
            print(response_text)
            
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            print(f"An error occurred: {e}")


def main():
    """Main entry point."""
    try:
        asyncio.run(run_agent())
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"A fatal error occurred: {e}")


if __name__ == "__main__":
    main()