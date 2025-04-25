"""
NLTK Setup Script

This script downloads the required NLTK data for sentiment analysis
and other NLP tasks used in the Artifish project.

Run this script once before using the sentiment analysis worker.
"""

import nltk
import sys
import os
from dotenv import load_dotenv  # Added dotenv import

# Load environment variables from .env file
load_dotenv()  # Added this line to load .env file

def setup_nltk():
    """Download required NLTK data packages."""
    print("Setting up NLTK data...")
    
    packages = [
        'punkt',           # For sentence tokenization
        'stopwords',       # Common stopwords
        'wordnet',         # For lemmatization
        'vader_lexicon',   # For VADER sentiment analysis
        'averaged_perceptron_tagger'  # For part-of-speech tagging
    ]
    
    for package in packages:
        try:
            print(f"Downloading {package}...")
            nltk.download(package)
            print(f"Successfully downloaded {package}")
        except Exception as e:
            print(f"Error downloading {package}: {str(e)}", file=sys.stderr)
            return False
    
    print("NLTK setup complete.")
    return True

if __name__ == "__main__":
    # Set NLTK data path to project directory if not already set
    if 'NLTK_DATA' not in os.environ:
        # Create a data directory if it doesn't exist
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'nltk_data')
        os.makedirs(data_dir, exist_ok=True)
        
        os.environ['NLTK_DATA'] = data_dir
        print(f"Setting NLTK_DATA to {data_dir}")
    
    # Download required packages
    setup_nltk()
