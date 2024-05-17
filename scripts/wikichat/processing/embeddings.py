# In wikichat/processing/embeddings.py

import logging
import os
import cohere
from cohere.responses import Embeddings
from dotenv import load_dotenv
import asyncio
from aiohttp.client_exceptions import ClientResponseError
from backoff import on_exception, expo

load_dotenv()

# Directly set the environment variable in the script for testing purposes
os.environ["COHERE_API_KEY"] = "#"  # Replace with your actual API key

# Retrieve the API key from the environment variables
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise ValueError("COHERE_API_KEY is not set in the environment variables")


# Log the API key for debugging purposes
logging.basicConfig(level=logging.INFO)
logging.info(f"Using API Key: {COHERE_API_KEY}")

# Initialize the Cohere client
COHERE_CLIENT = cohere.AsyncClient(COHERE_API_KEY)
EMBEDDING_MODEL = 'embed-english-v3.0'

@on_exception(expo, ClientResponseError, max_tries=5, jitter=None)
async def get_embeddings(texts, input_type='search_document'):
    try:
        logging.info(f"Requesting embeddings for {len(texts)} texts using model {EMBEDDING_MODEL}")
        response = await COHERE_CLIENT.embed(texts=texts, model=EMBEDDING_MODEL, input_type=input_type)
        embeddings = response.embeddings
        logging.info(f"Received {len(embeddings)} embeddings with dimension {len(embeddings[0]) if embeddings else 'unknown'}")

        # Verify dimensions
        embedding_dimension = len(embeddings[0]) if embeddings else 0

    except cohere.CohereAPIError as e:
        logging.error(f"Cohere API error: {e}")
        raise
    except AssertionError as e:
        logging.error(f"Assertion error: {e}")
        raise
    except Exception as e:
        logging.error("Unexpected error vectorizing texts", exc_info=True)
        raise
    finally:
        await COHERE_CLIENT.close()  # Ensure the client session is properly closed
    
    return embeddings, embedding_dimension

# Test function to ensure the API key is valid
async def test_api_key():
    try:
        logging.info("Testing API key...")
        response = await COHERE_CLIENT.embed(texts=["test"], model=EMBEDDING_MODEL, input_type='search_document')
        if response and response.embeddings:
            logging.info("API key is valid.")
        else:
            logging.error("API key validation failed: No embeddings returned.")
    except cohere.CohereError as e:
        logging.error(f"API key validation failed: {e}")
    finally:
        await COHERE_CLIENT.close()  # Ensure the client session is properly closed

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(f"Using API Key: {COHERE_API_KEY}")  # Print the API key being used
    asyncio.run(test_api_key())
