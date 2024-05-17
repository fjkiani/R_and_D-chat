import logging
import os
import sys
import asyncio

# Add the parent directory of 'wikichat' to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from wikichat.processing.embeddings import get_embeddings  # Adjust the import based on your project structure
from wikichat.database_setup import ASTRA_DB, EMBEDDINGS_COLLECTION, METADATA_COLLECTION, SUGGESTIONS_COLLECTION

load_dotenv()

# Directly set the environment variable in the script for testing purposes
os.environ["ASTRA_DB_APPLICATION_TOKEN"] = "#"  # Replace with your actual token
os.environ["ASTRA_DB_API_ENDPOINT"] = "#"  # Replace with your actual endpoint

# Collection names
_ARTICLE_EMBEDDINGS_NAME = "article_embeddings"
_ARTICLE_METADATA_NAME = "article_metadata"
_ARTICLE_SUGGESTIONS_NAME = "article_suggestions"

def delete_collection_if_exists(collection_name):
    try:
        ASTRA_DB.delete_collection(collection_name=collection_name)
        logging.info(f"Deleted existing collection {collection_name}")
    except Exception as e:
        if "does not exist" in str(e):
            logging.info(f"Collection {collection_name} does not exist, nothing to delete.")
        else:
            logging.error(f"Error deleting collection {collection_name}. Error: {e}")

def create_collection(collection_name, dimension=None):
    try:
        if dimension:
            ASTRA_DB.create_collection(collection_name=collection_name, dimension=dimension)
        else:
            ASTRA_DB.create_collection(collection_name=collection_name)
        logging.info(f"Created collection {collection_name} with dimension {dimension}")
    except Exception as e:
        logging.error(f"Error while creating collection {collection_name}. Error: {e}")

# Function to process articles and get embeddings
async def process_and_embed_articles(articles: list[str]) -> None:
    try:
        embeddings, embedding_dimension = await get_embeddings(articles)
        logging.info(f"Received {len(embeddings)} embeddings with dimension {embedding_dimension}")

        # Truncate embeddings to 1000 dimensions if necessary
        if embedding_dimension > 1000:
            logging.warning(f"Truncating embeddings from {embedding_dimension} to 1000 dimensions")
            embeddings = [embedding[:1000] for embedding in embeddings]
            embedding_dimension = 1000

        # Recreate the collection with the retrieved embedding dimension
        delete_collection_if_exists(_ARTICLE_EMBEDDINGS_NAME)
        create_collection(_ARTICLE_EMBEDDINGS_NAME, dimension=embedding_dimension)

        # Insert embeddings into the database
        for i, embedding in enumerate(embeddings):
            EMBEDDINGS_COLLECTION.insert_one({
                "article": articles[i],
                "embedding": embedding
            })
    except Exception as e:
        logging.error(f"Failed to process and embed articles: {e}")

async def truncate_all_collections():
    delete_collection_if_exists(_ARTICLE_EMBEDDINGS_NAME)
    delete_collection_if_exists(_ARTICLE_METADATA_NAME)
    delete_collection_if_exists(_ARTICLE_SUGGESTIONS_NAME)
    create_collection(_ARTICLE_EMBEDDINGS_NAME, dimension=1024)
    create_collection(_ARTICLE_METADATA_NAME)
    create_collection(_ARTICLE_SUGGESTIONS_NAME)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    articles = ["This is an article about AI.", "Another article on machine learning."]
    asyncio.run(process_and_embed_articles(articles))
