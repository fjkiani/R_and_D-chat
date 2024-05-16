# Ensure all necessary imports are at the top
import logging
import os
import sys
from pathlib import Path

# Ensure the scripts directory is in the Python path
script_path = Path(__file__).resolve().parent
project_root = script_path.parent.parent
sys.path.append(str(project_root / "scripts"))

from astrapy.db import AstraDB, AstraDBCollection
from dotenv import load_dotenv

import wikichat

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

# The client to connect to the Astra Data API
ASTRA_DB = AstraDB(token=os.getenv("ASTRA_DB_APPLICATION_TOKEN"), api_endpoint=os.getenv("ASTRA_DB_API_ENDPOINT"))

# We have three collections
_ARTICLE_EMBEDDINGS_NAME = "article_embeddings"
_ARTICLE_METADATA_NAME = "article_metadata"
_ARTICLE_SUGGESTIONS_NAME = "article_suggestions"
_ALL_COLLECTION_NAMES = [_ARTICLE_EMBEDDINGS_NAME, _ARTICLE_METADATA_NAME, _ARTICLE_SUGGESTIONS_NAME]

def drop_collection_if_exists(collection_name):
    try:
        ASTRA_DB.delete_collection(collection_name)
        logging.info(f"Collection '{collection_name}' deleted successfully.")
    except Exception as e:
        logging.info(f"Collection '{collection_name}' does not exist or cannot be deleted: {e}")

def setup_collections():
    # Drop the collections if they exist
    drop_collection_if_exists(_ARTICLE_EMBEDDINGS_NAME)
    drop_collection_if_exists(_ARTICLE_METADATA_NAME)
    drop_collection_if_exists(_ARTICLE_SUGGESTIONS_NAME)

    # Create the collections with the correct options
    ASTRA_DB.create_collection(collection_name=_ARTICLE_EMBEDDINGS_NAME, dimension=1536)
    ASTRA_DB.create_collection(collection_name=_ARTICLE_METADATA_NAME)
    ASTRA_DB.create_collection(collection_name=_ARTICLE_SUGGESTIONS_NAME)

    # Create the collection objects this code will use
    embeddings_collection = AstraDBCollection(collection_name=_ARTICLE_EMBEDDINGS_NAME, astra_db=ASTRA_DB)
    metadata_collection = AstraDBCollection(collection_name=_ARTICLE_METADATA_NAME, astra_db=ASTRA_DB)
    suggestions_collection = AstraDBCollection(collection_name=_ARTICLE_SUGGESTIONS_NAME, astra_db=ASTRA_DB)

    logging.info("Collections setup completed.")
    return [embeddings_collection, metadata_collection, suggestions_collection]

# Setup collections
_ALL_COLLECTIONS = setup_collections()
_ROTATED_COLLECTIONS = [_ALL_COLLECTIONS[0], _ALL_COLLECTIONS[1]]

# Define global collections for import
EMBEDDINGS_COLLECTION = _ALL_COLLECTIONS[0]
METADATA_COLLECTION = _ALL_COLLECTIONS[1]
SUGGESTIONS_COLLECTION = _ALL_COLLECTIONS[2]

async def truncate_all_collections():
    for collection in _ALL_COLLECTIONS:
        await try_truncate_collection(collection)

async def truncate_rotated_collections():
    for collection in _ROTATED_COLLECTIONS:
        await try_truncate_collection(collection)

async def try_truncate_collection(collection: AstraDBCollection):
    # This can timeout sometimes, so lets retry :)
    for i in range(5):
        try:
            logging.info(f"Attempt {i} Truncating collection {collection.collection_name}")
            await wikichat.utils.wrap_blocking_io(
                lambda: collection.delete_many({})
            )
            logging.info(f"Collection '{collection.collection_name}' truncated successfully.")
            break
        except Exception:
            logging.exception(f"Retrying, error truncating collection {collection.collection_name}", exc_info=True)

if __name__ == "__main__":
    setup_collections()
