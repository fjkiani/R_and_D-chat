import logging
import os
from astrapy.db import AstraDB, AstraDBCollection
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the Astra DB token and endpoint from the environment variables
ASTRA_DB_APPLICATION_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_API_ENDPOINT = os.getenv("ASTRA_DB_API_ENDPOINT")

if not ASTRA_DB_APPLICATION_TOKEN or not ASTRA_DB_API_ENDPOINT:
    raise ValueError("ASTRA_DB_APPLICATION_TOKEN or ASTRA_DB_API_ENDPOINT is not set in the environment variables")

# Initialize the AstraDB client
ASTRA_DB = AstraDB(token=ASTRA_DB_APPLICATION_TOKEN, api_endpoint=ASTRA_DB_API_ENDPOINT)

# Collection names
_ARTICLE_EMBEDDINGS_NAME = "article_embeddings"
_ARTICLE_METADATA_NAME = "article_metadata"
_ARTICLE_SUGGESTIONS_NAME = "article_suggestions"

# Create the collection objects
EMBEDDINGS_COLLECTION = AstraDBCollection(
    collection_name=_ARTICLE_EMBEDDINGS_NAME, astra_db=ASTRA_DB
)
METADATA_COLLECTION = AstraDBCollection(
    collection_name=_ARTICLE_METADATA_NAME, astra_db=ASTRA_DB
)
SUGGESTIONS_COLLECTION = AstraDBCollection(
    collection_name=_ARTICLE_SUGGESTIONS_NAME, astra_db=ASTRA_DB
)
