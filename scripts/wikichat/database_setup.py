# database_setup.py
import logging
import os
from astrapy.db import AstraDB, AstraDBCollection
from dotenv import load_dotenv

load_dotenv()

# Directly set the environment variable in the script for testing purposes
os.environ["ASTRA_DB_APPLICATION_TOKEN"] = "AstraCS:mBJwuzaKZCznEZqFOOOikXIc:13b1967f2f3b861a0a16fabf699a140b40ba8c89970b048f621af2842f853118"  # Replace with your actual token
os.environ["ASTRA_DB_API_ENDPOINT"] = "https://9db69b34-256f-441b-afbb-af21fe85dea0-us-east-2.apps.astra.datastax.com"  # Replace with your actual endpoint

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
