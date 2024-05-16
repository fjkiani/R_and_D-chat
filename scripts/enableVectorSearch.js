// scripts/enableVectorSearch.js
require('dotenv').config();
const axios = require('axios');

const ASTRA_DB_API_ENDPOINT = process.env.ASTRA_DB_API_ENDPOINT;
const ASTRA_DB_APPLICATION_TOKEN = process.env.ASTRA_DB_APPLICATION_TOKEN;
const ASTRA_DB_NAMESPACE = process.env.ASTRA_DB_NAMESPACE;
const COLLECTION_NAME = 'article_embeddings';

// Log environment variables to verify they're correct
console.log('ASTRA_DB_API_ENDPOINT:', ASTRA_DB_API_ENDPOINT);
console.log('ASTRA_DB_APPLICATION_TOKEN:', ASTRA_DB_APPLICATION_TOKEN ? 'Token is set' : 'Token is missing');
console.log('ASTRA_DB_NAMESPACE:', ASTRA_DB_NAMESPACE);

// Schema configuration with vector search enabled
const schema = {
  fields: {
    vector: {
      type: "vector",
      options: {
        dims: 1536,  // Example dimension size, adjust to your needs
        similarity: "cosine"
      }
    },
    // Add other fields if necessary
  }
};

async function enableVectorSearch() {
  if (!ASTRA_DB_API_ENDPOINT || !ASTRA_DB_APPLICATION_TOKEN || !ASTRA_DB_NAMESPACE) {
    console.error('Missing environment variables. Please check your .env file.');
    return;
  }

  try {
    const url = `${ASTRA_DB_API_ENDPOINT}/v2/namespaces/${ASTRA_DB_NAMESPACE}/collections/${COLLECTION_NAME}`;
    console.log('URL:', url);

    const response = await axios.put(
      url,
      schema,
      {
        headers: {
          'X-Cassandra-Token': ASTRA_DB_APPLICATION_TOKEN,
          'Content-Type': 'application/json',
        },
      }
    );

    console.log('Vector search enabled:', response.data);
  } catch (error) {
    console.error('Error enabling vector search:', error.response ? error.response.data : error.message);
  }
}

enableVectorSearch();
