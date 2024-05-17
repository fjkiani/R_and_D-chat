import hashlib
import json
import logging
from datetime import datetime

from langchain.text_splitter import RecursiveCharacterTextSplitter

import wikichat.utils
from wikichat.database import EMBEDDINGS_COLLECTION, METADATA_COLLECTION, SUGGESTIONS_COLLECTION
from wikichat.processing import embeddings, wikipedia
from wikichat.processing.model import ArticleMetadata, Article, ChunkedArticle, Chunk, ChunkMetadata, \
    ChunkedArticleDiff, \
    ChunkedArticleMetadataOnly, VectoredChunkedArticleDiff, VectoredChunk, EmbeddingDocument, RECENT_ARTICLES, \
    RecentArticles
from wikichat.utils.metrics import METRICS
from wikichat.utils.pipeline import AsyncPipeline
import logging
from wikichat.processing.embeddings import get_embeddings
from wikichat.database_setup import EMBEDDINGS_COLLECTION, METADATA_COLLECTION, SUGGESTIONS_COLLECTION
from wikichat.utils.metrics import METRICS


TEXT_SPLITTER = RecursiveCharacterTextSplitter(chunk_size=1024, chunk_overlap=200, length_function=len)

async def load_article(meta):
    return await wikipedia.scrape_article(meta)

async def chunk_article(article):
    chunks = TEXT_SPLITTER.split_text(article.content)
    logging.debug(f"Split article {article.metadata.url} into {len(chunks)} chunks")
    hashes = [hashlib.sha256(chunk.encode('utf-8')).hexdigest() for chunk in chunks]
    await METRICS.update_chunks(chunks_created=len(chunks))
    return ChunkedArticle(
        article=article,
        chunks=[Chunk(content=chunk, metadata=ChunkMetadata(index=idx, length=len(chunk), hash=chunk_hash)) for idx, (chunk, chunk_hash) in enumerate(zip(chunks, hashes))]
    )

async def calc_chunk_diff(chunked_article):
    new_metadata = ChunkedArticleMetadataOnly.from_chunked_article(chunked_article)
    logging.debug(f"Calculating chunk delta for article {chunked_article.article.metadata.url}")
    resp = await wikichat.utils.wrap_blocking_io(lambda x: METADATA_COLLECTION.find_one(filter={"_id": x}), new_metadata._id)
    prev_metadata_doc = resp["data"]["document"]
    if not prev_metadata_doc:
        logging.debug(f"No previous metadata, all chunks are new")
        await METRICS.update_chunks(chunk_diff_new=len(chunked_article.chunks))
        return ChunkedArticleDiff(chunked_article=chunked_article, new_chunks=chunked_article.chunks)
    await METRICS.update_database(articles_read=1)
    prev_metadata = ChunkedArticleMetadataOnly.from_dict(prev_metadata_doc)
    logging.debug(f"Found previous metadata with {len(prev_metadata.chunks_metadata)} chunks, comparing")
    new_chunks = [chunk for chunk in chunked_article.chunks if chunk.metadata.hash not in prev_metadata.chunks_metadata.keys()]
    deleted_chunks = [chunk_meta for chunk_meta in prev_metadata.chunks_metadata.values() if chunk_meta.hash not in new_metadata.chunks_metadata.keys()]
    unchanged_chunks = [chunk for chunk in chunked_article.chunks if chunk.metadata.hash in prev_metadata.chunks_metadata.keys()]
    await METRICS.update_chunks(chunk_diff_new=len(new_chunks), chunk_diff_deleted=len(deleted_chunks), chunk_diff_unchanged=len(unchanged_chunks))
    logging.debug(f"Found {len(new_chunks)} new chunks, {len(deleted_chunks)} deleted chunks and {len(unchanged_chunks)} unchanged chunks")
    return ChunkedArticleDiff(chunked_article=chunked_article, new_chunks=new_chunks, deleted_chunks=deleted_chunks, unchanged_chunks=unchanged_chunks)

# In wikichat/processing/articles.py

async def vectorize_diff(article_diff):
    logging.debug(f"Getting embeddings for article {article_diff.chunked_article.article.metadata.url} which has {len(article_diff.new_chunks)} new chunks")
    vectors, embedding_dimension = await get_embeddings([chunk.content for chunk in article_diff.new_chunks])
    await METRICS.update_chunks(chunks_vectorized=len(vectors))

    try:
        non_zero_vectors = [vector for vector in vectors if isinstance(vector, list) and any(x != 0 for x in vector)]
    except Exception as e:
        logging.error(f"Error processing vectors: {e}")
        non_zero_vectors = []

    zero_vector_count = len(vectors) - len(non_zero_vectors)
    if zero_vector_count > 0:
        logging.debug(f"Skipping article {article_diff.chunked_article.article.metadata.url} because {zero_vector_count} zero vectors were returned")
        for i in range(len(vectors)):
            if isinstance(vectors[i], list) and all([x == 0 for x in vectors[i]]):
                logging.debug(f"Zero vector for chunk in {article_diff.chunked_article.article.metadata.url} content= {article_diff.new_chunks[i].content}")
        await METRICS.update_article(zero_vectors=1)
        return None

    vectored_chunks = [
        {
            "vector": vector,
            "chunked_article": article_diff.chunked_article,
            "chunk": chunk
        }
        for vector, chunk in zip(non_zero_vectors, article_diff.new_chunks)
    ]

    try:
        EMBEDDINGS_COLLECTION.insert_many(vectored_chunks)
        logging.info(f"Inserted {len(vectored_chunks)} vectored chunks into the database")
    except Exception as e:
        logging.error(f"Failed to insert vectored chunks into the database: {e}")

    return VectoredChunkedArticleDiff(
        chunked_article=article_diff.chunked_article,
        new_chunks=[
            VectoredChunk(vector=vector, chunked_article=article_diff.chunked_article, chunk=chunk)
            for vector, chunk in zip(non_zero_vectors, article_diff.new_chunks)
        ],
        deleted_chunks=article_diff.deleted_chunks
    )


async def store_article_diff(article_diff):
    await update_article_metadata(article_diff)
    await insert_vectored_chunks(article_diff.new_chunks)
    await delete_vectored_chunks(article_diff.deleted_chunks)
    return article_diff

async def insert_vectored_chunks(vectored_chunks):
    existing_chunk_logger = logging.getLogger('existing_chunks')
    batch_size = 20
    logging.debug(f"Starting inserting {len(vectored_chunks)} vectored chunks into db using batches of {batch_size}")
    start_all = datetime.now()
    for batch_count, batch in wikichat.utils.batch_list(vectored_chunks, batch_size, enumerate_batches=True):
        start_batch = datetime.now()
        article_embeddings = list(map(EmbeddingDocument.from_vectored_chunk, batch))
        logging.debug(f"Inserting batch number {batch_count} with size {len(batch)}")
        resp = await wikichat.utils.wrap_blocking_io(lambda x: EMBEDDINGS_COLLECTION.insert_many(documents=x, options={"ordered": False}, partial_failures_allowed=True), [article_embedding.to_dict() for article_embedding in article_embeddings])
        errors = resp.get("errors", [])
        exists_errors = [error for error in errors if error.get("errorCode") == "DOCUMENT_ALREADY_EXISTS"]
        if exists_errors:
            logging.debug(f"Got {len(exists_errors)} DOCUMENT_ALREADY_EXISTS errors, ignoring. Chunks {exists_errors}")
            await METRICS.update_database(chunk_collision=len(exists_errors))
            inserted_ids = {doc_id for doc_id in resp["status"]["insertedIds"]}
            for article_embedding in article_embeddings:
                if article_embedding._id not in inserted_ids:
                    doc = article_embedding.to_dict()
                    doc.pop("$vector", None)
                    existing_chunk_logger.warning(doc)
        if len(errors) != len(exists_errors):
            logging.error(f"Got non DOCUMENT_ALREADY_EXISTS errors, stopping: {errors}")
            raise ValueError(json.dumps(errors))
        logging.debug(f"Finished inserting batch number {batch_count} duration {datetime.now() - start_batch}")
    await METRICS.update_database(chunks_inserted=len(vectored_chunks))
    logging.debug(f"Finished inserting {len(vectored_chunks)} article embeddings, total duration {datetime.now() - start_all}")

async def delete_vectored_chunks(chunks):
    batch_size = 20
    logging.debug(f"Starting deleting {len(chunks)} article embedding chunks into db using batches of {batch_size}")
    start_all = datetime.now()
    for batch_count, batch in wikichat.utils.batch_list(chunks, batch_size, enumerate_batches=True):
        start_batch = datetime.now()
        logging.debug(f"Deleting batch number {batch_count} with size {len(batch)}")
        resp = await wikichat.utils.wrap_blocking_io(lambda x: EMBEDDINGS_COLLECTION.delete_many(filter={"_id": {"$in": x}}), [chunk.hash for chunk in batch])
        logging.debug(f"Finished deleting batch number {batch_count} duration {datetime.now() - start_batch}")
    await METRICS.update_database(chunks_deleted=len(chunks))
    logging.debug(f"Finished deleting {len(chunks)} article embeddings total duration {datetime.now() - start_all}")

async def update_article_metadata(vectored_diff):
    new_metadata = ChunkedArticleMetadataOnly.from_vectored_diff(vectored_diff)
    logging.debug(f"Updating article metadata for article url {new_metadata.article_metadata.url}")
    await wikichat.utils.wrap_blocking_io(lambda x: METADATA_COLLECTION.find_one_and_replace(filter={"_id": x._id}, replacement=x.to_dict(), options={"upsert": True}), new_metadata)
    recent_articles = await RECENT_ARTICLES.update_and_clone(new_metadata)
    await wikichat.utils.wrap_blocking_io(lambda x: SUGGESTIONS_COLLECTION.find_one_and_replace(filter={"_id": x._id}, replacement=x.to_dict(), options={"upsert": True}), recent_articles)
    await METRICS.update_database(articles_inserted=1)
    await METRICS.update_article(recent_url=new_metadata.article_metadata.url)

async def process_article_metadata(pipeline, article_metadata):
    for metadata in article_metadata:
        if not await pipeline.put_to_first_step(metadata):
            logging.info(f"Reached max number of items to process ({pipeline.max_items}), stopping.")
            return False
    return True
