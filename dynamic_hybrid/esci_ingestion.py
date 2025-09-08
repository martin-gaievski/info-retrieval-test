#!/usr/bin/env python3
"""
ESCI Data Ingestion Script for Dynamic Hybrid Search
===================================================

This script ingests Amazon ESCI (Shopping Queries) dataset into OpenSearch
with proper field mappings for hybrid search evaluation.

Features:
- Creates optimal index mapping for ESCI product data
- Sets up ML pipeline for automatic embedding generation
- Supports both small (~1K) and full (~100K+) dataset versions
- Tests BM25, neural, and hybrid search functionality
- Configurable document limits for testing
- US-only product filtering to match O19S methodology

Usage Examples:
  # Ingest sample data with model
  python esci_ingestion.py -m MODEL_ID -d esci_data

  # Use full dataset instead of small version  
  python esci_ingestion.py -m MODEL_ID -d esci_data --full-dataset

  # Limit to specific number of documents
  python esci_ingestion.py -m MODEL_ID -d esci_data -n 5000

  # Test search functionality only
  python esci_ingestion.py -m MODEL_ID --test-only

Field Mappings:
  product_title → title_embedding (via ML pipeline)
  product_title → BM25 search field
  
Author: Dynamic Hybrid Search Team
License: Apache 2.0
"""

import os
import sys
import logging
import getopt
import pathlib
from opensearchpy import OpenSearch
from tqdm import tqdm
import json

# Add parent directory to path for BEIR imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from beir import util, LoggingHandler
from beir.datasets.data_loader_esci import DataLoader as ESCIDataLoader
from beir.hybrid.data_ingestor import OpenSearchDataIngestor

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[LoggingHandler()]
)
logger = logging.getLogger(__name__)

# Script version for tracking
__version__ = "1.1.0"


def load_esci_index_mapping():
    """
    Load ESCI index mapping from external JSON file.
    
    Returns:
        dict: OpenSearch index mapping with settings and field definitions
    """
    mapping_file = os.path.join(os.path.dirname(__file__), 'esci_index_mapping.json')
    
    try:
        with open(mapping_file, 'r') as f:
            mapping = json.load(f)
        logger.info(f"Loaded index mapping from {mapping_file}")
        return mapping
    except FileNotFoundError:
        logger.error(f"Index mapping file not found: {mapping_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in mapping file: {e}")
        raise


def filter_products_for_o19s_compatibility(corpus):
    """
    Filter products to match O19S methodology - US locale only.
    
    Args:
        corpus (dict): Full product corpus
        
    Returns:
        dict: Filtered corpus with US products only
    """
    original_count = len(corpus)
    
    # Filter for US products only (matching O19S approach)
    us_corpus = {}
    for doc_id, doc in corpus.items():
        if doc.get('product_locale') == 'us':
            us_corpus[doc_id] = doc
    
    filtered_count = len(us_corpus)
    logger.info(f"Product filtering for O19S compatibility:")
    logger.info(f"  Total documents before filtering: {original_count}")
    logger.info(f"  Total documents after filtering (US only): {filtered_count}")
    logger.info(f"  Filtered out: {original_count - filtered_count} non-US products")
    
    return us_corpus


def setup_index_with_pipeline(endpoint, port, index_name, model_id):
    """
    Set up OpenSearch index with proper mapping and ML pipeline.
    
    Args:
        endpoint (str): OpenSearch host
        port (int): OpenSearch port  
        index_name (str): Name of index to create
        model_id (str): ML model ID for embeddings
        
    Returns:
        OpenSearch: Configured OpenSearch client
    """
    client = OpenSearch(
        hosts=[{'host': endpoint, 'port': port}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        timeout=30
    )
    
    # Verify cluster is healthy
    try:
        health = client.cluster.health()
        if health['status'] == 'red':
            logger.warning("Cluster health is RED - proceed with caution")
        logger.info(f"Cluster health: {health['status']}")
    except Exception as e:
        logger.error(f"Cannot connect to OpenSearch: {e}")
        raise
    
    # Delete existing index if present
    if client.indices.exists(index=index_name):
        logger.info(f"Deleting existing index '{index_name}'")
        client.indices.delete(index=index_name)
    
    # Create index with mapping from external file
    logger.info(f"Creating index '{index_name}' with external mapping file")
    mapping = load_esci_index_mapping()
    client.indices.create(index=index_name, body=mapping)
    
    # Create ML pipeline for automatic embedding generation
    pipeline_name = "esci-embedding-pipeline"
    pipeline_body = {
        "description": "Pipeline for ESCI product_title to title_embedding conversion",
        "processors": [
            {
                "text_embedding": {
                    "model_id": model_id,
                    "field_map": {
                        "product_title": "title_embedding"
                    }
                }
            }
        ]
    }
    
    try:
        # Remove existing pipeline if present
        try:
            client.ingest.delete_pipeline(id=pipeline_name)
        except:
            pass  # Pipeline didn't exist
            
        client.ingest.put_pipeline(id=pipeline_name, body=pipeline_body)
        logger.info(f"Created ML pipeline '{pipeline_name}' for automatic embeddings")
        
        # Verify pipeline was created
        pipeline_info = client.ingest.get_pipeline(id=pipeline_name)
        logger.info("✓ ML pipeline verified and active")
        
    except Exception as e:
        logger.error(f"Failed to create ML pipeline: {e}")
        logger.error("Proceeding without embeddings - only BM25 search will work")
        raise
    
    return client


def ingest_esci_data(corpus, endpoint, index, port, model_id):
    """
    Ingest ESCI product data with bulk processing and progress tracking.
    
    Args:
        corpus (dict): Product data from ESCI loader (already US-filtered)
        endpoint (str): OpenSearch host
        index (str): Target index name
        port (int): OpenSearch port
        model_id (str): ML model ID for embeddings
    """
    logger.info(f"Setting up index and ingesting {len(corpus)} US products...")
    
    # Setup index with mapping and ML pipeline
    client = setup_index_with_pipeline(endpoint, port, index, model_id)
    
    # Check if index already has documents
    try:
        count_response = client.count(index=index)
        current_count = count_response["count"]
        if current_count > 0:
            logger.info(f"Index '{index}' already contains {current_count} documents.")
            logger.info("Delete the index to re-ingest or use a different index name.")
            return
    except Exception as e:
        logger.error(f"Error checking document count: {e}")
        return
    
    # Start bulk ingestion
    logger.info("Starting bulk ingestion with ML pipeline processing...")
    
    bulk_size = 400
    actions = []
    ingested_count = 0
    
    for doc_id, doc in tqdm(corpus.items(), desc="Processing products"):
        # Prepare bulk action
        action = {"index": {"_index": index, "_id": doc_id}}
        actions.append(action)
        actions.append(doc)  # Document already in correct format from loader
        
        # Execute bulk when batch is full
        if len(actions) >= bulk_size * 2:  # *2 for action/document pairs
            try:
                response = client.bulk(body=actions, timeout=60)
                
                # Check for errors
                if response.get('errors'):
                    errors = [item for item in response['items'] if 'error' in item.get('index', {})]
                    if errors:
                        logger.error(f"Bulk ingestion errors: {errors[:3]}...")  # Show first 3 errors
                else:
                    ingested_count += len(actions) // 2
                
                actions = []
                
            except Exception as e:
                logger.error(f"Bulk ingestion batch failed: {e}")
                actions = []
    
    # Process remaining documents
    if actions:
        try:
            response = client.bulk(body=actions, timeout=60)
            if not response.get('errors'):
                ingested_count += len(actions) // 2
        except Exception as e:
            logger.error(f"Final bulk ingestion failed: {e}")
    
    # Refresh index and verify final count
    logger.info("Refreshing index and generating embeddings...")
    client.indices.refresh(index=index)
    
    # Wait a moment for pipeline processing
    import time
    time.sleep(2)
    
    final_count = client.count(index=index)["count"]
    logger.info(f"✓ Ingestion complete! Documents in index: {final_count}")
    
    if final_count != len(corpus):
        logger.warning(f"Expected {len(corpus)} documents, but index contains {final_count}")


def detect_sample_queries(client, index_name, num_samples=5):
    """
    Detect sample product titles for testing search functionality.
    
    Args:
        client: OpenSearch client
        index_name (str): Index to sample from
        num_samples (int): Number of sample titles to retrieve
        
    Returns:
        list: Sample product titles for testing
    """
    try:
        # Get random sample of documents
        sample_query = {
            "size": num_samples,
            "query": {"match_all": {}},
            "_source": ["product_title"]
        }
        
        response = client.search(index=index_name, body=sample_query)
        hits = response.get('hits', {}).get('hits', [])
        
        sample_titles = []
        for hit in hits:
            title = hit['_source'].get('product_title', '')
            if title:
                # Extract meaningful keywords from titles
                words = title.split()
                if len(words) > 1:
                    # Use first meaningful word as test query
                    for word in words:
                        if len(word) > 3 and word.isalpha():
                            sample_titles.append(word)
                            break
        
        return sample_titles[:3] if sample_titles else ["Electronics", "Product", "Sample"]
        
    except Exception as e:
        logger.warning(f"Could not detect sample queries: {e}")
        return ["Electronics", "Product", "Sample"]


def test_search(endpoint, port, index_name, model_id):
    """
    Test all search functionality with automatic query detection.
    
    Args:
        endpoint (str): OpenSearch host
        port (int): OpenSearch port
        index_name (str): Index to test
        model_id (str): ML model ID for neural search
    """
    client = OpenSearch(
        hosts=[{'host': endpoint, 'port': port}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False
    )
    
    # Detect sample queries from actual data
    logger.info("Detecting sample queries from indexed data...")
    test_queries = detect_sample_queries(client, index_name)
    
    for i, test_query in enumerate(test_queries):
        logger.info(f"\n--- Test {i+1}: Query '{test_query}' ---")
        
        # Test BM25 search
        logger.info("Testing BM25 search...")
        bm25_query = {
            "query": {"match": {"product_title": test_query}},
            "size": 3
        }
        
        try:
            response = client.search(index=index_name, body=bm25_query)
            hits = response.get('hits', {}).get('hits', [])
            logger.info(f"BM25: {len(hits)} results")
            if hits:
                logger.info(f"  Top result: {hits[0]['_source']['product_title'][:60]}...")
        except Exception as e:
            logger.error(f"BM25 search failed: {e}")
        
        # Test neural search (if model available)
        if model_id:
            logger.info("Testing neural search...")
            neural_query = {
                "query": {
                    "neural": {
                        "title_embedding": {
                            "query_text": test_query,
                            "model_id": model_id,
                            "k": 3
                        }
                    }
                },
                "size": 3
            }
            
            try:
                response = client.search(index=index_name, body=neural_query)
                hits = response.get('hits', {}).get('hits', [])
                logger.info(f"Neural: {len(hits)} results")
                if hits:
                    logger.info(f"  Top result: {hits[0]['_source']['product_title'][:60]}...")
            except Exception as e:
                logger.error(f"Neural search failed: {e}")
        
        # Test hybrid search (if model available)
        if model_id:
            logger.info("Testing hybrid search...")
            hybrid_query = {
                "query": {
                    "hybrid": {
                        "queries": [
                            {"match": {"product_title": test_query}},
                            {
                                "neural": {
                                    "title_embedding": {
                                        "query_text": test_query,
                                        "model_id": model_id,
                                        "k": 3
                                    }
                                }
                            }
                        ]
                    }
                },
                "size": 3
            }
            
            try:
                response = client.search(index=index_name, body=hybrid_query)
                hits = response.get('hits', {}).get('hits', [])
                logger.info(f"Hybrid: {len(hits)} results")
                if hits:
                    logger.info(f"  Top result: {hits[0]['_source']['product_title'][:60]}...")
            except Exception as e:
                logger.error(f"Hybrid search failed: {e}")
        
        if i == 0:  # Only test first query in detail
            break
    
    logger.info("\n✓ Search functionality testing complete!")


def print_usage():
    """Print detailed usage information."""
    print(f"""
ESCI Data Ingestion Script v{__version__}
========================================

Usage: python esci_ingestion.py [OPTIONS]

Required:
  -m, --model-id ID        ML model ID for embeddings (required for neural/hybrid search)

Options:
  -h, --host HOST          OpenSearch host (default: localhost)
  -p, --port PORT          OpenSearch port (default: 9200)  
  -i, --index INDEX        Index name (default: esci-products)
  -d, --data-folder PATH   Data folder path (default: esci_data)
  -t, --test-only          Only test search functionality, don't ingest
  -f, --full-dataset       Use full dataset instead of small version
  -n, --max-docs NUM       Maximum documents to ingest (for testing)
  --help                   Show this help message

Examples:
  # Basic ingestion with sample data
  python esci_ingestion.py -m abc123 -d esci_data

  # Full dataset ingestion
  python esci_ingestion.py -m abc123 -d esci_data --full-dataset

  # Test search functionality only  
  python esci_ingestion.py -m abc123 --test-only

  # Limit ingestion for testing
  python esci_ingestion.py -m abc123 -d esci_data -n 1000

Data Files Required:
  esci_data/shopping_queries_dataset_products_us_small.parquet
  esci_data/shopping_queries_dataset_examples_us_small.parquet

For full dataset, use files without '_small' suffix.

O19S Compatibility:
  - Only US products are ingested (product_locale == 'us')
  - Index mapping loaded from esci_index_mapping.json
    """)


def main():
    """Main function with improved argument parsing and error handling."""
    
    # Default configuration
    config = {
        'endpoint': 'localhost',
        'port': 9200,
        'index': 'esci-products',
        'model_id': None,
        'data_folder': 'esci_data',
        'test_only': False,
        'small_version': True,
        'max_documents': None
    }
    
    # Parse command line arguments
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], 
            "h:p:i:m:d:tfn:", 
            ["host=", "port=", "index=", "model-id=", "data-folder=", 
             "test-only", "full-dataset", "max-docs=", "help"]
        )
    except getopt.GetoptError as e:
        logger.error(f"Error parsing arguments: {e}")
        print_usage()
        sys.exit(1)
    
    # Process arguments
    for opt, arg in opts:
        if opt in ("-h", "--host"):
            config['endpoint'] = arg
        elif opt in ("-p", "--port"):
            config['port'] = int(arg)
        elif opt in ("-i", "--index"):
            config['index'] = arg
        elif opt in ("-m", "--model-id"):
            config['model_id'] = arg
        elif opt in ("-d", "--data-folder"):
            config['data_folder'] = arg
        elif opt in ("-t", "--test-only"):
            config['test_only'] = True
        elif opt in ("-f", "--full-dataset"):
            config['small_version'] = False
        elif opt in ("-n", "--max-docs"):
            config['max_documents'] = int(arg)
        elif opt == "--help":
            print_usage()
            sys.exit(0)
    
    # Validate required parameters
    if not config['model_id'] and not config['test_only']:
        logger.error("Model ID is required for ingestion. Use -m/--model-id or --test-only")
        print_usage()
        sys.exit(1)
    
    # Display configuration
    logger.info(f"ESCI Data Ingestion v{__version__}")
    logger.info("Configuration:")
    for key, value in config.items():
        if key == 'small_version':
            logger.info(f"  Dataset version: {'small' if value else 'full'}")
        elif key == 'max_documents':
            logger.info(f"  Document limit: {value if value else 'none'}")
        else:
            logger.info(f"  {key.replace('_', ' ').title()}: {value}")
    
    # Test search functionality only
    if config['test_only']:
        test_search(config['endpoint'], config['port'], config['index'], config['model_id'])
        return
    
    # Validate data folder
    if not os.path.exists(config['data_folder']):
        logger.error(f"Data folder '{config['data_folder']}' does not exist")
        logger.error("Please ensure ESCI parquet files are available:")
        if config['small_version']:
            logger.error("  - shopping_queries_dataset_products_us_small.parquet")
            logger.error("  - shopping_queries_dataset_examples_us_small.parquet")
        else:
            logger.error("  - shopping_queries_dataset_products_us.parquet")
            logger.error("  - shopping_queries_dataset_examples_us.parquet")
        sys.exit(1)
    
    # Load ESCI dataset
    logger.info(f"Loading ESCI dataset ({'small' if config['small_version'] else 'full'} version)...")
    try:
        loader = ESCIDataLoader(
            data_folder=config['data_folder'],
            language="us",
            small_version=config['small_version']
        )
        corpus, queries, qrels = loader.load(split="test")
        logger.info(f"Loaded {len(corpus)} products, {len(queries)} queries, {len(qrels)} relevance judgments")
        
        # Apply O19S product filtering (US locale only)
        logger.info("Applying O19S-compatible product filtering...")
        corpus = filter_products_for_o19s_compatibility(corpus)
        
        # Apply document limit if specified (after filtering)
        if config['max_documents'] and config['max_documents'] < len(corpus):
            logger.info(f"Limiting corpus to {config['max_documents']} documents")
            corpus_items = list(corpus.items())[:config['max_documents']]
            corpus = dict(corpus_items)
            logger.info(f"Final corpus size: {len(corpus)} products")
        
        # Show sample document structure
        if corpus:
            sample_doc = list(corpus.values())[0]
            logger.info("Sample document structure:")
            for key, value in sample_doc.items():
                value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                logger.info(f"  {key}: {value_preview}")
        
    except Exception as e:
        logger.error(f"Failed to load ESCI dataset: {e}")
        sys.exit(1)
    
    # Execute ingestion
    try:
        ingest_esci_data(
            corpus, 
            config['endpoint'], 
            config['index'], 
            config['port'], 
            config['model_id']
        )
        logger.info("✓ ESCI data ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        sys.exit(1)
    
    # Test search functionality
    logger.info("Testing search functionality...")
    try:
        test_search(config['endpoint'], config['port'], config['index'], config['model_id'])
    except Exception as e:
        logger.error(f"Search testing failed: {e}")
    
    logger.info("✓ ESCI ingestion and validation complete!")
    logger.info(f"Ready for dynamic hybrid search evaluation with index '{config['index']}'")


if __name__ == "__main__":
    main()
