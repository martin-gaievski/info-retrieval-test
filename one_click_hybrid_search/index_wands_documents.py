#!/usr/bin/env python3
"""
Index WANDS documents into OpenSearch.
This script reads products from the WANDS dataset and indexes them into OpenSearch.
Can be run independently for testing or debugging.
"""

import sys
import json
import pandas as pd
from opensearchpy import OpenSearch, helpers
import argparse
from pathlib import Path


def load_wands_products(wands_path: str, sample_size: int = None):
    """
    Load products from the WANDS dataset.
    
    Args:
        wands_path: Path to the WANDS dataset directory
        sample_size: Number of products to sample (None for all)
    
    Returns:
        DataFrame containing product data
    """
    product_file = Path(wands_path) / "dataset" / "product.csv"
    
    if not product_file.exists():
        raise FileNotFoundError(f"Product file not found: {product_file}")
    
    print(f"Loading products from: {product_file}")
    
    # Load WANDS products (tab-separated file)
    try:
        # Read with tab separator and handle bad lines
        products_df = pd.read_csv(product_file, sep='\t', on_bad_lines='skip')
        print(f"  Loaded {len(products_df)} products from WANDS")
        
        # Sample products if requested
        if sample_size and sample_size < len(products_df):
            products_df = products_df.head(sample_size)
            print(f"  Sampled {sample_size} products")
        
        return products_df
        
    except Exception as e:
        raise Exception(f"Error loading products: {e}")


def prepare_documents(products_df, index_name: str):
    """
    Prepare documents for bulk indexing.
    
    Args:
        products_df: DataFrame containing product data
        index_name: Name of the OpenSearch index
    
    Returns:
        List of document actions for bulk indexing
    """
    actions = []
    
    for _, row in products_df.iterrows():
        # Handle NaN values and extract fields
        product_id = str(row.get('product_id', ''))
        product_name = row.get('product_name', '')
        product_class = row.get('product_class', '')
        product_description = row.get('product_description', '')
        product_features = row.get('product_features', '')
        
        # Skip if product_id is invalid
        if not product_id or pd.isna(product_id) or product_id == 'nan':
            print(f"  Warning: Skipping product with invalid ID: {product_id}")
            continue
        
        # Create document source
        doc_source = {
            "product_id": product_id
        }
        
        # Always add product_name (100% have this field)
        product_name_str = str(product_name) if not pd.isna(product_name) else ""
        doc_source["product_name"] = product_name_str
        
        # Add other fields if they have non-empty values
        if not pd.isna(product_class) and str(product_class).strip():
            doc_source["product_class"] = str(product_class)
        
        if not pd.isna(product_description) and str(product_description).strip():
            doc_source["product_description"] = str(product_description)
        
        if not pd.isna(product_features) and str(product_features).strip():
            doc_source["product_features"] = str(product_features)
        
        # Create combined search field: product_name + product_description
        # This ensures every document has searchable text even if description is empty
        product_desc_str = str(product_description) if not pd.isna(product_description) else ""
        product_search = f"{product_name_str} {product_desc_str}".strip()
        doc_source["product_search"] = product_search
        
        # Add placeholder embedding if needed (will be replaced by pipeline if available)
        # Note: Using 384 dimensions for all-MiniLM-L12-v2 model
        doc_source["product_embedding"] = [0.0] * 384
        
        # Create document
        doc = {
            "_index": index_name,
            "_id": product_id,
            "_source": doc_source
        }
        
        actions.append(doc)
    
    print(f"  Prepared {len(actions)} documents for indexing")
    return actions


def index_documents(client, actions, chunk_size: int = 50, progress_interval: int = 2000):
    """
    Bulk index documents into OpenSearch with progress reporting.
    
    Args:
        client: OpenSearch client
        actions: List of document actions
        chunk_size: Number of documents to index per batch
        progress_interval: Report progress every N documents
    
    Returns:
        Tuple of (success_count, failed_items)
    """
    import time
    from datetime import datetime, timedelta
    
    if not actions:
        print("  No documents to index")
        return 0, []
    
    total_docs = len(actions)
    print(f"  Starting bulk indexing ({total_docs:,} documents)...")
    print(f"  Chunk size: {chunk_size}, Progress update every {progress_interval:,} documents")
    print("-" * 70)
    
    start_time = time.time()
    success_count = 0
    failed_items = []
    processed_count = 0
    
    try:
        # Process in batches
        for i in range(0, len(actions), chunk_size):
            batch = actions[i:i + chunk_size]
            batch_size = len(batch)
            
            # Index the batch
            try:
                success, failed = helpers.bulk(
                    client, 
                    batch, 
                    chunk_size=chunk_size, 
                    raise_on_error=False,
                    max_retries=3,
                    initial_backoff=2
                )
                
                success_count += success
                if failed:
                    failed_items.extend(failed)
                
                processed_count += batch_size
                
                # Report progress at intervals
                if processed_count % progress_interval == 0 or processed_count == total_docs:
                    elapsed_time = time.time() - start_time
                    docs_per_second = processed_count / elapsed_time if elapsed_time > 0 else 0
                    
                    # Calculate estimated time remaining
                    remaining_docs = total_docs - processed_count
                    if docs_per_second > 0:
                        eta_seconds = remaining_docs / docs_per_second
                        eta_str = str(timedelta(seconds=int(eta_seconds)))
                    else:
                        eta_str = "calculating..."
                    
                    # Progress percentage
                    progress_pct = (processed_count / total_docs) * 100
                    
                    print(f"  Progress: {processed_count:,}/{total_docs:,} documents ({progress_pct:.1f}%)")
                    print(f"    - Successfully indexed: {success_count:,}")
                    print(f"    - Failed: {len(failed_items):,}")
                    print(f"    - Rate: {docs_per_second:.1f} docs/sec")
                    if processed_count < total_docs:
                        print(f"    - ETA: {eta_str}")
                    print()
                    
            except Exception as e:
                print(f"  Warning: Error in batch {i//chunk_size + 1}: {e}")
                # Continue with next batch
                continue
        
        # Final summary
        print("-" * 70)
        total_time = time.time() - start_time
        print(f"  Indexing completed in {timedelta(seconds=int(total_time))}")
        print(f"  Successfully indexed: {success_count:,} documents")
        
        if failed_items:
            print(f"  Failed to index: {len(failed_items)} documents")
            # Print first few failures for debugging
            for item in failed_items[:5]:
                print(f"    Error: {item}")
        
        return success_count, failed_items
        
    except Exception as e:
        print(f"  Bulk indexing error: {e}")
        raise


def verify_index(client, index_name: str):
    """
    Verify the index exists and get document count.
    
    Args:
        client: OpenSearch client
        index_name: Name of the index
    
    Returns:
        Document count or -1 if index doesn't exist
    """
    try:
        # Check if index exists
        if not client.indices.exists(index_name):
            print(f"  Warning: Index '{index_name}' does not exist")
            return -1
        
        # Get document count
        response = client.count(index=index_name)
        doc_count = response['count']
        print(f"  Index '{index_name}' contains {doc_count} documents")
        return doc_count
        
    except Exception as e:
        print(f"  Error verifying index: {e}")
        return -1


def main():
    """Main function to index WANDS products into OpenSearch."""
    
    parser = argparse.ArgumentParser(
        description='Index WANDS products into OpenSearch'
    )
    parser.add_argument('--host', default='localhost', 
                       help='OpenSearch host (default: localhost)')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port (default: 9200)')
    parser.add_argument('--index', default='wands_products',
                       help='Index name (default: wands_products)')
    parser.add_argument('--sample-size', type=int, default=100,
                       help='Number of products to index (default: 100, use -1 for all)')
    parser.add_argument('--wands-path', default='/Users/gaievski/dev/datasets/WANDS',
                       help='Path to WANDS dataset')
    parser.add_argument('--chunk-size', type=int, default=50,
                       help='Bulk indexing chunk size (default: 50)')
    parser.add_argument('--use-ssl', action='store_true',
                       help='Use SSL for OpenSearch connection')
    parser.add_argument('--username', help='OpenSearch username (if auth enabled)')
    parser.add_argument('--password', help='OpenSearch password (if auth enabled)')
    
    args = parser.parse_args()
    
    # Validate WANDS path
    wands_path = Path(args.wands_path)
    if not wands_path.exists():
        print(f"Error: WANDS dataset not found at {wands_path}")
        print("Please download the WANDS dataset from: https://github.com/wayfair/WANDS")
        sys.exit(1)
    
    # Prepare OpenSearch connection parameters
    client_config = {
        'hosts': [{'host': args.host, 'port': args.port}],
        'use_ssl': args.use_ssl,
        'verify_certs': False,
        'ssl_show_warn': False
    }
    
    # Add authentication if provided
    if args.username and args.password:
        client_config['http_auth'] = (args.username, args.password)
    
    # Connect to OpenSearch
    print(f"Connecting to OpenSearch at {args.host}:{args.port}...")
    try:
        client = OpenSearch(**client_config)
        
        # Test connection
        info = client.info()
        print(f"  Connected to OpenSearch version: {info['version']['number']}")
        
    except Exception as e:
        print(f"Error connecting to OpenSearch: {e}")
        print("\nMake sure OpenSearch is running and accessible.")
        print("To start OpenSearch with Docker:")
        print("  docker run -d --name opensearch \\")
        print("    -p 9200:9200 -p 9600:9600 \\")
        print("    -e 'discovery.type=single-node' \\")
        print("    -e 'plugins.security.disabled=true' \\")
        print("    opensearchproject/opensearch:latest")
        sys.exit(1)
    
    # Load products
    try:
        sample_size = None if args.sample_size == -1 else args.sample_size
        products_df = load_wands_products(args.wands_path, sample_size)
    except Exception as e:
        print(f"Error loading products: {e}")
        sys.exit(1)
    
    # Prepare documents
    actions = prepare_documents(products_df, args.index)
    
    if not actions:
        print("No valid documents to index")
        sys.exit(1)
    
    # Index documents
    try:
        success, failed = index_documents(client, actions, args.chunk_size)
        
        # Verify index
        verify_index(client, args.index)
        
        # Exit with appropriate code
        if success > 0:
            print(f"\nSuccessfully indexed {success} documents")
            sys.exit(0)
        else:
            print("\nNo documents were indexed successfully")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error during indexing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
