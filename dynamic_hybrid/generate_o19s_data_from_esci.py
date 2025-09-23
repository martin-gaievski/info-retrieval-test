#!/usr/bin/env python3
"""Generate O19S format data files from ESCI parquet dataset."""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Generate O19S data files from ESCI dataset."""
    
    # Input/output paths
    # Use the correct parquet file that exists
    esci_path = "esci_data/shopping_queries_dataset_examples.parquet"
    output_dir = Path("data")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    query_train_path = output_dir / "query_train.csv"
    ratings_path = output_dir / "ratings.csv"
    
    # Load ESCI data
    logger.info(f"Loading ESCI data from {esci_path}")
    df = pd.read_parquet(esci_path)
    logger.info(f"Loaded {len(df)} records")
    
    # Check columns
    logger.info(f"Columns: {df.columns.tolist()}")
    
    # Extract unique queries for train set (80% of queries)
    unique_queries = df['query'].unique()
    logger.info(f"Found {len(unique_queries)} unique queries")
    
    # Split queries into train/test (80/20)
    np.random.seed(42)
    np.random.shuffle(unique_queries)
    split_idx = int(len(unique_queries) * 0.8)
    train_queries = unique_queries[:split_idx]
    test_queries = unique_queries[split_idx:]
    
    logger.info(f"Train queries: {len(train_queries)}, Test queries: {len(test_queries)}")
    
    # Create query_train.csv (just the queries)
    query_train_df = pd.DataFrame({
        'query_string': train_queries  # Use 'query_string' to match O19S format
    })
    query_train_df.to_csv(query_train_path, index=False)
    logger.info(f"Wrote {len(query_train_df)} queries to {query_train_path}")
    
    # Create ratings.csv (query-product pairs with ESCI labels)
    # Filter to only train queries
    train_df = df[df['query'].isin(train_queries)].copy()
    
    # Map ESCI labels to numeric ratings
    # 'E' (Exact) -> 3
    # 'S' (Substitute) -> 2  
    # 'C' (Complement) -> 1
    # 'I' (Irrelevant) -> 0
    rating_map = {'E': 3, 'S': 2, 'C': 1, 'I': 0}
    train_df['rating'] = train_df['esci_label'].map(rating_map)
    
    # Create ratings dataframe with required columns
    ratings_df = pd.DataFrame({
        'query': train_df['query'],
        'product_id': train_df['product_id'],
        'rating': train_df['rating']
    })
    
    # Remove any rows with NaN ratings
    ratings_df = ratings_df.dropna(subset=['rating'])
    
    # Sort by query for better organization
    ratings_df = ratings_df.sort_values('query')
    
    ratings_df.to_csv(ratings_path, index=False)
    logger.info(f"Wrote {len(ratings_df)} ratings to {ratings_path}")
    
    # Summary statistics
    logger.info("\n=== SUMMARY ===")
    logger.info(f"Train queries: {len(train_queries)}")
    logger.info(f"Test queries: {len(test_queries)}")
    logger.info(f"Total ratings: {len(ratings_df)}")
    logger.info(f"Avg ratings per query: {len(ratings_df) / len(train_queries):.1f}")
    
    # Rating distribution
    rating_dist = ratings_df['rating'].value_counts().sort_index()
    logger.info("\nRating distribution:")
    for rating, count in rating_dist.items():
        label = {3: 'Exact', 2: 'Substitute', 1: 'Complement', 0: 'Irrelevant'}[rating]
        logger.info(f"  {rating} ({label}): {count} ({count/len(ratings_df)*100:.1f}%)")
    
    # Sample some data to verify
    logger.info("\nSample ratings:")
    sample = ratings_df.head(5)
    for _, row in sample.iterrows():
        logger.info(f"  Query: '{row['query'][:50]}...' -> Product: {row['product_id']} -> Rating: {row['rating']}")
    
    logger.info("\n✓ O19S data files generated successfully!")
    logger.info(f"  - {query_train_path}")
    logger.info(f"  - {ratings_path}")

if __name__ == "__main__":
    main()
