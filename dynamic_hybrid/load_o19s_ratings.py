#!/usr/bin/env python3
"""
Load O19S ratings data for training.
"""

import pandas as pd
import json
from collections import defaultdict
from pathlib import Path

def load_ratings_data(ratings_path: str = "dynamic_hybrid/data/ratings.csv", 
                      sample_size: int = None):
    """
    Load ratings data and convert to query-based format.
    
    Args:
        ratings_path: Path to ratings.csv file
        sample_size: Number of unique queries to sample (None for all)
        
    Returns:
        DataFrame with columns: query_id, query, ratings (dict)
    """
    print(f"Loading ratings from {ratings_path}")
    
    # Read the tab-separated file without headers
    df = pd.read_csv(ratings_path, sep='\t', header=None, 
                     names=['query', 'product_id', 'rating', 'extra'])
    
    # Remove any rows with NaN in critical columns
    df = df.dropna(subset=['query', 'product_id', 'rating'])
    
    # Convert rating to int
    df['rating'] = df['rating'].astype(int)
    
    # Group by query
    query_ratings = defaultdict(dict)
    for _, row in df.iterrows():
        query = row['query']
        product_id = row['product_id']
        rating = row['rating']
        query_ratings[query][product_id] = rating
    
    # Convert to DataFrame format
    result_data = []
    for idx, (query, ratings) in enumerate(query_ratings.items()):
        result_data.append({
            'query_id': f'q_{idx}',
            'query': query,
            'ratings': ratings
        })
    
    result_df = pd.DataFrame(result_data)
    
    # Sample if needed
    if sample_size and sample_size < len(result_df):
        result_df = result_df.sample(n=sample_size, random_state=42)
        result_df = result_df.reset_index(drop=True)
    
    print(f"Loaded {len(result_df)} unique queries")
    print(f"Average products per query: {result_df['ratings'].apply(len).mean():.1f}")
    
    # Show rating distribution
    all_ratings = []
    for ratings_dict in result_df['ratings']:
        all_ratings.extend(ratings_dict.values())
    
    rating_dist = pd.Series(all_ratings).value_counts().sort_index()
    print("\nRating distribution:")
    for rating, count in rating_dist.items():
        print(f"  Rating {rating}: {count} ({100*count/len(all_ratings):.1f}%)")
    
    return result_df


if __name__ == "__main__":
    # Test loading
    df = load_ratings_data(sample_size=10)
    
    print("\nSample queries:")
    for idx, row in df.head(3).iterrows():
        print(f"Query: {row['query'][:50]}...")
        print(f"  Products rated: {len(row['ratings'])}")
        print(f"  Sample ratings: {dict(list(row['ratings'].items())[:3])}")
