#!/usr/bin/env python3
"""
O19S-Compatible Ratings Generator

Replicates O19S methodology for creating ratings.csv:
1. Load O19S pre-selected query sets (query_train.csv + query_test.csv)
2. Load ESCI examples (already US-filtered in small version)
3. Match queries with judgments and apply O19S rating conversion
4. Generate ratings.csv in O19S format: query, docid, rating, idx

Author: Dynamic Hybrid Search Team
Version: 1.0.0
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def load_o19s_queries(data_dir: str) -> pd.DataFrame:
    """
    Load O19S pre-selected query sets.
    
    Args:
        data_dir: Path to O19S data directory containing query_train.csv and query_test.csv
        
    Returns:
        Combined DataFrame with all O19S queries
    """
    train_file = Path(data_dir) / 'query_train.csv'
    test_file = Path(data_dir) / 'query_test.csv'
    
    if not train_file.exists():
        raise FileNotFoundError(f"O19S train queries not found: {train_file}")
    if not test_file.exists():
        raise FileNotFoundError(f"O19S test queries not found: {test_file}")
    
    logger.info(f"Loading O19S query sets from {data_dir}")
    
    df_train = pd.read_csv(train_file)
    df_test = pd.read_csv(test_file)
    
    logger.info(f"Loaded {len(df_train)} train queries and {len(df_test)} test queries")
    
    # Combine train and test sets (as O19S does)
    df_query_set = pd.concat([df_train, df_test], ignore_index=True)
    
    logger.info(f"Combined query set: {len(df_query_set)} total queries")
    
    return df_query_set


def load_esci_examples(esci_data_dir: str, small_version: bool = True) -> pd.DataFrame:
    """
    Load ESCI examples dataset (already US-filtered for small version).
    
    Args:
        esci_data_dir: Path to ESCI data directory
        small_version: Whether to use small version of dataset
        
    Returns:
        DataFrame with ESCI examples (US-filtered if small version)
    """
    if small_version:
        examples_file = Path(esci_data_dir) / 'shopping_queries_dataset_examples_us_small.parquet'
    else:
        examples_file = Path(esci_data_dir) / 'shopping_queries_dataset_examples.parquet'
    
    if not examples_file.exists():
        raise FileNotFoundError(f"ESCI examples not found: {examples_file}")
    
    logger.info(f"Loading ESCI examples from {examples_file}")
    
    df_examples = pd.read_parquet(examples_file)
    logger.info(f"Loaded {len(df_examples)} ESCI example rows")
    
    # For small version, data is already US-filtered
    if small_version:
        logger.info("Using small version - data is already US-filtered")
        return df_examples
    else:
        # For full version, we would need to filter by product_locale
        # But this would require loading the products file too
        logger.warning("Full version filtering not implemented - assuming US-filtered data")
        return df_examples


def create_o19s_ratings(df_query_set: pd.DataFrame, df_examples: pd.DataFrame) -> pd.DataFrame:
    """
    Create O19S-compatible ratings following their exact methodology.
    
    Args:
        df_query_set: O19S pre-selected queries
        df_examples: ESCI examples (US-filtered)
        
    Returns:
        DataFrame with O19S-compatible ratings
    """
    logger.info("Creating O19S-compatible ratings...")
    
    # O19S rating conversion: E/S/C/I → numerical scores
    # Their mapping: {"E": 0, "S": 1, "C": 2, "I": 3}
    # Their score conversion: [3, 2, 1, 0] (reverse of mapping)
    label_num = {"E": 0, "S": 1, "C": 2, "I": 3}
    label_score = [3, 2, 1, 0]
    
    def label_to_score(label):
        return label_score[label_num[label]]
    
    # Get unique query strings from O19S sets
    o19s_query_strings = set(df_query_set["query_string"].values)
    logger.info(f"O19S unique query strings: {len(o19s_query_strings)}")
    
    # Filter ESCI examples to only include O19S queries
    df_judge = df_examples[df_examples["query"].isin(o19s_query_strings)].copy()
    logger.info(f"ESCI examples matching O19S queries: {len(df_judge)}")
    
    if len(df_judge) == 0:
        logger.error("No matching queries found between O19S sets and ESCI examples!")
        logger.info("Sample O19S queries:")
        for i, q in enumerate(list(o19s_query_strings)[:5]):
            logger.info(f"  {i+1}: '{q}'")
        logger.info("Sample ESCI queries:")
        for i, q in enumerate(df_examples["query"].unique()[:5]):
            logger.info(f"  {i+1}: '{q}'")
        return pd.DataFrame()
    
    # Apply O19S rating conversion
    df_judge["judgment"] = df_judge.esci_label.apply(lambda x: label_to_score(x))
    df_judge["document"] = df_judge.product_id
    
    # Select required columns (matching O19S format)
    df_judge = df_judge[["query", "document", "judgment"]].reset_index(drop=True)
    
    logger.info(f"Created {len(df_judge)} judgment records")
    
    # Group by queries and create query indices (as O19S does)
    df_queries = df_judge.groupby(by='query', as_index=False).agg({
        'judgment': ['count']
    })
    df_query_idx = df_queries['query']
    
    # Create query index mapping
    df_query_idx = pd.DataFrame(df_query_idx)
    df_query_idx = df_query_idx.reset_index().rename(columns={'index': 'idx'})
    
    # Merge judgments with query indices
    df_merged = pd.merge(df_judge, df_query_idx, on='query', how='left')
    df_merged.columns = ['query', 'docid', 'rating', 'idx']
    
    logger.info(f"Final ratings dataset: {len(df_merged)} rows, {len(df_merged['query'].unique())} unique queries")
    
    return df_merged


def save_ratings(df_ratings: pd.DataFrame, output_file: str):
    """
    Save ratings in O19S format.
    
    Args:
        df_ratings: Ratings DataFrame
        output_file: Output CSV file path
    """
    logger.info(f"Saving ratings to {output_file}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save in O19S format: tab-separated, no header, no index
    df_ratings.to_csv(output_file, sep="\t", header=False, index=False)
    
    logger.info(f"✓ Ratings saved: {len(df_ratings)} records")
    
    # Show sample data
    logger.info("Sample ratings:")
    for i, row in df_ratings.head(3).iterrows():
        logger.info(f"  Query: '{row['query'][:50]}...' → Doc: {row['docid']} → Rating: {row['rating']}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate O19S-compatible ratings.csv from ESCI data",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory (default: dynamic_hybrid/data)')
    parser.add_argument('--esci-data', default='esci_data',
                       help='Path to ESCI data directory (default: esci_data)')
    parser.add_argument('--output', default='dynamic_hybrid/data/ratings.csv',
                       help='Output ratings file (default: dynamic_hybrid/data/ratings.csv)')
    parser.add_argument('--small-version', action='store_true',
                       help='Use small version of ESCI dataset')
    
    args = parser.parse_args()
    
    try:
        # Load O19S pre-selected queries
        df_query_set = load_o19s_queries(args.o19s_data)
        
        # Load ESCI examples
        df_examples = load_esci_examples(args.esci_data, args.small_version)
        
        # Create O19S-compatible ratings
        df_ratings = create_o19s_ratings(df_query_set, df_examples)
        
        if df_ratings.empty:
            logger.error("Failed to create ratings - no matching data found")
            sys.exit(1)
        
        # Save ratings
        save_ratings(df_ratings, args.output)
        
        logger.info("✓ O19S-compatible ratings generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Failed to generate ratings: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
