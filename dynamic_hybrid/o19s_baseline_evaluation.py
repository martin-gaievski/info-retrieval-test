#!/usr/bin/env python3
"""
O19S-Compatible Baseline Evaluation

Complete O19S methodology implementation:
1. Generate O19S-compatible ratings.csv
2. Execute baseline multi_match queries 
3. Merge results with ratings
4. Calculate O19S metrics (DCG, NDCG, Precision@10, Ratio of Ratings)

Author: Dynamic Hybrid Search Team
Version: 1.0.0
"""

import os
import sys
import pandas as pd
import logging
import requests
import json
import numpy as np
from pathlib import Path
from tqdm import tqdm

# Add current directory to path for utils import
sys.path.append(os.path.dirname(__file__))
from utils import metrics

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def load_o19s_queries(data_dir: str) -> tuple:
    """
    Load O19S pre-selected query sets.
    
    Returns:
        Tuple of (combined_query_set, test_queries)
    """
    train_file = Path(data_dir) / 'query_train.csv'
    test_file = Path(data_dir) / 'query_test.csv'
    
    if not train_file.exists() or not test_file.exists():
        raise FileNotFoundError(f"O19S query files not found in {data_dir}")
    
    df_train = pd.read_csv(train_file)
    df_test = pd.read_csv(test_file)
    
    logger.info(f"Loaded {len(df_train)} train queries and {len(df_test)} test queries")
    
    # Combine train and test sets
    df_query_set = pd.concat([df_train, df_test], ignore_index=True)
    
    return df_query_set, df_test


def create_o19s_ratings(df_query_set: pd.DataFrame, esci_data_dir: str, small_version: bool = True) -> tuple:
    """Create O19S-compatible ratings"""
    
    # Load ESCI examples
    if small_version:
        examples_file = Path(esci_data_dir) / 'shopping_queries_dataset_examples_us_small.parquet'
    else:
        examples_file = Path(esci_data_dir) / 'shopping_queries_dataset_examples.parquet'
    
    df_examples = pd.read_parquet(examples_file)
    logger.info(f"Loaded {len(df_examples)} ESCI example rows")
    
    # O19S rating conversion
    label_num = {"E": 0, "S": 1, "C": 2, "I": 3}
    label_score = [3, 2, 1, 0]
    
    def label_to_score(label):
        return label_score[label_num[label]]
    
    # Filter for O19S queries only
    o19s_query_strings = set(df_query_set["query_string"].values)
    df_judge = df_examples[df_examples["query"].isin(o19s_query_strings)].copy()
    
    logger.info(f"Found {len(df_judge)} ESCI examples matching O19S queries")
    
    if len(df_judge) == 0:
        raise ValueError("No matching queries found between O19S sets and ESCI examples")
    
    # Apply rating conversion
    df_judge["judgment"] = df_judge.esci_label.apply(label_to_score)
    df_judge["document"] = df_judge.product_id
    df_judge = df_judge[["query", "document", "judgment"]].reset_index(drop=True)
    
    # Create query index mapping (as O19S does)
    df_queries = df_judge.groupby(by='query', as_index=False).agg({'judgment': ['count']})
    df_query_idx = pd.DataFrame(df_queries['query']).reset_index().rename(columns={'index': 'idx'})
    
    # Merge with query indices
    df_merged = pd.merge(df_judge, df_query_idx, on='query', how='left')
    df_merged.columns = ['query', 'docid', 'rating', 'idx']
    
    logger.info(f"Generated ratings: {len(df_merged)} records, {len(df_merged['query'].unique())} unique queries")
    
    return df_merged, df_query_idx


def execute_baseline_queries(df_test: pd.DataFrame, df_query_idx: pd.DataFrame, 
                           host: str, port: int, index_name: str) -> pd.DataFrame:
    """
    Execute O19S baseline queries (multi_match) against OpenSearch.
    """
    url = f"http://{host}:{port}/{index_name}/_search"
    headers = {'Content-Type': 'application/json'}
    
    logger.info(f"Executing baseline queries against {url}")
    
    df_relevance = pd.DataFrame()
    
    # Filter to test queries only
    test_query_strings = set(df_test['query_string'].values)
    test_queries = df_query_idx[df_query_idx['query'].isin(test_query_strings)]
    
    logger.info(f"Executing {len(test_queries)} test queries...")
    
    for _, query_row in tqdm(test_queries.iterrows(), total=len(test_queries), desc="Baseline queries"):
        query_id = str(query_row['idx'])
        query_string = query_row['query']
        
        # O19S baseline payload (multi_match with field weights)
        payload = {
            "_source": {
                "excludes": ["title_embedding"]
            },
            "query": {
                "multi_match": {
                    "type": "best_fields",
                    "fields": [
                        "product_id^100",
                        "product_bullet_point^3",
                        "product_color^2", 
                        "product_brand^5",
                        "product_description",
                        "product_title^10"
                    ],
                    "operator": "and",
                    "query": query_string
                }
            },
            "size": 10  # O19S gets top 10 results
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            
            position = 0
            num_results = result['hits']['total']['value']
            
            for hit in result['hits']['hits']:
                row = {
                    'query_id': query_id,
                    'query_string': query_string,
                    'product_id': hit["_id"],
                    'position': position,
                    'num_results': num_results,
                    'relevance': hit["_score"],
                    'run': 'baseline'
                }
                
                df_relevance = pd.concat([df_relevance, pd.DataFrame([row])], ignore_index=True)
                position += 1
                
        except Exception as e:
            logger.warning(f"Query failed: '{query_string[:50]}...' - {e}")
            continue
    
    logger.info(f"Collected {len(df_relevance)} search results")
    return df_relevance


def merge_results_with_ratings(df_relevance: pd.DataFrame, df_ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Merge search results with ratings following O19S methodology.
    """
    logger.info("Merging search results with ratings...")
    
    # Ensure compatible data types
    df_relevance['query_id'] = df_relevance['query_id'].astype(str)
    df_ratings['idx'] = df_ratings['idx'].astype(str)
    
    # Remove duplicates from ratings (as O19S does)
    df_unique_ratings = df_ratings.drop_duplicates(subset=['docid', 'idx'])
    logger.info(f"Unique ratings after deduplication: {len(df_unique_ratings)}")
    
    # Merge results with ratings
    df_merged = df_relevance.merge(
        df_unique_ratings, 
        left_on=['query_id', 'product_id'], 
        right_on=['idx', 'docid'], 
        how='left'
    )
    
    # Clean up columns
    df_merged = df_merged.drop(columns=['query', 'docid', 'idx'])
    
    # Count missing ratings
    nan_count = df_merged['rating'].isna().sum()
    total_rows = len(df_merged)
    
    logger.info(f"Merged results: {total_rows} rows, {nan_count} without ratings ({100*nan_count/total_rows:.1f}%)")
    
    return df_merged


def calculate_o19s_metrics(df_merged: pd.DataFrame, df_ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate metrics following O19S methodology exactly.
    """
    logger.info("Calculating metrics using O19S methodology...")
    
    # O19S metrics configuration
    metric_functions = [
        ("dcg", metrics.dcg_at_10, None),
        ("ndcg", metrics.ndcg_at_10, None),
        ("prec@10", metrics.precision_at_k, None),
        ("ratio_of_ratings", metrics.ratio_of_ratings, None)
    ]
    
    # Create reference dictionary (as O19S does)
    reference = {query: df for query, df in df_ratings.groupby("query")}
    logger.info(f"Created reference for {len(reference)} queries")
    
    # Calculate metrics for each query-run combination
    df_metrics = []
    
    for m_name, m_function, ref_search in metric_functions:
        logger.info(f"Calculating {m_name}...")
        
        for (query_string, run), df_gr in df_merged.groupby(["query_string", "run"]):
            # Get reference for this query
            if query_string in reference:
                try:
                    metric_value = m_function(df_gr, reference=reference[query_string])
                    df_metrics.append(pd.DataFrame({
                        "query": [query_string],
                        "pipeline": [run],
                        "metric": [m_name],
                        "value": [metric_value],
                    }))
                except Exception as e:
                    logger.warning(f"Metric calculation failed for {m_name}, query '{query_string}': {e}")
                    continue
    
    if not df_metrics:
        logger.error("No metrics calculated")
        return pd.DataFrame()
    
    df_metrics_combined = pd.concat(df_metrics, ignore_index=True)
    logger.info(f"Calculated {len(df_metrics_combined)} metric records")
    
    return df_metrics_combined


def print_o19s_summary(df_metrics: pd.DataFrame):
    """Print O19S-style metrics summary"""
    
    print("\n" + "="*70)
    print("O19S BASELINE EVALUATION RESULTS")
    print("="*70)
    
    if df_metrics.empty:
        print("No metrics calculated")
        return
    
    # Calculate averages for each metric
    avg_dcg = df_metrics[df_metrics['metric'] == 'dcg']['value'].mean()
    avg_ndcg = df_metrics[df_metrics['metric'] == 'ndcg']['value'].mean()
    avg_precision = df_metrics[df_metrics['metric'] == 'prec@10']['value'].mean()
    avg_ratio = df_metrics[df_metrics['metric'] == 'ratio_of_ratings']['value'].mean()
    
    print(f"Average DCG: {avg_dcg:.2f}")
    print(f"Average NDCG: {avg_ndcg:.2f}")
    print(f"Average Precision@10: {avg_precision:.2f}")
    print(f"Average Ratio of Ratings: {avg_ratio:.2f}")
    
    queries_count = len(df_metrics[df_metrics['metric'] == 'ndcg']['query'].unique())
    print(f"Queries evaluated: {queries_count}")
    
    print("="*70)


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="O19S-compatible baseline evaluation with ratings generation",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--host', default='localhost',
                       help='OpenSearch host (default: localhost)')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port (default: 9200)')
    parser.add_argument('--index', default='esci-products',
                       help='OpenSearch index name (default: esci-products)')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--esci-data', default='esci_data',
                       help='Path to ESCI data directory')
    parser.add_argument('--output-ratings', default='dynamic_hybrid/data/ratings.csv',
                       help='Output ratings file')
    parser.add_argument('--output-results', default='dynamic_hybrid/data/baseline_results.json',
                       help='Output results file')
    parser.add_argument('--small-version', action='store_true',
                       help='Use small version of ESCI dataset')
    parser.add_argument('--skip-ratings', action='store_true',
                       help='Skip ratings generation (use existing ratings.csv)')
    
    args = parser.parse_args()
    
    try:
        # Step 1: Load O19S queries
        df_query_set, df_test = load_o19s_queries(args.o19s_data)
        
        # Step 2: Generate or load ratings
        if not args.skip_ratings:
            logger.info("Generating O19S-compatible ratings...")
            df_ratings, df_query_idx = create_o19s_ratings(df_query_set, args.esci_data, args.small_version)
            
            # Save ratings
            os.makedirs(os.path.dirname(args.output_ratings), exist_ok=True)
            df_ratings.to_csv(args.output_ratings, sep="\t", header=False, index=False)
            logger.info(f"✓ Ratings saved to {args.output_ratings}")
        else:
            logger.info(f"Loading existing ratings from {args.output_ratings}")
            df_ratings = pd.read_csv(args.output_ratings, sep="\t", names=['query', 'docid', 'rating', 'idx'])
            
            # Recreate query index mapping
            df_query_idx = df_ratings[['query', 'idx']].drop_duplicates().reset_index(drop=True)
            logger.info(f"Loaded {len(df_ratings)} ratings, {len(df_query_idx)} unique queries")
        
        # Step 3: Execute baseline queries
        logger.info("Executing O19S baseline queries...")
        df_relevance = execute_baseline_queries(df_test, df_query_idx, args.host, args.port, args.index)
        
        if df_relevance.empty:
            logger.error("No search results obtained - check OpenSearch connection and index")
            sys.exit(1)
        
        # Step 4: Merge results with ratings
        df_merged = merge_results_with_ratings(df_relevance, df_ratings)
        
        # Step 5: Calculate metrics using O19S methodology
        df_metrics = calculate_o19s_metrics(df_merged, df_ratings)
        
        if df_metrics.empty:
            logger.error("No metrics calculated")
            sys.exit(1)
        
        # Step 6: Extract summary metrics
        avg_dcg = df_metrics[df_metrics['metric'] == 'dcg']['value'].mean()
        avg_ndcg = df_metrics[df_metrics['metric'] == 'ndcg']['value'].mean()
        avg_precision = df_metrics[df_metrics['metric'] == 'prec@10']['value'].mean()
        avg_ratio = df_metrics[df_metrics['metric'] == 'ratio_of_ratings']['value'].mean()
        
        queries_evaluated = len(df_metrics[df_metrics['metric'] == 'ndcg']['query'].unique())
        
        results = {
            'baseline_dcg': avg_dcg,
            'baseline_ndcg': avg_ndcg,
            'baseline_precision_at_10': avg_precision,
            'ratio_of_ratings': avg_ratio,
            'queries_evaluated': queries_evaluated,
            'total_search_results': len(df_relevance),
            'total_ratings': len(df_ratings),
            'ratings_coverage': (len(df_merged) - df_merged['rating'].isna().sum()) / len(df_merged),
            'config': {
                'host': args.host,
                'port': args.port,
                'index': args.index,
                'small_version': args.small_version
            }
        }
        
        # Save detailed results
        with open(args.output_results, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Save detailed metrics
        metrics_file = args.output_results.replace('.json', '_metrics.csv')
        df_metrics.to_csv(metrics_file, index=False)
        
        logger.info(f"✓ Results saved to {args.output_results}")
        logger.info(f"✓ Detailed metrics saved to {metrics_file}")
        
        # Print O19S-style summary
        print_o19s_summary(df_metrics)
        
        return results
        
    except Exception as e:
        logger.error(f"Evaluation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
