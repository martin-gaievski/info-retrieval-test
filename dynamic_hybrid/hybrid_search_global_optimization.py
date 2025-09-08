#!/usr/bin/env python3
"""
Hybrid Search Global Optimization Script

Performs exhaustive grid search over all static weight combinations to find
the globally optimal hybrid search configuration. This replicates the O19S
"Best Hybrid Search" methodology.

Usage:
    python hybrid_search_global_optimization.py [OPTIONS]
"""

import sys
import os
import json
import argparse
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple
from tqdm import tqdm
import time
from pathlib import Path

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, '..'))

# OpenSearch imports
from opensearchpy import OpenSearch
import requests

# BEIR imports
from beir import LoggingHandler

# O19S metrics import
from utils import metrics

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class HybridSearchGlobalOptimizer:
    """
    Global optimization of hybrid search using exhaustive grid search.
    Tests all static weight combinations to find the optimal configuration.
    """
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None):
        """Initialize the optimizer"""
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_compress=True,
            http_auth=None,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        
        self.host = host
        self.port = port
        self.index_name = index_name
        self.model_id = model_id
        
        logger.info(f"Initialized optimizer for {host}:{port}/{index_name}")
        
    def load_o19s_data(self, o19s_data_path: str, ratings_file: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load O19S query sets and ratings"""
        
        # Load O19S query sets
        train_file = Path(o19s_data_path) / 'query_train.csv'
        test_file = Path(o19s_data_path) / 'query_test.csv'
        
        if not train_file.exists() or not test_file.exists():
            raise FileNotFoundError(f"O19S query files not found in {o19s_data_path}")
            
        df_train = pd.read_csv(train_file)
        df_test = pd.read_csv(test_file)
        
        logger.info(f"Loaded {len(df_train)} train queries and {len(df_test)} test queries")
        
        # Load ratings
        if not Path(ratings_file).exists():
            raise FileNotFoundError(f"O19S ratings file not found: {ratings_file}")
            
        df_ratings = pd.read_csv(ratings_file, sep="\t", names=['query', 'docid', 'rating', 'idx'])
        logger.info(f"Loaded {len(df_ratings)} rating records")
        
        return df_train, df_test, df_ratings
        
    def run_global_optimization(self, 
                               o19s_data_path: str,
                               ratings_file: str,
                               sample_size: int = 1000,
                               weight_steps: int = 9,
                               seed: int = 42) -> Dict:
        """
        Run global optimization using O19S query sets and metrics.
        
        Args:
            o19s_data_path: Path to O19S data directory (query_train.csv, query_test.csv)
            ratings_file: Path to O19S ratings.csv file
            sample_size: Number of test queries to sample
            weight_steps: Number of weight steps (9 for 0.1-0.9)
            seed: Random seed for query sampling
        """
        logger.info(f"Starting O19S-compatible global optimization")
        
        # Load O19S data
        df_train, df_test, df_ratings = self.load_o19s_data(o19s_data_path, ratings_file)
        
        # Create reference dictionary for metrics
        reference = {query: df for query, df in df_ratings.groupby("query")}
        logger.info(f"Created reference for {len(reference)} queries")
        
        # Sample test queries if needed (O19S uses test set only)
        test_queries = df_test['query_string'].tolist()
        if sample_size < len(test_queries):
            np.random.seed(seed)
            test_queries = np.random.choice(test_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(test_queries)} test queries (seed: {seed})")
        else:
            logger.info(f"Using all {len(test_queries)} test queries")
        
        # Generate weight combinations (0.1 to 0.9 as O19S does)
        weight_values = [round(0.1 + i * 0.1, 1) for i in range(weight_steps)]
        logger.info(f"Testing {len(weight_values)} weight values: {weight_values}")
        
        # Test baseline first (multi_match only)
        logger.info("Testing O19S baseline (multi_match only)")
        baseline_metrics = self._evaluate_baseline_with_o19s_metrics(test_queries, reference)
        
        results = [{
            'neural_weight': 0.0,
            'lexical_weight': 1.0,
            'avg_ndcg': baseline_metrics['avg_ndcg'],
            'avg_dcg': baseline_metrics['avg_dcg'],
            'avg_precision': baseline_metrics['avg_precision'],
            'query_type': 'O19S_baseline',
            'num_queries': baseline_metrics['queries_evaluated']
        }]
        
        best_ndcg = baseline_metrics['avg_ndcg']
        best_result = results[0].copy()
        
        # Test hybrid combinations
        for neural_weight in weight_values:
            lexical_weight = round(1.0 - neural_weight, 2)
            
            logger.info(f"Testing weights: neural={neural_weight:.2f}, lexical={lexical_weight:.2f}")
            
            hybrid_metrics = self._evaluate_hybrid_with_o19s_metrics(
                test_queries, reference, lexical_weight, neural_weight
            )
            
            result = {
                'neural_weight': neural_weight,
                'lexical_weight': lexical_weight,
                'avg_ndcg': hybrid_metrics['avg_ndcg'],
                'avg_dcg': hybrid_metrics['avg_dcg'],
                'avg_precision': hybrid_metrics['avg_precision'],
                'query_type': 'hybrid',
                'num_queries': hybrid_metrics['queries_evaluated']
            }
            
            results.append(result)
            
            # Track best combination
            if hybrid_metrics['avg_ndcg'] > best_ndcg:
                best_ndcg = hybrid_metrics['avg_ndcg']
                best_result = result.copy()
            
            logger.info(f"Neural weight {neural_weight:.2f}: NDCG = {hybrid_metrics['avg_ndcg']:.4f} ({hybrid_metrics['queries_evaluated']} queries)")
        
        # Create summary
        summary = {
            'experiment_config': {
                'sample_size': len(test_queries),
                'weight_steps': weight_steps,
                'seed': seed,
                'index_name': self.index_name,
                'model_id': self.model_id,
                'total_train_queries': len(df_train),
                'total_test_queries': len(df_test),
                'ratings_available': len(reference)
            },
            'o19s_baseline': {
                'ndcg': baseline_metrics['avg_ndcg'],
                'dcg': baseline_metrics['avg_dcg'],
                'precision': baseline_metrics['avg_precision'],
                'description': 'multi_match only (no neural search)'
            },
            'best_hybrid': {
                'neural_weight': best_result['neural_weight'],
                'lexical_weight': best_result['lexical_weight'], 
                'ndcg': best_result['avg_ndcg'],
                'dcg': best_result['avg_dcg'],
                'precision': best_result['avg_precision'],
                'improvement_over_baseline': ((best_result['avg_ndcg'] - baseline_metrics['avg_ndcg']) / baseline_metrics['avg_ndcg'] * 100) if baseline_metrics['avg_ndcg'] > 0 else 0
            },
            'all_combinations': results,
            'o19s_claims_comparison': self._compare_with_o19s_claims(baseline_metrics['avg_ndcg'], best_result['avg_ndcg'])
        }
        
        return summary
    
    def _evaluate_baseline_with_o19s_metrics(self, test_queries: List[str], reference: Dict) -> Dict:
        """Evaluate baseline using O19S metrics methodology"""
        
        all_metrics = []
        queries_evaluated = 0
        
        for query_string in tqdm(test_queries, desc="Baseline evaluation"):
            if query_string not in reference:
                continue
                
            try:
                # Execute baseline search
                search_results = self._execute_baseline_search(query_string)
                
                if not search_results.empty:
                    # Merge search results with ratings for O19S metrics compatibility
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
                        # Calculate O19S metrics individually
                        query_metrics = {
                            'dcg': metrics.dcg_at_10(df_with_ratings),
                            'ndcg': metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string]),
                            'prec@10': metrics.precision_at_k(df_with_ratings),
                            'ratio_of_ratings': metrics.ratio_of_ratings(df_with_ratings)
                        }
                        all_metrics.append(query_metrics)
                        queries_evaluated += 1
                    
            except Exception as e:
                logger.warning(f"Baseline evaluation failed for query '{query_string}': {e}")
                continue
        
        # Average all metrics
        if all_metrics:
            avg_ndcg = np.mean([m['ndcg'] for m in all_metrics])
            avg_dcg = np.mean([m['dcg'] for m in all_metrics])
            avg_precision = np.mean([m['prec@10'] for m in all_metrics])
        else:
            avg_ndcg = avg_dcg = avg_precision = 0.0
        
        return {
            'avg_ndcg': avg_ndcg,
            'avg_dcg': avg_dcg, 
            'avg_precision': avg_precision,
            'queries_evaluated': queries_evaluated
        }
    
    def _evaluate_hybrid_with_o19s_metrics(self, test_queries: List[str], reference: Dict, 
                                          lexical_weight: float, neural_weight: float) -> Dict:
        """Evaluate hybrid search using O19S metrics methodology"""
        
        all_metrics = []
        queries_evaluated = 0
        
        for query_string in tqdm(test_queries, desc=f"Hybrid {neural_weight:.1f}"):
            if query_string not in reference:
                continue
                
            try:
                # Execute hybrid search
                search_results = self._execute_hybrid_search(query_string, lexical_weight, neural_weight)
                
                if not search_results.empty:
                    # Merge search results with ratings for O19S metrics compatibility
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
                        # Calculate O19S metrics individually
                        query_metrics = {
                            'dcg': metrics.dcg_at_10(df_with_ratings),
                            'ndcg': metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string]),
                            'prec@10': metrics.precision_at_k(df_with_ratings),
                            'ratio_of_ratings': metrics.ratio_of_ratings(df_with_ratings)
                        }
                        all_metrics.append(query_metrics)
                        queries_evaluated += 1
                    
            except Exception as e:
                logger.warning(f"Hybrid evaluation failed for query '{query_string}': {e}")
                continue
        
        # Average all metrics
        if all_metrics:
            avg_ndcg = np.mean([m['ndcg'] for m in all_metrics])
            avg_dcg = np.mean([m['dcg'] for m in all_metrics])
            avg_precision = np.mean([m['prec@10'] for m in all_metrics])
        else:
            avg_ndcg = avg_dcg = avg_precision = 0.0
        
        return {
            'avg_ndcg': avg_ndcg,
            'avg_dcg': avg_dcg,
            'avg_precision': avg_precision,
            'queries_evaluated': queries_evaluated
        }
    
    def _merge_results_with_reference(self, search_results: pd.DataFrame, reference_ratings: pd.DataFrame) -> pd.DataFrame:
        """Merge search results with reference ratings for O19S metrics compatibility"""
        
        if search_results.empty or reference_ratings.empty:
            return pd.DataFrame()
            
        # Merge on product_id = docid
        merged = search_results.merge(
            reference_ratings,
            left_on='product_id',
            right_on='docid',
            how='left'
        )
        
        # Fill missing ratings with 0 (no rating)
        merged['rating'] = merged['rating'].fillna(0)
        
        return merged[['position', 'rating', 'product_id', 'relevance']]
    
    def _execute_baseline_search(self, query: str) -> pd.DataFrame:
        """Execute baseline search and return DataFrame compatible with O19S metrics"""
        
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            "_source": {"excludes": ["title_embedding"]},
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
                    "query": query
                }
            },
            "size": 10
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            
            # Convert to DataFrame format expected by O19S metrics
            rows = []
            for position, hit in enumerate(result['hits']['hits']):
                rows.append({
                    'product_id': hit['_id'],
                    'position': position,
                    'relevance': hit['_score']
                })
            
            return pd.DataFrame(rows) if rows else pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Baseline search failed for query '{query}': {e}")
            return pd.DataFrame()
    
    def _execute_hybrid_search(self, query: str, lexical_weight: float, neural_weight: float) -> pd.DataFrame:
        """Execute hybrid search and return DataFrame compatible with O19S metrics"""
        
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            "_source": {"excludes": ["title_embedding"]},
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "multi_match": {
                                "query": query,
                                "type": "best_fields",
                                "operator": "and",
                                "fields": [
                                    "product_id^100",
                                    "product_bullet_point^3", 
                                    "product_color^2",
                                    "product_brand^5",
                                    "product_description",
                                    "product_title^10"
                                ]
                            }
                        },
                        {
                            "neural": {
                                "title_embedding": {
                                    "query_text": query,
                                    "model_id": self.model_id,
                                    "k": 10
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "Global optimization hybrid search",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": "min_max"},
                            "combination": {
                                "technique": "arithmetic_mean",
                                "parameters": {"weights": [lexical_weight, neural_weight]}
                            }
                        }
                    }
                ]
            },
            "size": 10
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            
            # Convert to DataFrame format expected by O19S metrics
            rows = []
            for position, hit in enumerate(result['hits']['hits']):
                rows.append({
                    'product_id': hit['_id'],
                    'position': position,
                    'relevance': hit['_score']
                })
            
            return pd.DataFrame(rows) if rows else pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Hybrid search failed for query '{query}': {e}")
            return pd.DataFrame()
    
    def _run_o19s_baseline_search(self, query: str) -> Dict[str, float]:
        """Run O19S baseline search (multi_match only)"""
        query_body = {
            "size": 100,
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
                    "query": query
                }
            }
        }
        
        try:
            response = self.client.search(index=self.index_name, body=query_body)
            
            results = {}
            for hit in response['hits']['hits']:
                doc_id = hit['_id']
                score = float(hit['_score'])
                results[doc_id] = score
            
            return results
            
        except Exception as e:
            logger.error(f"Search failed for query '{query}': {e}")
            return {}
    
    def _run_hybrid_search(self, query: str, lexical_weight: float, 
                          neural_weight: float) -> Dict[str, float]:
        """Run hybrid search with specified weights"""
        query_body = {
            "_source": False,
            "size": 100,
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "multi_match": {
                                "query": query,
                                "type": "best_fields",
                                "operator": "and",
                                "fields": [
                                    "product_id^100",
                                    "product_bullet_point^3", 
                                    "product_color^2",
                                    "product_brand^5",
                                    "product_description",
                                    "product_title^10"
                                ]
                            }
                        },
                        {
                            "neural": {
                                "title_embedding": {
                                    "query_text": query,
                                    "model_id": self.model_id,
                                    "k": 100
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "Global optimization hybrid search",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {
                                "technique": "min_max"
                            },
                            "combination": {
                                "technique": "arithmetic_mean",
                                "parameters": {
                                    "weights": [lexical_weight, neural_weight]
                                }
                            }
                        }
                    }
                ]
            }
        }
        
        try:
            response = self.client.search(index=self.index_name, body=query_body)
            
            results = {}
            for hit in response['hits']['hits']:
                doc_id = hit['_id']
                score = float(hit['_score'])
                results[doc_id] = score
            
            return results
            
        except Exception as e:
            logger.error(f"Hybrid search failed for query '{query}': {e}")
            return {}
    
    def _calculate_ndcg_at_k(self, results: Dict[str, float], 
                           relevant_docs: Dict[str, int], k: int = 10) -> float:
        """Calculate NDCG@k"""
        if not results or not relevant_docs:
            return 0.0
        
        # Sort results by score
        sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)[:k]
        
        # Calculate DCG
        dcg = 0.0
        for i, (doc_id, _) in enumerate(sorted_results):
            if doc_id in relevant_docs and relevant_docs[doc_id] > 0:
                relevance = relevant_docs[doc_id]
                dcg += relevance / np.log2(i + 2)
        
        # Calculate IDCG  
        ideal_relevances = sorted([rel for rel in relevant_docs.values() if rel > 0], reverse=True)[:k]
        if not ideal_relevances:
            return 0.0
            
        idcg = sum(rel / np.log2(i + 2) for i, rel in enumerate(ideal_relevances))
        
        return dcg / idcg if idcg > 0 else 0.0
    
    def _compare_with_o19s_claims(self, baseline_ndcg: float, best_hybrid_ndcg: float) -> Dict:
        """Compare results with O19S claims"""
        o19s_claims = {
            'baseline_ndcg': 0.26,
            'best_hybrid_ndcg': 0.27,
            'improvement_pct': 3.85  # (0.27 - 0.26) / 0.26 * 100
        }
        
        our_improvement = ((best_hybrid_ndcg - baseline_ndcg) / baseline_ndcg * 100) if baseline_ndcg > 0 else 0
        
        return {
            'o19s_claims': o19s_claims,
            'our_results': {
                'baseline_ndcg': baseline_ndcg,
                'best_hybrid_ndcg': best_hybrid_ndcg,
                'improvement_pct': our_improvement
            },
            'differences': {
                'baseline_diff_pct': ((baseline_ndcg - o19s_claims['baseline_ndcg']) / o19s_claims['baseline_ndcg'] * 100) if o19s_claims['baseline_ndcg'] > 0 else -100.0,
                'hybrid_diff_pct': ((best_hybrid_ndcg - o19s_claims['best_hybrid_ndcg']) / o19s_claims['best_hybrid_ndcg'] * 100) if o19s_claims['best_hybrid_ndcg'] > 0 else -100.0
            }
        }


def print_optimization_results(summary: Dict):
    """Print comprehensive optimization results"""
    
    print("\n" + "="*80)
    print("HYBRID SEARCH GLOBAL OPTIMIZATION RESULTS")
    print("="*80)
    
    config = summary['experiment_config']
    print(f"Sample size: {config['sample_size']} queries")
    print(f"Total train queries: {config['total_train_queries']}")
    print(f"Total test queries: {config['total_test_queries']}")
    print(f"Ratings available for: {config['ratings_available']} queries")
    print(f"Weight combinations tested: {config['weight_steps']}")
    print(f"Random seed: {config['seed']}")
    
    print("\n" + "-"*60)
    print("BASELINE PERFORMANCE")
    print("-"*60)
    baseline = summary['o19s_baseline']
    print(f"O19S Baseline (multi_match only): {baseline['ndcg']:.4f} NDCG")
    
    print("\n" + "-"*60)
    print("BEST HYBRID CONFIGURATION")
    print("-"*60)
    best = summary['best_hybrid']
    print(f"Optimal neural weight: {best['neural_weight']:.2f}")
    print(f"Optimal lexical weight: {best['lexical_weight']:.2f}")
    print(f"Best hybrid NDCG: {best['ndcg']:.4f}")
    print(f"Improvement over baseline: {best['improvement_over_baseline']:.2f}%")
    
    print("\n" + "-"*60)
    print("O19S CLAIMS COMPARISON")
    print("-"*60)
    comparison = summary['o19s_claims_comparison']
    claims = comparison['o19s_claims']
    our_results = comparison['our_results']
    diffs = comparison['differences']
    
    print("O19S Claims:")
    print(f"  Baseline: {claims['baseline_ndcg']:.3f} NDCG")
    print(f"  Best Hybrid: {claims['best_hybrid_ndcg']:.3f} NDCG")
    print(f"  Improvement: {claims['improvement_pct']:.2f}%")
    
    print("Our Results:")
    print(f"  Baseline: {our_results['baseline_ndcg']:.4f} NDCG")
    print(f"  Best Hybrid: {our_results['best_hybrid_ndcg']:.4f} NDCG")
    print(f"  Improvement: {our_results['improvement_pct']:.2f}%")
    
    print("Differences from O19S:")
    if diffs['baseline_diff_pct'] != -100.0:
        print(f"  Baseline: {diffs['baseline_diff_pct']:+.1f}% higher")
    else:
        print(f"  Baseline: Results too low for comparison")
    if diffs['hybrid_diff_pct'] != -100.0:
        print(f"  Best Hybrid: {diffs['hybrid_diff_pct']:+.1f}% higher")
    else:
        print(f"  Best Hybrid: Results too low for comparison")
    
    print("\n" + "-"*60)
    print("WEIGHT COMBINATION RESULTS")
    print("-"*60)
    print("Neural  Lexical    NDCG     Type       Valid Queries")
    print("------  -------  -------  --------   -------------")
    
    for result in summary['all_combinations']:
        weight_type = "baseline" if result['query_type'] == 'O19S_baseline' else "hybrid"
        print(f"{result['neural_weight']:6.2f}  {result['lexical_weight']:7.2f}  {result['avg_ndcg']:7.4f}  {weight_type:8}   {result['num_queries']:5d}")


def main():
    parser = argparse.ArgumentParser(
        description="O19S-compatible global optimization of hybrid search static weights",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--host', default='localhost', 
                       help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products',
                       help='OpenSearch index name')
    parser.add_argument('-m', '--model-id', required=True,
                       help='Neural search model ID')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory (query_train.csv, query_test.csv)')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to O19S ratings.csv file')
    parser.add_argument('--sample-size', type=int, default=1000,
                       help='Number of test queries to sample (default: 1000)')
    parser.add_argument('--weight-steps', type=int, default=9,
                       help='Number of weight steps to test (default: 9 for 0.1-0.9)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for query sampling')
    parser.add_argument('--output', default='o19s_global_optimization_results.json',
                       help='Output file for results')
    
    args = parser.parse_args()
    
    # Create optimizer
    optimizer = HybridSearchGlobalOptimizer(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id
    )
    
    # Run global optimization
    start_time = time.time()
    
    try:
        summary = optimizer.run_global_optimization(
            o19s_data_path=args.o19s_data,
            ratings_file=args.ratings_file,
            sample_size=args.sample_size,
            weight_steps=args.weight_steps,
            seed=args.seed
        )
        
        end_time = time.time()
        summary['execution_time_seconds'] = end_time - start_time
        
        # Print results
        print_optimization_results(summary)
        
        # Save results
        with open(args.output, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"\nResults saved to: {args.output}")
        print(f"Total execution time: {summary['execution_time_seconds']:.1f} seconds")
        
    except Exception as e:
        logger.error(f"Global optimization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
