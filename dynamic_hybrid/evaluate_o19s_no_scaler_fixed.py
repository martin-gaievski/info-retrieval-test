#!/usr/bin/env python3
"""
Fixed evaluation script for O19S models trained without scalers (exact O19S methodology).

This script evaluates models trained with plain LinearRegression without any feature scaling,
normalization, or amplification - matching the exact O19S approach.
"""

import os
import sys
import json
import pickle
import logging
import argparse
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from opensearchpy import OpenSearch
from dynamic_hybrid.feature_extractor_o19s_exact import O19SExactFeatureExtractor
from dynamic_hybrid.utils import metrics

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class EvaluationResult:
    """Store evaluation results for a specific weight configuration."""
    weight: float
    ndcg: float
    num_queries: int


class O19SNoScalerEvaluator:
    """Evaluator for O19S models trained without scalers."""
    
    def __init__(
        self,
        model_path: str,
        opensearch_host: str = "localhost",
        opensearch_port: int = 9200,
        index_name: str = "esci-products",
        neural_model_id: str = "X9F6UJkBstwCd_QonT1s",
        k: int = 10
    ):
        """Initialize the evaluator.
        
        Args:
            model_path: Path to the trained model pickle file
            opensearch_host: OpenSearch host
            opensearch_port: OpenSearch port
            index_name: Index name for searching
            neural_model_id: Neural model ID for semantic search
            k: Number of top results to consider for NDCG
        """
        self.model_path = model_path
        self.opensearch_host = opensearch_host
        self.opensearch_port = opensearch_port
        self.index_name = index_name
        self.neural_model_id = neural_model_id
        self.k = k
        
        # Load the model
        logger.info(f"Loading model from {model_path}")
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
        
        # Initialize OpenSearch client
        self.client = OpenSearch(
            hosts=[{'host': opensearch_host, 'port': opensearch_port}],
            http_auth=None,
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        # Initialize feature extractor
        self.feature_extractor = O19SExactFeatureExtractor(self.client, index_name)
        
        logger.info(f"Initialized evaluator with neural model: {neural_model_id}")
    
    def load_test_queries(self, query_file: str = "dynamic_hybrid/data/query_test.csv",
                         ratings_file: str = "dynamic_hybrid/data/ratings.csv",
                         sample_size: Optional[int] = None) -> pd.DataFrame:
        """
        Load test queries with ratings.
        
        Args:
            query_file: Path to query_test.csv
            ratings_file: Path to ratings.csv
            sample_size: Number of queries to use (None for all)
            
        Returns:
            DataFrame with queries and ratings
        """
        logger.info(f"Loading test data from {query_file}")
        
        # Load test queries
        queries_df = pd.read_csv(query_file)
        logger.info(f"Loaded {len(queries_df)} test queries")
        
        # Load ratings - tab-delimited file with specific columns
        ratings_df = pd.read_csv(ratings_file, sep='\t', header=None, 
                               names=['query_string', 'product_id', 'esci_label', 'query_id'],
                               on_bad_lines='skip')
        logger.info(f"Loaded {len(ratings_df)} rating entries")
        
        # Group ratings by query_string
        ratings_grouped = ratings_df.groupby('query_string').apply(
            lambda x: dict(zip(x['product_id'], x['esci_label']))
        ).to_dict()
        
        # Map ratings to queries
        queries_df['ratings'] = queries_df['query_string'].map(ratings_grouped)
        queries_df['query'] = queries_df['query_string']  # Add query column
        
        # Filter queries with ratings
        queries_with_ratings = queries_df[queries_df['ratings'].notna()]
        logger.info(f"Test queries with ratings: {len(queries_with_ratings)}")
        
        # Apply sample size if specified
        if sample_size:
            queries_with_ratings = queries_with_ratings.head(sample_size)
            logger.info(f"Using sample size: {sample_size}")
        
        return queries_with_ratings
    
    def _search_hybrid(self, query: str, weight: float, size: int = 100) -> List[str]:
        """Perform hybrid search with given weight using proper query structure.
        
        Args:
            query: Query string
            weight: Weight for neural search (0-1)
            size: Number of results to retrieve
            
        Returns:
            List of document IDs in ranked order
        """
        lexical_weight = round(1.0 - weight, 2)
        neural_weight = round(weight, 2)
        
        # Construct proper hybrid query with search pipeline
        body = {
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
                                    "model_id": self.neural_model_id,
                                    "k": 100
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "O19S no-scaler evaluation pipeline",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": "l2"},
                            "combination": {
                                "technique": "arithmetic_mean",
                                "parameters": {"weights": [lexical_weight, neural_weight]}
                            }
                        }
                    }
                ]
            },
            "size": size
        }
        
        try:
            response = self.client.search(index=self.index_name, body=body)
            return [hit['_id'] for hit in response['hits']['hits']]
        except Exception as e:
            logger.error(f"Search failed for query '{query}': {e}")
            return []
    
    def predict_best_weight(self, query: str) -> Tuple[float, float]:
        """Predict the best weight for a query using the trained model.
        
        Args:
            query: Query string
            
        Returns:
            Tuple of (predicted_weight, predicted_ndcg)
        """
        # Test weights to evaluate
        test_weights = np.arange(0.0, 1.1, 0.1)
        best_weight = 0.5
        best_score = -float('inf')
        
        for weight in test_weights:
            try:
                # Extract features using O19S exact methodology
                features_dict = self.feature_extractor.extract_features(query)
                
                if features_dict is not None:
                    # Convert to list in O19S exact order with weight as first feature
                    features = [
                        weight,  # Weight as first feature
                        features_dict.get('query_length', 0),
                        features_dict.get('has_special_chars', 0),
                        features_dict.get('has_punctuation', 0),
                        features_dict.get('capitalization_ratio', 0),
                        features_dict.get('stopword_ratio', 0),
                        features_dict.get('max_document_frequency', 0),
                        features_dict.get('min_document_frequency', 0),
                        features_dict.get('total_document_frequency', 0),
                        features_dict.get('average_document_frequency', 0),
                        features_dict.get('variance_document_frequency', 0),
                        features_dict.get('std_dev_document_frequency', 0),
                        features_dict.get('max_inverse_document_frequency', 0),
                        features_dict.get('min_inverse_document_frequency', 0),
                        features_dict.get('total_inverse_document_frequency', 0),
                        features_dict.get('average_inverse_document_frequency', 0),
                        features_dict.get('variance_inverse_document_frequency', 0),
                        features_dict.get('std_dev_inverse_document_frequency', 0)
                    ]
                    
                    # Reshape for prediction
                    X = np.array(features).reshape(1, -1)
                    
                    # Predict NDCG score
                    predicted_ndcg = self.model.predict(X)[0]
                    
                    if predicted_ndcg > best_score:
                        best_score = predicted_ndcg
                        best_weight = weight
                        
            except Exception as e:
                logger.debug(f"Feature extraction failed for weight {weight}: {e}")
                continue
        
        # O19S-style rounding
        return round(best_weight, 1), best_score
    
    def evaluate_queries(self, queries_df: pd.DataFrame, 
                        evaluate_static_weights: bool = True) -> Dict:
        """Evaluate model on a set of queries.
        
        Args:
            queries_df: DataFrame with queries and ratings
            evaluate_static_weights: Whether to evaluate static weight baselines
            
        Returns:
            Evaluation results dictionary
        """
        results = {}
        
        # Initialize results dictionary
        if evaluate_static_weights:
            for weight in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
                results[f'static_{weight}'] = []
        
        results['dynamic'] = []
        
        weight_predictions = []
        prediction_scores = []
        
        logger.info(f"Evaluating {len(queries_df)} queries...")
        
        for idx, row in queries_df.iterrows():
            query = row['query']
            relevance_dict = row['ratings']
            
            # Skip if no relevant items
            if not relevance_dict:
                continue
            
            # Create reference DataFrame for NDCG calculation
            reference_df = pd.DataFrame([
                {'docid': doc_id, 'rating': rating}
                for doc_id, rating in relevance_dict.items()
            ])
            
            # Static weight evaluations (optional)
            if evaluate_static_weights:
                for weight in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
                    search_results = self._search_hybrid(query, weight, size=100)
                    
                    # Create search results DataFrame
                    if search_results:
                        search_df = pd.DataFrame([
                            {'product_id': doc_id, 'position': pos + 1, 'relevance': 1.0}
                            for pos, doc_id in enumerate(search_results[:10])
                        ])
                        
                        # Merge with ratings
                        merged_df = search_df.merge(
                            reference_df,
                            left_on='product_id',
                            right_on='docid',
                            how='left'
                        )
                        merged_df['rating'] = merged_df['rating'].fillna(0)
                        
                        # Calculate NDCG
                        if not merged_df.empty:
                            ndcg = metrics.ndcg_at_10(merged_df, reference=reference_df)
                        else:
                            ndcg = 0.0
                    else:
                        ndcg = 0.0
                        
                    results[f'static_{weight}'].append(ndcg)
            
            # Dynamic weight prediction
            optimal_weight, predicted_score = self.predict_best_weight(query)
            weight_predictions.append(optimal_weight)
            prediction_scores.append(predicted_score)
            
            # Evaluate with predicted weight
            search_results = self._search_hybrid(query, optimal_weight, size=100)
            
            if search_results:
                search_df = pd.DataFrame([
                    {'product_id': doc_id, 'position': pos + 1, 'relevance': 1.0}
                    for pos, doc_id in enumerate(search_results[:10])
                ])
                
                # Merge with ratings
                merged_df = search_df.merge(
                    reference_df,
                    left_on='product_id',
                    right_on='docid',
                    how='left'
                )
                merged_df['rating'] = merged_df['rating'].fillna(0)
                
                # Calculate NDCG 
                if not merged_df.empty:
                    ndcg = metrics.ndcg_at_10(merged_df, reference=reference_df)
                else:
                    ndcg = 0.0
            else:
                ndcg = 0.0
                
            results['dynamic'].append(ndcg)
            
            if (idx + 1) % 10 == 0:
                logger.info(f"  Processed {idx + 1} queries...")
        
        # Compute averages
        avg_results = {
            key: np.mean(values) if values else 0.0
            for key, values in results.items()
        }
        
        # Analyze weight distribution
        weight_distribution = pd.Series(weight_predictions).value_counts(normalize=True).to_dict()
        
        # Check for model collapse
        unique_weights = len(set(weight_predictions))
        is_collapsed = unique_weights == 1
        
        return {
            'ndcg_scores': avg_results,
            'weight_distribution': weight_distribution,
            'mean_weight': np.mean(weight_predictions),
            'std_weight': np.std(weight_predictions),
            'unique_weights': unique_weights,
            'is_collapsed': is_collapsed,
            'mean_predicted_score': np.mean(prediction_scores),
            'num_queries': len(queries_df),
            'static_weights_evaluated': evaluate_static_weights
        }


def main():
    """Main evaluation function."""
    parser = argparse.ArgumentParser(description='Evaluate O19S no-scaler model')
    parser.add_argument(
        '--model-file',
        type=str,
        default='o19s_no_scaler_model.pkl',
        help='Path to trained model file'
    )
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='OpenSearch host'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=9200,
        help='OpenSearch port'
    )
    parser.add_argument(
        '--index',
        type=str,
        default='esci-products',
        help='Index name'
    )
    parser.add_argument(
        '--model-id',
        type=str,
        required=True,
        help='Neural model ID'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=100,
        help='Number of queries to evaluate'
    )
    parser.add_argument(
        '--skip-static-weights',
        action='store_true',
        help='Skip static weight evaluation for faster results'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='o19s_no_scaler_evaluation.json',
        help='Output file for results'
    )
    
    args = parser.parse_args()
    
    # Check if model file exists
    if not os.path.exists(args.model_file):
        logger.error(f"Model file not found: {args.model_file}")
        sys.exit(1)
    
    # Initialize evaluator
    evaluator = O19SNoScalerEvaluator(
        model_path=args.model_file,
        opensearch_host=args.host,
        opensearch_port=args.port,
        index_name=args.index,
        neural_model_id=args.model_id
    )
    
    # Load test queries
    queries_df = evaluator.load_test_queries(
        query_file="dynamic_hybrid/data/query_test.csv",
        ratings_file="dynamic_hybrid/data/ratings.csv",
        sample_size=args.sample_size
    )
    
    # Run evaluation
    evaluate_static = not args.skip_static_weights
    results = evaluator.evaluate_queries(queries_df, evaluate_static_weights=evaluate_static)
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Results saved to {args.output}")
    
    # Print summary
    print("\n" + "="*60)
    print("EVALUATION SUMMARY")
    print("="*60)
    print(f"Model: {args.model_file}")
    print(f"Test Queries: {results['num_queries']}")
    
    print("\nNDCG@10 Scores:")
    for key, value in results['ndcg_scores'].items():
        print(f"  {key:15s}: {value:.4f}")
    
    print(f"\nModel Analysis:")
    print(f"  Is Collapsed: {results['is_collapsed']}")
    print(f"  Unique Weights: {results['unique_weights']}")
    print(f"  Mean Weight: {results['mean_weight']:.3f}")
    print(f"  Std Weight: {results['std_weight']:.3f}")
    
    print(f"\nWeight Distribution:")
    for weight, freq in sorted(results['weight_distribution'].items()):
        print(f"  {weight}: {freq*100:.1f}%")
    
    # Performance comparison
    dynamic_ndcg = results['ndcg_scores']['dynamic']
    
    if results.get('static_weights_evaluated', True):
        static_scores = [v for k, v in results['ndcg_scores'].items() if k.startswith('static')]
        if static_scores:
            best_static = max(static_scores)
            best_static_weight = [k for k, v in results['ndcg_scores'].items() 
                                 if k.startswith('static') and v == best_static][0]
            improvement = ((dynamic_ndcg - best_static) / best_static) * 100 if best_static > 0 else 0
            
            print(f"\nPerformance Summary:")
            print(f"  Dynamic NDCG: {dynamic_ndcg:.4f}")
            print(f"  Best Static ({best_static_weight}): {best_static:.4f}")
            print(f"  Improvement: {improvement:+.1f}%")
    else:
        print(f"\nPerformance Summary:")
        print(f"  Dynamic NDCG: {dynamic_ndcg:.4f}")
        print(f"  Static weights: Not evaluated (--skip-static-weights used)")
    
    # Alert if model is collapsed
    if results['is_collapsed']:
        print("\n⚠️  WARNING: Model is collapsed to a single weight value!")
        print("    The model failed to learn meaningful patterns.")
        print("    Consider:")
        print("    - Using Ridge regression with proper alpha")
        print("    - Adding feature scaling/normalization")
        print("    - Increasing training data size")
    
    print("="*60)


if __name__ == "__main__":
    main()