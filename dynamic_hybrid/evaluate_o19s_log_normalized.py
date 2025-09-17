#!/usr/bin/env python3
"""
Evaluation Script for O19S Log-Normalized Model

This script evaluates a trained log-normalized Ridge regression model that predicts
NDCG scores based on 18 features (including weight). It tests whether the log
normalization strategy successfully prevents model collapse.

Key Features:
- Loads trained log-normalized model and scaler
- Extracts features with proper log normalization
- Predicts optimal weights for queries
- Compares against static weight baselines
- Reports weight distribution and NDCG performance
"""

import json
import numpy as np
import pandas as pd
import pickle
import argparse
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import StandardScaler
from opensearchpy import OpenSearch
import warnings
warnings.filterwarnings('ignore')

# Import feature extractor and metrics
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.feature_extractor_o19s_exact import O19SExactFeatureExtractor
from dynamic_hybrid.utils import metrics


class O19SLogNormalizedEvaluator:
    """Evaluator for log-normalized O19S model"""
    
    def __init__(self, model_path: str, scaler_path: str, model_id: str, 
                 host: str = 'localhost', port: int = 9200):
        """
        Initialize evaluator
        
        Args:
            model_path: Path to trained model pickle file
            scaler_path: Path to scaler pickle file
            model_id: Neural model ID for OpenSearch
            host: OpenSearch host
            port: OpenSearch port
        """
        # Load model and scaler
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
        
        with open(scaler_path, 'rb') as f:
            scaler_data = pickle.load(f)
            
        # Extract scaler components and metadata
        if isinstance(scaler_data, dict):
            self.query_scaler = scaler_data.get('query_scaler')
            self.corpus_scaler = scaler_data.get('corpus_scaler')
            self.full_scaler = scaler_data.get('full_scaler')
            self.amplification_factor = scaler_data.get('amplification_factor', 2.0)
        else:
            # Legacy format - just a scaler
            self.full_scaler = scaler_data
            self.amplification_factor = 2.0
        
        # Initialize OpenSearch client
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=None,
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        self.model_id = model_id
        self.index_name = "esci-products"
        
        # Initialize feature extractor with O19S exact methodology
        self.feature_extractor = O19SExactFeatureExtractor(
            client=self.client,
            index_name=self.index_name
        )
        
    def extract_and_normalize_features(self, query: str, weight: float) -> np.ndarray:
        """
        Extract features and apply normalization (matching training)
        
        Args:
            query: Search query
            weight: Weight value to test
            
        Returns:
            Normalized feature vector
        """
        # Extract features using O19S exact methodology (returns dict)
        features_dict = self.feature_extractor.extract_features(query)
        
        # Convert to list in O19S exact order from feature_extractor.get_feature_names()
        # The order matches what O19SExactFeatureExtractor returns
        features = [
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
        
        # Add weight as first feature (18 features total: weight + 17 O19S features)
        features_with_weight = [weight] + features
        features_array = np.array(features_with_weight)
        
        # Apply same normalization as training
        # Split features
        weight_features = features_array[0:1].reshape(-1, 1)
        query_features = features_array[1:6].reshape(1, -1)
        corpus_features = features_array[6:].reshape(1, -1)
        
        # Double log transform for corpus features (if using improved trainer)
        corpus_features_log = np.log1p(np.log1p(np.abs(corpus_features)))
        
        # Apply scaling if we have component scalers
        if hasattr(self, 'query_scaler') and hasattr(self, 'corpus_scaler'):
            query_normalized = self.query_scaler.transform(query_features)
            corpus_normalized = self.corpus_scaler.transform(corpus_features_log)
            
            # Combine
            X_combined = np.hstack([
                weight_features.reshape(1, -1),
                query_normalized,
                corpus_normalized
            ])
            
            # Apply full scaling
            features_scaled = self.full_scaler.transform(X_combined)[0]
        else:
            # Legacy path - just use full scaler
            features_scaled = self.full_scaler.transform(features_array.reshape(1, -1))[0]
        
        # Apply amplification to weight feature
        features_scaled[0] *= self.amplification_factor
        
        return features_scaled
    
    def predict_optimal_weight(self, query: str, weight_range: List[float] = None) -> Tuple[float, float]:
        """
        Predict optimal weight for a query
        
        Args:
            query: Search query
            weight_range: Weights to test (default: 0.0 to 1.0 in 0.1 steps)
            
        Returns:
            Tuple of (optimal_weight, predicted_ndcg)
        """
        if weight_range is None:
            weight_range = [round(w * 0.1, 1) for w in range(11)]
        
        best_weight = 0.5
        best_score = -1.0
        
        for weight in weight_range:
            features = self.extract_and_normalize_features(query, weight)
            predicted_ndcg = self.model.predict(features.reshape(1, -1))[0]
            
            if predicted_ndcg > best_score:
                best_score = predicted_ndcg
                best_weight = weight
        
        # O19S-style rounding
        best_weight = round(best_weight * 10) / 10
        
        return best_weight, best_score
    
    def hybrid_search(self, query: str, weight: float, size: int = 10) -> List[str]:
        """
        Perform hybrid search with given weight
        
        Args:
            query: Search query
            weight: Neural search weight
            size: Number of results
            
        Returns:
            List of document IDs
        """
        lexical_weight = round(1.0 - weight, 2)
        neural_weight = round(weight, 2)
        
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
                                    "model_id": self.model_id,
                                    "k": 100
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "O19S evaluation pipeline",
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
            print(f"Hybrid search failed for query '{query}': {e}")
            return []
    
    def evaluate_queries(self, queries_df: pd.DataFrame, sample_size: int = None, 
                        evaluate_static_weights: bool = True) -> Dict:
        """
        Evaluate model on a set of queries using O19S corpus-aware methodology
        
        Args:
            queries_df: DataFrame with queries and ratings
            sample_size: Number of queries to evaluate
            evaluate_static_weights: Whether to evaluate static weight baselines (default: True)
            
        Returns:
            Evaluation results dictionary
        """
        if sample_size:
            queries_df = queries_df.head(sample_size)
        
        results = {}
        
        # Initialize results dictionary based on what we're evaluating
        if evaluate_static_weights:
            for weight in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
                results[f'static_{weight}'] = []
        
        results['dynamic'] = []
        
        weight_predictions = []
        prediction_scores = []
        
        print(f"\nEvaluating {len(queries_df)} queries...")
        
        for idx, row in queries_df.iterrows():
            query = row['query']
            relevance_dict = row['ratings']
            
            # Skip if no relevant items
            if not relevance_dict:
                continue
            
            # Create reference DataFrame for NDCG calculation (matching corpus_aware approach)
            reference_df = pd.DataFrame([
                {'docid': doc_id, 'rating': rating}
                for doc_id, rating in relevance_dict.items()
            ])
            
            # Static weight evaluations (optional)
            if evaluate_static_weights:
                for weight in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
                    search_results = self.hybrid_search(query, weight, size=100)  # Get more results
                    
                    # Create search results DataFrame
                    if search_results:
                        search_df = pd.DataFrame([
                            {'product_id': doc_id, 'position': pos, 'relevance': 1.0}
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
                        
                        # Calculate NDCG using the same method as corpus_aware
                        if not merged_df.empty:
                            ndcg = metrics.ndcg_at_10(merged_df, reference=reference_df)
                        else:
                            ndcg = 0.0
                    else:
                        ndcg = 0.0
                        
                    results[f'static_{weight}'].append(ndcg)
            
            # Dynamic weight prediction
            optimal_weight, predicted_score = self.predict_optimal_weight(query)
            weight_predictions.append(optimal_weight)
            prediction_scores.append(predicted_score)
            
            # Evaluate with predicted weight
            search_results = self.hybrid_search(query, optimal_weight, size=100)
            
            if search_results:
                search_df = pd.DataFrame([
                    {'product_id': doc_id, 'position': pos, 'relevance': 1.0}
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
                print(f"  Processed {idx + 1} queries...")
        
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


def load_test_queries(query_file: str = "dynamic_hybrid/data/query_test.csv",
                     ratings_file: str = "dynamic_hybrid/data/ratings.csv",
                     sample_size: Optional[int] = None) -> pd.DataFrame:
    """
    Load test queries from query_test.csv with ratings.
    
    Args:
        query_file: Path to query_test.csv (FIXED to use test file)
        ratings_file: Path to ratings.csv
        sample_size: Number of queries to use (None for all)
        
    Returns:
        DataFrame with queries and ratings
    """
    print(f"\n📂 Loading TEST data from {query_file}")
    
    # Load test queries
    queries_df = pd.read_csv(query_file)
    print(f"  Loaded {len(queries_df)} test queries")
    
    # Load ratings - FIXED: Handle tab-delimited file with no headers
    ratings_df = pd.read_csv(ratings_file, sep='\t', header=None, 
                           names=['query_string', 'product_id', 'esci_label', 'query_id'],
                           on_bad_lines='skip')
    print(f"  Loaded {len(ratings_df)} rating entries")
    
    # FIXED: Group ratings by query_string (not query_id) to match training
    ratings_grouped = ratings_df.groupby('query_string').apply(
        lambda x: dict(zip(x['product_id'], x['esci_label']))
    ).to_dict()
    
    # FIXED: Map ratings using query_string instead of query_id
    queries_df['ratings'] = queries_df['query_string'].map(ratings_grouped)
    queries_df['query'] = queries_df['query_string']  # Map column name
    
    # Filter queries with ratings
    queries_with_ratings = queries_df[queries_df['ratings'].notna()]
    print(f"  Test queries with ratings: {len(queries_with_ratings)}")
    
    # Apply sample size if specified
    if sample_size:
        queries_with_ratings = queries_with_ratings.head(sample_size)
        print(f"  Using sample size: {sample_size}")
    
    return queries_with_ratings


def main():
    parser = argparse.ArgumentParser(description='Evaluate O19S Log-Normalized Model')
    parser.add_argument('--model-file', type=str, default='o19s_log_normalized_model.pkl',
                       help='Path to trained model')
    parser.add_argument('--scaler-file', type=str, default='o19s_log_normalized_scaler.pkl',
                       help='Path to fitted scaler')
    parser.add_argument('--model-id', type=str, required=True,
                       help='Neural model ID in OpenSearch')
    parser.add_argument('--host', type=str, default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('--ratings-path', type=str, default='dynamic_hybrid/data/ratings.csv',
                       help='Path to ratings CSV file')
    parser.add_argument('--sample-size', type=int, default=100,
                       help='Number of queries to evaluate')
    parser.add_argument('--evaluate-static-weights', action='store_true', default=True,
                       help='Evaluate static weight baselines (default: True)')
    parser.add_argument('--skip-static-weights', action='store_true',
                       help='Skip static weight evaluations to reduce running time')
    parser.add_argument('--output', type=str, default='o19s_log_normalized_evaluation.json',
                       help='Output file for results')
    
    args = parser.parse_args()
    
    print("O19S Log-Normalized Model Evaluation")
    print("=" * 50)
    
    # FIXED: Load TEST queries from query_test.csv
    queries_df = load_test_queries(
        query_file="dynamic_hybrid/data/query_test.csv",
        ratings_file=args.ratings_path,
        sample_size=args.sample_size
    )
    
    # Initialize evaluator
    print(f"\nLoading model from {args.model_file}...")
    evaluator = O19SLogNormalizedEvaluator(
        model_path=args.model_file,
        scaler_path=args.scaler_file,
        model_id=args.model_id,
        host=args.host,
        port=args.port
    )
    
    # Determine whether to evaluate static weights
    evaluate_static = args.evaluate_static_weights and not args.skip_static_weights
    
    # Run evaluation
    print(f"\nEvaluation mode:")
    print(f"  Dynamic weight prediction: Yes")
    print(f"  Static weight baselines: {'Yes (11 weights)' if evaluate_static else 'No (skipped for speed)'}")
    
    results = evaluator.evaluate_queries(queries_df, args.sample_size, 
                                        evaluate_static_weights=evaluate_static)
    
    # Display results
    print("\n" + "=" * 50)
    print("EVALUATION RESULTS")
    print("=" * 50)
    
    print("\nNDCG@10 Scores:")
    for key, value in results['ndcg_scores'].items():
        print(f"  {key:15s}: {value:.4f}")
    
    print(f"\nModel Collapse Detection:")
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
    else:
        print(f"\nPerformance Summary:")
        print(f"  Dynamic NDCG: {dynamic_ndcg:.4f}")
        print(f"  Static weights: Not evaluated (--skip-static-weights used)")
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {args.output}")
    
    # Alert if model is still collapsed
    if results['is_collapsed']:
        print("\n⚠️  WARNING: Model is still collapsed to a single weight value!")
        print("    Log normalization may not be sufficient. Consider:")
        print("    - Increasing amplification factor")
        print("    - Different normalization strategy")
        print("    - Adjusting alpha regularization")
    else:
        print("\n✅ SUCCESS: Model produces varied weight predictions!")


if __name__ == "__main__":
    main()
