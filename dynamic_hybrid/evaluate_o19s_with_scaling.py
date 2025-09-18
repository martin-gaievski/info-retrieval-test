#!/usr/bin/env python3
"""
O19S Evaluation with MinMaxScaler Feature Normalization

This script evaluates models trained with MinMaxScaler:
- Loads both model and scaler from pickle files
- Applies same feature scaling during evaluation
- Compares dynamic vs static weight baselines
- Validates that scaling improves performance
"""

import json
import numpy as np
import pandas as pd
import pickle
import argparse
from typing import Dict, List, Tuple
from opensearchpy import OpenSearch
import warnings
warnings.filterwarnings('ignore')

# Import feature extractor and metrics
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.feature_extractor_o19s_exact import O19SExactFeatureExtractor
from dynamic_hybrid.utils import metrics


class O19SScaledEvaluator:
    """O19S evaluation with MinMaxScaler normalization"""
    
    def __init__(self, model, scaler, model_id: str, host: str = 'localhost', port: int = 9200):
        """
        Initialize evaluator with model and scaler
        
        Args:
            model: Trained LinearRegression model
            scaler: Fitted MinMaxScaler
            model_id: Neural model ID for OpenSearch
            host: OpenSearch host
            port: OpenSearch port
        """
        self.model = model
        self.scaler = scaler
        
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
        
        # Initialize feature extractor
        self.feature_extractor = O19SExactFeatureExtractor(
            client=self.client,
            index_name=self.index_name
        )
        
    def extract_features(self, query: str) -> List[float]:
        """
        Extract features using O19S exact methodology
        
        Args:
            query: Search query
            
        Returns:
            List of feature values (no weight included)
        """
        features_dict = self.feature_extractor.extract_features(query)
        
        # O19S exact feature order (18 features - weight will be added separately)
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
        
        return features
    
    def predict_optimal_weight(self, query: str) -> float:
        """
        Predict optimal weight for a query using scaled features
        
        Args:
            query: Search query
            
        Returns:
            Predicted optimal weight
        """
        # Extract query features
        query_features = self.extract_features(query)
        
        # Test different weights (using finer granularity for evaluation)
        test_weights = np.arange(0.0, 1.01, 0.05)
        best_weight = 0.5
        best_score = -1
        
        feature_vectors = []
        for weight in test_weights:
            # Create feature vector with weight
            features = [weight] + query_features
            feature_vectors.append(features)
        
        # Scale all feature vectors at once
        feature_vectors = np.array(feature_vectors)
        feature_vectors_scaled = self.scaler.transform(feature_vectors)
        
        # Predict NDCG scores for all weights
        predicted_scores = self.model.predict(feature_vectors_scaled)
        
        # Find weight with highest predicted NDCG
        best_idx = np.argmax(predicted_scores)
        best_weight = test_weights[best_idx]
        best_score = predicted_scores[best_idx]
        
        # Round to 0.05 granularity
        best_weight = round(best_weight * 20) / 20
        
        return best_weight
    
    def hybrid_search(self, query: str, weight: float, size: int = 100) -> List[str]:
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
                "description": "O19S scaled evaluation pipeline",
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
            print(f"Search failed for query '{query}': {e}")
            return []
    
    def evaluate_queries(self, queries_df: pd.DataFrame, 
                        static_weights: List[float] = None) -> Dict:
        """
        Evaluate queries using dynamic weight prediction and static baselines
        
        Args:
            queries_df: DataFrame with queries and ratings
            static_weights: List of static weights to compare against
            
        Returns:
            Dictionary with evaluation results
        """
        if static_weights is None:
            static_weights = [0.1, 0.3, 0.5, 0.7]
        
        # Initialize results
        results = {
            'dynamic': {'ndcg_scores': [], 'weights': []},
            'static': {f'{w:.1f}': {'ndcg_scores': []} for w in static_weights},
            'query_details': []
        }
        
        print(f"\n📊 Evaluating {len(queries_df)} queries...")
        print(f"Static weight baselines: {static_weights}")
        
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
            
            # Dynamic weight prediction
            predicted_weight = self.predict_optimal_weight(query)
            
            # Get dynamic results
            dynamic_results = self.hybrid_search(query, predicted_weight, size=100)
            
            if dynamic_results:
                search_df = pd.DataFrame([
                    {'product_id': doc_id, 'position': pos, 'relevance': 1.0}
                    for pos, doc_id in enumerate(dynamic_results[:10])
                ])
                
                merged_df = search_df.merge(
                    reference_df,
                    left_on='product_id',
                    right_on='docid',
                    how='left'
                )
                merged_df['rating'] = merged_df['rating'].fillna(0)
                
                if not merged_df.empty:
                    dynamic_ndcg = metrics.ndcg_at_10(merged_df, reference=reference_df)
                else:
                    dynamic_ndcg = 0.0
            else:
                dynamic_ndcg = 0.0
            
            results['dynamic']['ndcg_scores'].append(dynamic_ndcg)
            results['dynamic']['weights'].append(predicted_weight)
            
            # Static weight baselines
            static_ndcgs = {}
            for static_weight in static_weights:
                static_results = self.hybrid_search(query, static_weight, size=100)
                
                if static_results:
                    search_df = pd.DataFrame([
                        {'product_id': doc_id, 'position': pos, 'relevance': 1.0}
                        for pos, doc_id in enumerate(static_results[:10])
                    ])
                    
                    merged_df = search_df.merge(
                        reference_df,
                        left_on='product_id',
                        right_on='docid',
                        how='left'
                    )
                    merged_df['rating'] = merged_df['rating'].fillna(0)
                    
                    if not merged_df.empty:
                        static_ndcg = metrics.ndcg_at_10(merged_df, reference=reference_df)
                    else:
                        static_ndcg = 0.0
                else:
                    static_ndcg = 0.0
                
                results['static'][f'{static_weight:.1f}']['ndcg_scores'].append(static_ndcg)
                static_ndcgs[static_weight] = static_ndcg
            
            # Store query details
            results['query_details'].append({
                'query': query[:50] + '...' if len(query) > 50 else query,
                'predicted_weight': predicted_weight,
                'dynamic_ndcg': dynamic_ndcg,
                **{f'static_{w:.1f}_ndcg': static_ndcgs[w] for w in static_weights}
            })
            
            if (idx + 1) % 10 == 0:
                print(f"  Processed {idx + 1} queries...")
        
        return results


def load_test_queries(query_file: str = "dynamic_hybrid/data/query_test.csv",
                      ratings_file: str = "dynamic_hybrid/data/ratings.csv",
                      sample_size: int = None) -> pd.DataFrame:
    """
    Load test queries from query_test.csv with ratings
    
    Args:
        query_file: Path to query_test.csv
        ratings_file: Path to ratings.csv
        sample_size: Number of queries to use
        
    Returns:
        DataFrame with queries and ratings
    """
    print(f"\n📂 Loading TEST data from {query_file}")
    
    # Load test queries
    queries_df = pd.read_csv(query_file)
    print(f"  Loaded {len(queries_df)} test queries")
    
    # Load ratings - Handle tab-delimited file with no headers
    ratings_df = pd.read_csv(ratings_file, sep='\t', header=None, 
                           names=['query_string', 'product_id', 'esci_label', 'query_id'],
                           on_bad_lines='skip')
    print(f"  Loaded {len(ratings_df)} rating entries")
    
    # Group ratings by query_string
    ratings_grouped = ratings_df.groupby('query_string').apply(
        lambda x: dict(zip(x['product_id'], x['esci_label']))
    ).to_dict()
    
    # Map ratings using query_string
    queries_df['ratings'] = queries_df['query_string'].map(ratings_grouped)
    queries_df['query'] = queries_df['query_string']
    
    # Filter queries with ratings
    queries_with_ratings = queries_df[queries_df['ratings'].notna()]
    print(f"  Test queries with ratings: {len(queries_with_ratings)}")
    
    # Apply sample size if specified
    if sample_size:
        queries_with_ratings = queries_with_ratings.head(sample_size)
        print(f"  Using sample size: {sample_size}")
    
    return queries_with_ratings


def main():
    parser = argparse.ArgumentParser(description='Evaluate O19S Model with MinMaxScaler')
    parser.add_argument('--model-file', type=str, required=True,
                       help='Path to trained model pickle file')
    parser.add_argument('--scaler-file', type=str, required=True,
                       help='Path to fitted scaler pickle file')
    parser.add_argument('--model-id', type=str, required=True,
                       help='Neural model ID in OpenSearch')
    parser.add_argument('--host', type=str, default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('--sample-size', type=int, default=50,
                       help='Number of queries to evaluate')
    parser.add_argument('--output-file', type=str, default='o19s_minmax_scaled_evaluation.json',
                       help='Output file for evaluation results')
    
    args = parser.parse_args()
    
    print("O19S Evaluation with MinMaxScaler Feature Normalization")
    print("=" * 50)
    
    # Load model and scaler
    print(f"\n📦 Loading model from {args.model_file}")
    with open(args.model_file, 'rb') as f:
        model = pickle.load(f)
    
    print(f"📦 Loading scaler from {args.scaler_file}")
    with open(args.scaler_file, 'rb') as f:
        scaler = pickle.load(f)
    
    # Show scaler info
    print(f"\n🔧 MinMaxScaler Configuration:")
    print(f"  Feature range: {scaler.feature_range}")
    print(f"  Number of features: {scaler.n_features_in_}")
    
    # Load test queries
    queries_df = load_test_queries(sample_size=args.sample_size)
    
    # Initialize evaluator
    print(f"\nInitializing evaluator with model ID: {args.model_id}")
    evaluator = O19SScaledEvaluator(
        model=model,
        scaler=scaler,
        model_id=args.model_id,
        host=args.host,
        port=args.port
    )
    
    # Evaluate queries
    results = evaluator.evaluate_queries(queries_df)
    
    # Calculate statistics
    dynamic_ndcg_scores = results['dynamic']['ndcg_scores']
    dynamic_weights = results['dynamic']['weights']
    
    print("\n" + "=" * 50)
    print("📊 EVALUATION RESULTS")
    print("=" * 50)
    
    print(f"\n🎯 Dynamic Weight Prediction (with MinMaxScaler):")
    print(f"  Mean NDCG@10: {np.mean(dynamic_ndcg_scores):.4f}")
    print(f"  Std NDCG@10: {np.std(dynamic_ndcg_scores):.4f}")
    print(f"  Min NDCG@10: {np.min(dynamic_ndcg_scores):.4f}")
    print(f"  Max NDCG@10: {np.max(dynamic_ndcg_scores):.4f}")
    
    print(f"\n⚖️ Weight Distribution:")
    weight_counts = pd.Series(dynamic_weights).value_counts().sort_index()
    for weight, count in weight_counts.items():
        pct = count / len(dynamic_weights) * 100
        print(f"  Weight {weight:.2f}: {count:3d} queries ({pct:5.1f}%)")
    print(f"  Mean weight: {np.mean(dynamic_weights):.3f}")
    
    print(f"\n📈 Static Weight Baselines:")
    for weight_key, weight_data in results['static'].items():
        static_scores = weight_data['ndcg_scores']
        print(f"  Weight {weight_key}:")
        print(f"    Mean NDCG@10: {np.mean(static_scores):.4f}")
        print(f"    Std NDCG@10: {np.std(static_scores):.4f}")
    
    # Calculate improvements
    print(f"\n🚀 Performance Comparison:")
    for weight_key, weight_data in results['static'].items():
        static_mean = np.mean(weight_data['ndcg_scores'])
        dynamic_mean = np.mean(dynamic_ndcg_scores)
        improvement = (dynamic_mean - static_mean) / static_mean * 100
        print(f"  vs Static {weight_key}: {improvement:+.1f}% improvement")
    
    # Wins/Losses analysis
    print(f"\n🏆 Win/Loss Analysis (Dynamic vs Static):")
    for weight_key, weight_data in results['static'].items():
        wins = sum(d > s for d, s in zip(dynamic_ndcg_scores, weight_data['ndcg_scores']))
        losses = sum(d < s for d, s in zip(dynamic_ndcg_scores, weight_data['ndcg_scores']))
        ties = len(dynamic_ndcg_scores) - wins - losses
        print(f"  vs Static {weight_key}: {wins} wins, {losses} losses, {ties} ties")
    
    # Save results
    evaluation_results = {
        'model_file': args.model_file,
        'scaler_file': args.scaler_file,
        'sample_size': args.sample_size,
        'num_queries_evaluated': len(dynamic_ndcg_scores),
        'scaling': 'MinMaxScaler',
        'dynamic': {
            'mean_ndcg': float(np.mean(dynamic_ndcg_scores)),
            'std_ndcg': float(np.std(dynamic_ndcg_scores)),
            'min_ndcg': float(np.min(dynamic_ndcg_scores)),
            'max_ndcg': float(np.max(dynamic_ndcg_scores)),
            'mean_weight': float(np.mean(dynamic_weights)),
            'weight_distribution': weight_counts.to_dict()
        },
        'static': {
            weight_key: {
                'mean_ndcg': float(np.mean(weight_data['ndcg_scores'])),
                'std_ndcg': float(np.std(weight_data['ndcg_scores']))
            }
            for weight_key, weight_data in results['static'].items()
        },
        'query_details': results['query_details'][:20]  # Save first 20 for inspection
    }
    
    with open(args.output_file, 'w') as f:
        json.dump(evaluation_results, f, indent=2)
    print(f"\n💾 Results saved to {args.output_file}")
    
    print("\n" + "=" * 50)
    print("✅ Evaluation Complete with MinMaxScaler!")


if __name__ == "__main__":
    main()
