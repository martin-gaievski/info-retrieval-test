#!/usr/bin/env python3
"""
O19S Ridge Model Evaluation Script

This script evaluates Ridge Regression models trained by train_o19s_ridge_optimized.py:
- Supports both MinMaxScaler and StandardScaler
- Works with Ridge models (with alpha regularization)
- Uses 18 features (1 weight + 17 O19S features)
- Evaluates dynamic weight prediction vs static baselines
- Provides comprehensive metrics following O19S methodology
"""

import json
import numpy as np
import pandas as pd
import pickle
import argparse
from typing import Dict, List, Tuple, Optional
from opensearchpy import OpenSearch
import warnings
warnings.filterwarnings('ignore')

# Import feature extractor and metrics
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.feature_extractor_o19s_exact import O19SExactFeatureExtractor
from dynamic_hybrid.utils import metrics


class O19SRidgeEvaluator:
    """O19S evaluation for Ridge models with flexible scaler support"""
    
    def __init__(self, model, scaler, model_id: str, host: str = 'localhost', port: int = 9200):
        """
        Initialize evaluator with Ridge model and scaler
        
        Args:
            model: Trained Ridge model
            scaler: Feature scaler (MinMaxScaler or StandardScaler)
            model_id: Neural model ID for OpenSearch
            host: OpenSearch host
            port: OpenSearch port
        """
        self.model = model
        self.scaler = scaler
        
        # Detect scaler type
        scaler_class_name = self.scaler.__class__.__name__
        self.scaler_type = 'minmax' if 'MinMax' in scaler_class_name else 'standard'
        
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
    
    def predict_optimal_weight(self, query: str, granularity: float = 0.1) -> Tuple[float, float]:
        """
        Predict optimal weight for a query using scaled features
        
        Args:
            query: Search query
            granularity: Weight step size for evaluation
            
        Returns:
            Tuple of (predicted_weight, predicted_ndcg)
        """
        # Extract query features
        query_features = self.extract_features(query)
        
        # Test different weights with specified granularity
        test_weights = np.arange(0.0, 1.0 + granularity, granularity)
        best_weight = 0.5
        best_score = -1
        
        feature_vectors = []
        for weight in test_weights:
            # Create feature vector with weight as first feature (O19S style)
            # f_0_neuralness is the weight
            features = [weight] + query_features
            feature_vectors.append(features)
        
        # Scale all feature vectors at once
        feature_vectors = np.array(feature_vectors)
        feature_vectors_scaled = self.scaler.transform(feature_vectors)
        
        # Predict NDCG scores for all weights
        predicted_scores = self.model.predict(feature_vectors_scaled)
        
        # Handle potential negative predictions
        if self.scaler_type == 'standard':
            # StandardScaler can produce negative predictions
            # Clip to [0, 1] range for NDCG
            predicted_scores = np.clip(predicted_scores, 0, 1)
        
        # Find weight with highest predicted NDCG
        best_idx = np.argmax(predicted_scores)
        best_weight = test_weights[best_idx]
        best_score = predicted_scores[best_idx]
        
        # Round to granularity
        best_weight = round(best_weight / granularity) * granularity
        
        return best_weight, best_score
    
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
                "description": "O19S Ridge optimized evaluation pipeline",
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
                        static_weights: List[float] = None,
                        evaluate_static_baselines: bool = True,
                        weight_granularity: float = 0.1) -> Dict:
        """
        Evaluate queries using dynamic weight prediction and optionally static baselines
        Following O19S methodology with multiple metrics
        
        Args:
            queries_df: DataFrame with queries and ratings
            static_weights: List of static weights to compare against
            evaluate_static_baselines: Whether to evaluate static weight baselines
            weight_granularity: Granularity for weight prediction
            
        Returns:
            Dictionary with evaluation results including all metrics
        """
        if static_weights is None:
            static_weights = [0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 0.9]
        
        # Define metrics to evaluate (following O19S team)
        metric_functions = [
            ("dcg", metrics.dcg_at_10, None),
            ("ndcg", metrics.ndcg_at_10, None),
            ("prec@10", metrics.precision_at_k, None),
            ("ratio_of_ratings", metrics.ratio_of_ratings, None)
        ]
        
        # Initialize results with all metrics
        results = {
            'dynamic': {
                'weights': [],
                'predicted_scores': []
            },
            'static': {},
            'query_details': [],
            'metrics_df': [],  # For O19S-style aggregation
            'model_info': {
                'scaler_type': self.scaler_type,
                'model_type': self.model.__class__.__name__,
                'alpha': getattr(self.model, 'alpha', None)
            }
        }
        
        # Initialize metric storage for each approach
        for metric_name, _, _ in metric_functions:
            results['dynamic'][metric_name] = []
            if evaluate_static_baselines:
                for w in static_weights:
                    if f'{w:.1f}' not in results['static']:
                        results['static'][f'{w:.1f}'] = {}
                    results['static'][f'{w:.1f}'][metric_name] = []
        
        print(f"\n📊 Evaluating {len(queries_df)} queries with Ridge model...")
        print(f"📐 Model: {results['model_info']['model_type']}")
        print(f"🔧 Scaler: {self.scaler_type.capitalize()}Scaler")
        if results['model_info']['alpha']:
            print(f"📈 Alpha: {results['model_info']['alpha']}")
        print(f"⚖️ Weight granularity: {weight_granularity}")
        print(f"📏 Metrics: DCG@10, NDCG@10, Precision@10, Ratio of Ratings")
        if evaluate_static_baselines:
            print(f"📍 Static weight baselines: {static_weights}")
        else:
            print("📍 Static weight baselines: Disabled")
        
        evaluated_count = 0
        for idx, row in queries_df.iterrows():
            query = row['query']
            relevance_dict = row['ratings']
            
            # Skip if no relevant items
            if not relevance_dict:
                continue
            
            evaluated_count += 1
            
            # Create reference DataFrame for metric calculation
            reference_df = pd.DataFrame([
                {'docid': doc_id, 'rating': rating}
                for doc_id, rating in relevance_dict.items()
            ])
            
            # Dynamic weight prediction
            predicted_weight, predicted_score = self.predict_optimal_weight(
                query, 
                granularity=weight_granularity
            )
            
            # Get dynamic results
            dynamic_results = self.hybrid_search(query, predicted_weight, size=100)
            
            # Calculate metrics for dynamic approach
            dynamic_metrics = {}
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
                
                # Calculate all metrics
                for metric_name, metric_func, _ in metric_functions:
                    if metric_name == "ndcg":
                        metric_value = metric_func(merged_df, reference=reference_df)
                    else:
                        metric_value = metric_func(merged_df)
                    dynamic_metrics[metric_name] = metric_value
                    results['dynamic'][metric_name].append(metric_value)
            else:
                for metric_name, _, _ in metric_functions:
                    dynamic_metrics[metric_name] = 0.0
                    results['dynamic'][metric_name].append(0.0)
            
            results['dynamic']['weights'].append(predicted_weight)
            results['dynamic']['predicted_scores'].append(predicted_score)
            
            # Store for O19S-style DataFrame
            results['metrics_df'].append({
                'query_id': idx,
                'query': query[:50] + '...' if len(query) > 50 else query,
                'model': 'dynamic',
                'weight': predicted_weight,
                'predicted_ndcg': predicted_score,
                **{f'metric_{k}': v for k, v in dynamic_metrics.items()}
            })
            
            # Static weight baselines (only if enabled)
            static_metrics = {}
            if evaluate_static_baselines:
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
                        
                        # Calculate all metrics
                        for metric_name, metric_func, _ in metric_functions:
                            if metric_name == "ndcg":
                                metric_value = metric_func(merged_df, reference=reference_df)
                            else:
                                metric_value = metric_func(merged_df)
                            results['static'][f'{static_weight:.1f}'][metric_name].append(metric_value)
                            
                            if static_weight not in static_metrics:
                                static_metrics[static_weight] = {}
                            static_metrics[static_weight][metric_name] = metric_value
                    else:
                        for metric_name, _, _ in metric_functions:
                            results['static'][f'{static_weight:.1f}'][metric_name].append(0.0)
                            if static_weight not in static_metrics:
                                static_metrics[static_weight] = {}
                            static_metrics[static_weight][metric_name] = 0.0
                    
                    # Store for O19S-style DataFrame
                    results['metrics_df'].append({
                        'query_id': idx,
                        'query': query[:50] + '...' if len(query) > 50 else query,
                        'model': f'static_{static_weight:.1f}',
                        'weight': static_weight,
                        'predicted_ndcg': None,  # Static doesn't predict
                        **{f'metric_{k}': v for k, v in static_metrics[static_weight].items()}
                    })
            
            # Store query details with all metrics
            query_detail = {
                'query': query[:50] + '...' if len(query) > 50 else query,
                'predicted_weight': predicted_weight,
                'predicted_score': predicted_score,
            }
            
            # Add dynamic metrics
            for metric_name in dynamic_metrics:
                query_detail[f'dynamic_{metric_name}'] = dynamic_metrics[metric_name]
            
            # Add static metrics
            if evaluate_static_baselines:
                for static_weight in static_weights:
                    for metric_name in static_metrics.get(static_weight, {}):
                        query_detail[f'static_{static_weight:.1f}_{metric_name}'] = static_metrics[static_weight][metric_name]
            
            results['query_details'].append(query_detail)
            
            if evaluated_count % 10 == 0:
                print(f"  Processed {evaluated_count} queries...")
        
        print(f"\n✅ Evaluated {evaluated_count} queries successfully")
        
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
    parser = argparse.ArgumentParser(
        description='Evaluate O19S Ridge Model (Trained by train_o19s_ridge_optimized.py)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  # Evaluate Ridge model with separate scaler file
  python evaluate_o19s_ridge_optimized.py \\
    --model-file o19s_ridge_optimized_model.pkl \\
    --scaler-file o19s_ridge_optimized_scaler.pkl \\
    --model-id huggingface/sentence-transformers/all-MiniLM-L6-v2 \\
    --sample-size 100

  # Evaluate model with embedded scaler (if trained with new version)
  python evaluate_o19s_ridge_optimized.py \\
    --model-file o19s_ridge_model_with_scaler.pkl \\
    --model-id huggingface/sentence-transformers/all-MiniLM-L6-v2 \\
    --sample-size 100
        """
    )
    
    parser.add_argument('--model-file', type=str, required=True,
                       help='Path to trained Ridge model pickle file')
    parser.add_argument('--scaler-file', type=str, default=None,
                       help='Path to scaler pickle file (optional if model has embedded scaler)')
    parser.add_argument('--model-id', type=str, required=True,
                       help='Neural model ID in OpenSearch')
    parser.add_argument('--host', type=str, default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('--sample-size', type=int, default=50,
                       help='Number of queries to evaluate')
    parser.add_argument('--skip-static-baselines', action='store_true',
                       help='Skip static weight baseline evaluation (dynamic only)')
    parser.add_argument('--weight-granularity', type=float, default=0.1,
                       help='Granularity for weight prediction (default: 0.1)')
    parser.add_argument('--static-weights', type=str, default=None,
                       help='Comma-separated list of static weights to test')
    parser.add_argument('--output-file', type=str, default='o19s_ridge_evaluation.json',
                       help='Output file for evaluation results')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("O19S RIDGE MODEL EVALUATION")
    print("Evaluating models trained by train_o19s_ridge_optimized.py")
    print("=" * 70)
    
    # Load model
    print(f"\n📦 Loading model from {args.model_file}")
    with open(args.model_file, 'rb') as f:
        model = pickle.load(f)
    
    # Check if this is actually a scaler (common user error)
    if hasattr(model, 'transform') and hasattr(model, 'fit'):
        print(f"\n❌ ERROR: The file '{args.model_file}' appears to be a scaler, not a model!")
        print(f"   It's a {model.__class__.__name__} object.")
        print(f"\n📝 Correct usage:")
        print(f"   --model-file should point to the Ridge model file (e.g., o19s_ridge_optimized_model.pkl)")
        print(f"   --scaler-file should point to the scaler file (e.g., o19s_ridge_optimized_scaler.pkl)")
        return
    
    # Check for embedded scaler or load separate scaler
    if hasattr(model, 'scaler'):
        # Model has embedded scaler (new approach)
        scaler = model.scaler
        scaler_type = scaler.__class__.__name__
        print(f"✅ Found embedded scaler in model")
    elif args.scaler_file:
        # Load separate scaler file (backward compatibility)
        print(f"📦 Loading separate scaler from {args.scaler_file}")
        with open(args.scaler_file, 'rb') as f:
            scaler = pickle.load(f)
        scaler_type = scaler.__class__.__name__
        print(f"✅ Loaded separate {scaler_type}")
    else:
        # No embedded scaler and no scaler file provided
        print(f"\n❌ ERROR: Model does not have an embedded scaler!")
        print(f"   The model appears to be a plain {model.__class__.__name__} without scaling.")
        print(f"\n📝 Solutions:")
        print(f"   1. Provide the scaler file using --scaler-file parameter")
        print(f"      Example: --scaler-file o19s_ridge_optimized_scaler.pkl")
        print(f"   2. Re-train the model using train_o19s_ridge_optimized.py to get embedded scaler")
        print(f"\n📋 Available files in current directory:")
        import os
        ridge_files = [f for f in os.listdir('.') if 'ridge' in f.lower() and f.endswith('.pkl')]
        for f in ridge_files[:10]:  # Show up to 10 files
            print(f"   - {f}")
        return
    
    # Display model and scaler info
    model_type = model.__class__.__name__
    
    print(f"\n🎯 Model Information:")
    print(f"  Model type: {model_type}")
    if hasattr(model, 'alpha'):
        print(f"  Alpha (regularization): {model.alpha}")
    if hasattr(model, 'coef_'):
        print(f"  Number of coefficients: {len(model.coef_)}")
        print(f"  Intercept: {model.intercept_:.4f}")
    
    print(f"\n🔧 Scaler Information:")
    print(f"  Scaler type: {scaler_type}")
    if hasattr(scaler, 'feature_range'):
        print(f"  Feature range: {scaler.feature_range}")
    print(f"  Number of features: {scaler.n_features_in_}")
    
    # Parse static weights if provided
    if args.static_weights:
        static_weights = [float(w.strip()) for w in args.static_weights.split(',')]
    else:
        static_weights = [0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 0.9]
    
    # Load test queries
    queries_df = load_test_queries(sample_size=args.sample_size)
    
    # Initialize evaluator
    print(f"\n🚀 Initializing evaluator with model ID: {args.model_id}")
    evaluator = O19SRidgeEvaluator(
        model=model,
        scaler=scaler,
        model_id=args.model_id,
        host=args.host,
        port=args.port
    )
    
    # Evaluate queries
    results = evaluator.evaluate_queries(
        queries_df,
        static_weights=static_weights,
        evaluate_static_baselines=not args.skip_static_baselines,
        weight_granularity=args.weight_granularity
    )
    
    # Create DataFrame from metrics results (O19S methodology)
    df_metrics = pd.DataFrame(results['metrics_df'])
    
    # Melt the DataFrame to have metric names and values in separate columns
    metric_cols = [col for col in df_metrics.columns if col.startswith('metric_')]
    df_melted = df_metrics.melt(
        id_vars=['query_id', 'query', 'model', 'weight', 'predicted_ndcg'],
        value_vars=metric_cols,
        var_name='metric',
        value_name='value'
    )
    df_melted['metric'] = df_melted['metric'].str.replace('metric_', '')
    
    # O19S Team Method: Calculate Metrics per Model by Averaging the Query Metrics
    df_metrics_per_pipeline = df_melted.pivot_table(
        index="model", 
        columns="metric", 
        values="value", 
        aggfunc=lambda x: x.mean().round(4)
    )
    df_metrics_per_pipeline = df_metrics_per_pipeline.reset_index()
    
    print("\n" + "=" * 70)
    print("📊 EVALUATION RESULTS")
    print("=" * 70)
    
    # Display the aggregated metrics table
    print("\n📈 Metrics Summary (All Models):")
    print("-" * 70)
    print(df_metrics_per_pipeline.to_string(index=False))
    
    # Extract dynamic metrics for detailed display
    dynamic_row = df_metrics_per_pipeline[df_metrics_per_pipeline['model'] == 'dynamic']
    if not dynamic_row.empty:
        print(f"\n🎯 Dynamic Weight Prediction ({scaler_type}):")
        for metric in ['dcg', 'ndcg', 'prec@10', 'ratio_of_ratings']:
            if metric in dynamic_row.columns:
                value = dynamic_row[metric].values[0]
                print(f"  {metric.upper()}: {value:.4f}")
    
    # Weight distribution
    dynamic_weights = results['dynamic']['weights']
    predicted_scores = results['dynamic']['predicted_scores']
    
    print(f"\n⚖️ Weight Distribution:")
    weight_counts = pd.Series(dynamic_weights).value_counts().sort_index()
    for weight, count in weight_counts.items():
        pct = count / len(dynamic_weights) * 100
        print(f"  Weight {weight:.2f}: {count:3d} queries ({pct:5.1f}%)")
    print(f"  Mean weight: {np.mean(dynamic_weights):.3f}")
    print(f"  Std weight: {np.std(dynamic_weights):.3f}")
    
    print(f"\n📊 Prediction Statistics:")
    print(f"  Mean predicted NDCG: {np.mean(predicted_scores):.4f}")
    print(f"  Std predicted NDCG: {np.std(predicted_scores):.4f}")
    print(f"  Min predicted NDCG: {np.min(predicted_scores):.4f}")
    print(f"  Max predicted NDCG: {np.max(predicted_scores):.4f}")
    
    # Calculate improvements for each metric
    if not args.skip_static_baselines:
        print(f"\n🚀 Performance Comparison (Dynamic vs Static):")
        print("-" * 70)
        
        for metric in ['dcg', 'ndcg', 'prec@10', 'ratio_of_ratings']:
            if metric not in df_metrics_per_pipeline.columns:
                continue
                
            print(f"\n{metric.upper()} Improvements:")
            dynamic_value = df_metrics_per_pipeline[df_metrics_per_pipeline['model'] == 'dynamic'][metric].values[0]
            
            improvements = []
            for _, row in df_metrics_per_pipeline[df_metrics_per_pipeline['model'].str.startswith('static')].iterrows():
                static_model = row['model']
                static_value = row[metric]
                if static_value > 0:
                    improvement = (dynamic_value - static_value) / static_value * 100
                else:
                    improvement = 0
                improvements.append(improvement)
                print(f"  vs {static_model}: {improvement:+.1f}% ({static_value:.4f} → {dynamic_value:.4f})")
            
            if improvements:
                avg_improvement = np.mean(improvements)
                print(f"  Average improvement: {avg_improvement:+.1f}%")
    
    # Win/Loss analysis for NDCG (primary metric)
    if not args.skip_static_baselines:
        print(f"\n🏆 Win/Loss Analysis for NDCG@10 (Dynamic vs Static):")
        ndcg_scores = results['dynamic']['ndcg']
        for weight_key, weight_data in results['static'].items():
            static_ndcg = weight_data['ndcg']
            wins = sum(d > s for d, s in zip(ndcg_scores, static_ndcg))
            losses = sum(d < s for d, s in zip(ndcg_scores, static_ndcg))
            ties = len(ndcg_scores) - wins - losses
            print(f"  vs Static {weight_key}: {wins} wins, {losses} losses, {ties} ties")
    
    # Model insights
    if hasattr(model, 'coef_'):
        print(f"\n🔍 Model Insights:")
        weight_coef = model.coef_[0]  # First coefficient is for weight
        print(f"  Weight coefficient: {weight_coef:+.6f}")
        print(f"  Weight importance rank: {np.argsort(np.abs(model.coef_))[::-1].tolist().index(0) + 1}/{len(model.coef_)}")
        
        # Top features by coefficient magnitude
        feature_names = ['weight'] + [
            'query_length', 'has_special_char', 'has_punctuation',
            'capital_letters_ratio', 'stopwords_ratio',
            'max_doc_freq', 'min_doc_freq', 'total_doc_freq',
            'avg_doc_freq', 'var_doc_freq', 'std_doc_freq',
            'max_idf', 'min_idf', 'total_idf',
            'avg_idf', 'var_idf', 'std_idf'
        ]
        
        # Get top 5 features by absolute coefficient value
        coef_abs = np.abs(model.coef_)
        top_indices = np.argsort(coef_abs)[::-1][:5]
        
        print(f"\n  Top 5 features by coefficient magnitude:")
        for idx in top_indices:
            if idx < len(feature_names):
                feature_name = feature_names[idx]
                coef_value = model.coef_[idx]
                print(f"    {feature_name}: {coef_value:+.6f}")
    
    # Save results
    print(f"\n💾 Saving results to {args.output_file}")
    
    # Add metadata to results
    results['metadata'] = {
        'model_file': args.model_file,
        'model_type': model_type,
        'scaler_type': scaler_type,
        'num_queries': len(queries_df),
        'weight_granularity': args.weight_granularity,
        'static_weights_evaluated': static_weights if not args.skip_static_baselines else None,
        'neural_model_id': args.model_id
    }
    
    # Calculate summary statistics
    results['summary'] = {
        'dynamic': {},
        'static': {}
    }
    
    # Dynamic summary
    for metric in ['dcg', 'ndcg', 'prec@10', 'ratio_of_ratings']:
        if metric in results['dynamic']:
            values = results['dynamic'][metric]
            results['summary']['dynamic'][metric] = {
                'mean': np.mean(values),
                'std': np.std(values),
                'min': np.min(values),
                'max': np.max(values)
            }
    
    # Static summaries
    if not args.skip_static_baselines:
        for weight_key, weight_data in results['static'].items():
            results['summary']['static'][weight_key] = {}
            for metric in ['dcg', 'ndcg', 'prec@10', 'ratio_of_ratings']:
                if metric in weight_data:
                    values = weight_data[metric]
                    results['summary']['static'][weight_key][metric] = {
                        'mean': np.mean(values),
                        'std': np.std(values),
                        'min': np.min(values),
                        'max': np.max(values)
                    }
    
    # Save JSON results
    with open(args.output_file, 'w') as f:
        json.dump(results, f, indent=2, default=lambda x: float(x) if isinstance(x, np.number) else str(x))
    
    print(f"✅ Results saved successfully")
    
    # Save CSV results for easy analysis
    csv_file = args.output_file.replace('.json', '.csv')
    df_metrics.to_csv(csv_file, index=False)
    print(f"📊 Metrics CSV saved to {csv_file}")
    
    # Save aggregated results
    agg_csv_file = args.output_file.replace('.json', '_aggregated.csv')
    df_metrics_per_pipeline.to_csv(agg_csv_file, index=False)
    print(f"📊 Aggregated metrics CSV saved to {agg_csv_file}")
    
    print("\n" + "=" * 70)
    print("✅ EVALUATION COMPLETE")
    print("=" * 70)
    print(f"\n📄 Full results saved to:")
    print(f"  - JSON: {args.output_file}")
    print(f"  - CSV: {csv_file}")
    print(f"  - Aggregated: {agg_csv_file}")


if __name__ == "__main__":
    main()
