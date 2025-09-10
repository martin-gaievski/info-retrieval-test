#!/usr/bin/env python3
"""
O19S True Methodology Evaluation Script

Implements the actual O19S evaluation methodology where:
1. For each query, test all weight values (0.1-0.9)
2. Use model to predict NDCG for each weight
3. Select the weight that produces highest predicted NDCG
4. Execute search with selected weight and calculate actual NDCG

Author: Dynamic Hybrid Search Team
Version: 2.0.0
"""

import os
import sys
import json
import argparse
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
import pickle
from tqdm import tqdm
from pathlib import Path
import requests
from collections import defaultdict

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, '..'))

# O19S imports
from dynamic_hybrid.utils import metrics
from opensearchpy import OpenSearch

# BEIR imports
from beir import LoggingHandler

# Feature extractors
from feature_extractor_o19s_enhanced import O19SEnhancedFeatureExtractor

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class O19STrueMethodologyEvaluator:
    """Evaluate using O19S true methodology - predict NDCG for all weights"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None):
        """Initialize O19S true methodology evaluator"""
        
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
        
        # Initialize O19S enhanced feature extractor
        self.feature_extractor = O19SEnhancedFeatureExtractor(
            client=self.client,
            host=host,
            port=port,
            index_name=index_name,
            model_id=model_id,
            cache_term_stats=True
        )
        
        logger.info(f"Initialized O19S true methodology evaluator for {host}:{port}/{index_name}")
    
    def load_model(self, model_path: str) -> Dict:
        """Load trained model"""
        with open(model_path, 'rb') as f:
            model_dict = pickle.load(f)
        
        # Verify this is a true methodology model
        if not model_dict.get('o19s_true_methodology', False):
            logger.warning("Model was not trained with O19S true methodology!")
        
        if model_dict.get('predicts') != 'ndcg':
            raise ValueError(f"Model predicts {model_dict.get('predicts')}, expected 'ndcg'")
        
        logger.info(f"Loaded {model_dict['model_type']} model")
        logger.info(f"Model predicts: {model_dict.get('predicts', 'unknown').upper()}")
        logger.info(f"Features: {len(model_dict['feature_columns'])}")
        
        return model_dict
    
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
    
    def predict_best_weight(self, 
                           query_string: str,
                           model_dict: Dict,
                           weight_values: List[float] = None) -> Tuple[float, float, Dict[float, float]]:
        """
        O19S TRUE methodology: Test all weights and select one with highest predicted NDCG
        
        Returns:
            best_weight: Weight with highest predicted NDCG
            best_predicted_ndcg: The predicted NDCG for best weight
            all_predictions: Dict of weight -> predicted NDCG
        """
        if weight_values is None:
            weight_values = [round(0.1 + i * 0.1, 1) for i in range(9)]
        
        # Extract base features (without weight)
        base_features = self.feature_extractor.extract_features(query_string)
        
        # Get model components
        model = model_dict['model']
        scaler = model_dict['scaler']
        feature_columns = model_dict['feature_columns']
        
        # Test each weight value
        all_predictions = {}
        best_weight = None
        best_predicted_ndcg = -1
        
        for neural_weight in weight_values:
            # Create feature vector with weight as input
            features_dict = {
                'neural_weight': neural_weight,
                **base_features
            }
            
            # Ensure features are in correct order
            feature_values = [features_dict[col] for col in feature_columns]
            
            # Scale features
            X = np.array(feature_values).reshape(1, -1)
            X_scaled = scaler.transform(X)
            
            # Predict NDCG for this weight
            predicted_ndcg = model.predict(X_scaled)[0]
            all_predictions[neural_weight] = predicted_ndcg
            
            # Track best weight
            if predicted_ndcg > best_predicted_ndcg:
                best_predicted_ndcg = predicted_ndcg
                best_weight = neural_weight
        
        return best_weight, best_predicted_ndcg, all_predictions
    
    def evaluate_o19s_test_set(self,
                              model_dict: Dict,
                              o19s_data_path: str,
                              ratings_file: str,
                              sample_size: Optional[int] = None,
                              weight_values: List[float] = None) -> Dict:
        """Evaluate on O19S test set using true methodology"""
        
        logger.info("Evaluating with O19S TRUE methodology...")
        logger.info("Testing all weights per query, selecting highest predicted NDCG")
        
        # Load O19S data
        df_train, df_test, df_ratings = self.load_o19s_data(o19s_data_path, ratings_file)
        
        # Create reference dictionary
        reference = {query: df for query, df in df_ratings.groupby("query")}
        
        # Get test queries
        test_queries = df_test['query_string'].tolist()
        if sample_size and sample_size < len(test_queries):
            np.random.seed(42)
            test_queries = np.random.choice(test_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(test_queries)} test queries")
        else:
            logger.info(f"Using all {len(test_queries)} O19S test queries")
        
        # Filter to queries with ratings
        test_queries_with_ratings = [q for q in test_queries if q in reference]
        logger.info(f"Test queries with ratings: {len(test_queries_with_ratings)}")
        
        # Evaluation results
        results = []
        weight_distribution = defaultdict(int)
        prediction_details = []
        
        # Process each test query
        for query_string in tqdm(test_queries_with_ratings, desc="Evaluating"):
            try:
                # Predict best weight using O19S true methodology
                best_weight, best_predicted_ndcg, all_predictions = self.predict_best_weight(
                    query_string, model_dict, weight_values
                )
                
                weight_distribution[best_weight] += 1
                
                # Execute search with predicted best weight
                lexical_weight = round(1.0 - best_weight, 2)
                search_results = self._execute_hybrid_search(query_string, lexical_weight, best_weight)
                
                if not search_results.empty:
                    # Calculate actual NDCG
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
                        actual_ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                        
                        results.append({
                            'query': query_string,
                            'predicted_weight': best_weight,
                            'predicted_ndcg': best_predicted_ndcg,
                            'actual_ndcg': actual_ndcg,
                            'prediction_error': abs(best_predicted_ndcg - actual_ndcg)
                        })
                        
                        # Store detailed predictions for analysis
                        prediction_details.append({
                            'query': query_string,
                            'all_predictions': all_predictions,
                            'selected_weight': best_weight,
                            'actual_ndcg': actual_ndcg
                        })
                
            except Exception as e:
                logger.warning(f"Failed to evaluate query '{query_string[:50]}...': {e}")
                continue
        
        # Calculate metrics
        if results:
            results_df = pd.DataFrame(results)
            
            # Overall NDCG
            mean_ndcg = results_df['actual_ndcg'].mean()
            
            # Prediction accuracy
            mean_prediction_error = results_df['prediction_error'].mean()
            prediction_correlation = results_df[['predicted_ndcg', 'actual_ndcg']].corr().iloc[0, 1]
            
            # Weight selection analysis
            weight_stats = {
                'distribution': dict(weight_distribution),
                'most_common': max(weight_distribution.items(), key=lambda x: x[1])[0],
                'unique_weights': len(weight_distribution)
            }
            
            # Compare with fixed weights
            fixed_weight_results = self._evaluate_fixed_weights(
                test_queries_with_ratings, reference, weight_values
            )
            
            evaluation_results = {
                'methodology': 'o19s_true',
                'num_queries': len(results),
                'mean_ndcg': mean_ndcg,
                'std_ndcg': results_df['actual_ndcg'].std(),
                'min_ndcg': results_df['actual_ndcg'].min(),
                'max_ndcg': results_df['actual_ndcg'].max(),
                'prediction_metrics': {
                    'mean_error': mean_prediction_error,
                    'correlation': prediction_correlation,
                    'mean_predicted_ndcg': results_df['predicted_ndcg'].mean()
                },
                'weight_selection': weight_stats,
                'fixed_weight_comparison': fixed_weight_results,
                'improvement_over_best_fixed': mean_ndcg - max(fixed_weight_results.values()),
                'detailed_results': results_df,
                'prediction_details': prediction_details
            }
            
            return evaluation_results
        
        else:
            logger.error("No successful evaluations")
            return {'error': 'No successful evaluations'}
    
    def _evaluate_fixed_weights(self, 
                               queries: List[str], 
                               reference: Dict,
                               weight_values: List[float]) -> Dict[float, float]:
        """Evaluate performance of fixed weights for comparison"""
        
        logger.info("Evaluating fixed weights for comparison...")
        fixed_results = {}
        
        for neural_weight in weight_values:
            lexical_weight = round(1.0 - neural_weight, 2)
            ndcg_scores = []
            
            for query_string in queries[:50]:  # Sample for speed
                try:
                    search_results = self._execute_hybrid_search(query_string, lexical_weight, neural_weight)
                    
                    if not search_results.empty and query_string in reference:
                        df_with_ratings = self._merge_results_with_reference(
                            search_results, reference[query_string]
                        )
                        
                        if not df_with_ratings.empty:
                            ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                            ndcg_scores.append(ndcg)
                
                except:
                    continue
            
            if ndcg_scores:
                fixed_results[neural_weight] = np.mean(ndcg_scores)
        
        return fixed_results
    
    def _execute_hybrid_search(self, query: str, lexical_weight: float, neural_weight: float) -> pd.DataFrame:
        """Execute hybrid search"""
        
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
                "description": "O19S evaluation hybrid search",
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
    
    def _merge_results_with_reference(self, search_results: pd.DataFrame, reference_ratings: pd.DataFrame) -> pd.DataFrame:
        """Merge search results with reference ratings"""
        
        if search_results.empty or reference_ratings.empty:
            return pd.DataFrame()
            
        merged = search_results.merge(
            reference_ratings,
            left_on='product_id',
            right_on='docid',
            how='left'
        )
        
        merged['rating'] = merged['rating'].fillna(0)
        
        return merged[['position', 'rating', 'product_id', 'relevance']]


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate O19S TRUE methodology (test all weights, select highest predicted NDCG)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This implements the ACTUAL O19S evaluation methodology:
1. For each query, test all weights (0.1-0.9)
2. Predict NDCG for each weight using trained model
3. Select weight with highest predicted NDCG
4. Execute search with selected weight
5. Calculate actual NDCG

Examples:
  # Evaluate with trained model
  python3 %(prog)s --model model.pkl --host your-cluster.com --port 80 --model-id MODEL_ID
  
  # Fast evaluation with sampling
  python3 %(prog)s --model model.pkl --host your-cluster.com --port 80 --model-id MODEL_ID --sample-size 100
        """
    )
    
    parser.add_argument('--model', required=True, help='Path to trained model pickle file')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products', help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to O19S ratings.csv file')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of test queries to sample (default: None = use all)')
    parser.add_argument('--weight-values', nargs='+', type=float, default=None,
                       help='Neural weight values to test (default: 0.1-0.9)')
    parser.add_argument('-o', '--output', default='o19s_true_methodology_results.json',
                       help='Output results file')
    
    args = parser.parse_args()
    
    # Initialize evaluator
    evaluator = O19STrueMethodologyEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id
    )
    
    # Load model
    model_dict = evaluator.load_model(args.model)
    
    # Evaluate on O19S test set
    results = evaluator.evaluate_o19s_test_set(
        model_dict=model_dict,
        o19s_data_path=args.o19s_data,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        weight_values=args.weight_values
    )
    
    if 'error' not in results:
        # Save detailed results
        with open(args.output, 'w') as f:
            # Convert DataFrame to dict for JSON serialization
            results_copy = results.copy()
            if 'detailed_results' in results_copy:
                results_copy['detailed_results'] = results_copy['detailed_results'].to_dict('records')
            json.dump(results_copy, f, indent=2)
        
        # Print summary
        print("\n=== O19S TRUE Methodology Evaluation Results ===")
        print(f"Methodology: {results['methodology']}")
        print(f"Queries evaluated: {results['num_queries']}")
        print(f"\nPerformance:")
        print(f"  Mean NDCG@10: {results['mean_ndcg']:.4f}")
        print(f"  Std NDCG@10: {results['std_ndcg']:.4f}")
        print(f"  Range: [{results['min_ndcg']:.4f}, {results['max_ndcg']:.4f}]")
        
        print(f"\nPrediction Quality:")
        print(f"  Mean prediction error: {results['prediction_metrics']['mean_error']:.4f}")
        print(f"  Prediction correlation: {results['prediction_metrics']['correlation']:.4f}")
        print(f"  Mean predicted NDCG: {results['prediction_metrics']['mean_predicted_ndcg']:.4f}")
        
        print(f"\nWeight Selection:")
        print(f"  Unique weights used: {results['weight_selection']['unique_weights']}")
        print(f"  Most common weight: {results['weight_selection']['most_common']}")
        print(f"  Distribution:")
        for weight, count in sorted(results['weight_selection']['distribution'].items()):
            print(f"    {weight}: {count} queries")
        
        print(f"\nFixed Weight Comparison:")
        best_fixed_weight = max(results['fixed_weight_comparison'].items(), key=lambda x: x[1])
        print(f"  Best fixed weight: {best_fixed_weight[0]} (NDCG: {best_fixed_weight[1]:.4f})")
        print(f"  Improvement over best fixed: {results['improvement_over_best_fixed']:.4f}")
        
        print(f"\nResults saved to: {args.output}")
    
    else:
        print(f"Evaluation failed: {results['error']}")


if __name__ == "__main__":
    main()
