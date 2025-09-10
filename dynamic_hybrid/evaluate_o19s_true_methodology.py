#!/usr/bin/env python3
"""
O19S True Methodology Evaluation Script

Implements the ACTUAL O19S evaluation methodology where:
1. For each query, test ALL weights [0.1-0.9]
2. Predict NDCG for each weight (weight is an input feature)
3. Select the weight that produces the highest predicted NDCG
4. Use that weight to execute the actual search and measure performance

This matches the O19S documentation: "The search weight that produces 
the highest NDCG prediction is the best search weight."

Author: Dynamic Hybrid Search Team
Version: 2.0.0 - True O19S Implementation
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

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, '..'))

# O19S imports
from dynamic_hybrid.utils import metrics
from opensearchpy import OpenSearch

# BEIR imports
from beir import LoggingHandler

# Evaluation-only feature extractor
from feature_extractor_enhanced_evaluation import EnhancedEvaluationFeatureExtractor

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class O19STrueMethodologyEvaluator:
    """Evaluate using O19S TRUE methodology: predict NDCG for all weights, select best"""
    
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
        
        # Initialize evaluation feature extractor
        self.feature_extractor = EnhancedEvaluationFeatureExtractor()
        
        logger.info(f"Initialized O19S TRUE methodology evaluator")
        logger.info(f"Will test all weights and select based on predicted NDCG")
    
    def load_model(self, model_path: str) -> Dict:
        """Load trained O19S NDCG predictor model"""
        
        if not Path(model_path).exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
            
        with open(model_path, 'rb') as f:
            model_dict = pickle.load(f)
            
        logger.info(f"Loaded O19S model from {model_path}")
        logger.info(f"Model type: {model_dict['model_type']}")
        logger.info(f"Target: {model_dict.get('target', 'unknown')}")
        logger.info(f"Methodology: {model_dict.get('methodology', 'unknown')}")
        logger.info(f"Features: {len(model_dict['feature_columns'])}")
        
        # Verify this is a true methodology model
        if model_dict.get('target') != 'ndcg' or model_dict.get('methodology') != 'weight_as_input':
            logger.warning("Model may not be using true O19S methodology (weight as input, NDCG as target)")
        
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
                           weight_values: List[float] = None) -> Tuple[float, Dict[float, float]]:
        """
        Predict NDCG for all weights and select the best one.
        This is the core of O19S true methodology.
        
        Args:
            query_string: Query to predict for
            model_dict: Loaded model dictionary
            weight_values: Weights to test (default: 0.1-0.9)
            
        Returns:
            Tuple of (best_weight, ndcg_predictions_dict)
        """
        
        if weight_values is None:
            weight_values = [round(0.1 + i * 0.1, 1) for i in range(9)]
        
        model = model_dict['model']
        scaler = model_dict['scaler']
        feature_columns = model_dict['feature_columns']
        
        # Extract base features for the query (evaluation-only, no API calls)
        base_features = self.feature_extractor.extract_features(query_string)
        
        # Predict NDCG for each weight value
        ndcg_predictions = {}
        
        for neural_weight in weight_values:
            # Prepare features with weight as input
            features_with_weight = {
                **base_features,
                'neural_weight_input': neural_weight  # Add weight as input feature
            }
            
            # Get feature values in correct order
            feature_values = [features_with_weight.get(col, 0) for col in feature_columns]
            X = np.array([feature_values])
            X_scaled = scaler.transform(X)
            
            # Predict NDCG for this weight
            predicted_ndcg = float(model.predict(X_scaled)[0])
            ndcg_predictions[neural_weight] = predicted_ndcg
        
        # Select weight with highest predicted NDCG
        best_weight = max(ndcg_predictions.keys(), key=lambda w: ndcg_predictions[w])
        
        return best_weight, ndcg_predictions
    
    def evaluate_true_methodology(self,
                                 model_dict: Dict,
                                 o19s_data_path: str,
                                 ratings_file: str,
                                 sample_size: Optional[int] = None,
                                 compare_with_static: bool = True) -> Dict:
        """
        Evaluate using O19S TRUE methodology.
        For each query: predict NDCG for all weights, select best, execute search.
        
        Args:
            model_dict: Loaded model dictionary
            o19s_data_path: Path to O19S data
            ratings_file: Path to O19S ratings
            sample_size: Number of test queries to sample (None = use all)
            compare_with_static: Whether to compare with static weights
            
        Returns:
            Evaluation results dictionary
        """
        logger.info("Evaluating with O19S TRUE methodology...")
        
        # Load O19S data
        df_train, df_test, df_ratings = self.load_o19s_data(o19s_data_path, ratings_file)
        
        # Create reference dictionary
        reference = {query: df for query, df in df_ratings.groupby("query")}
        logger.info(f"Created reference for {len(reference)} queries")
        
        # Use test queries
        test_queries = df_test['query_string'].tolist()
        if sample_size and sample_size < len(test_queries):
            np.random.seed(42)
            test_queries = np.random.choice(test_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(test_queries)} test queries")
        else:
            logger.info(f"Using all {len(test_queries)} O19S test queries")
        
        # Filter to queries that have ratings
        test_queries_with_ratings = [q for q in test_queries if q in reference]
        logger.info(f"Test queries with ratings: {len(test_queries_with_ratings)}")
        
        # Evaluate with true methodology
        all_metrics = []
        weight_selections = []
        ndcg_prediction_details = []
        
        for query_string in tqdm(test_queries_with_ratings, desc="TRUE methodology evaluation"):
            try:
                # Step 1: Predict NDCG for all weights and select best
                best_weight, ndcg_predictions = self.predict_best_weight(query_string, model_dict)
                weight_selections.append(best_weight)
                
                # Store prediction details
                ndcg_prediction_details.append({
                    'query': query_string[:50],
                    'selected_weight': best_weight,
                    'predicted_ndcg': ndcg_predictions[best_weight],
                    'all_predictions': ndcg_predictions
                })
                
                # Step 2: Execute search with selected weight
                lexical_weight = round(1.0 - best_weight, 2)
                search_results = self._execute_hybrid_search(query_string, lexical_weight, best_weight)
                
                if not search_results.empty:
                    # Merge with ratings and calculate actual metrics
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
                        # Calculate actual O19S metrics
                        query_metrics = {
                            'dcg': metrics.dcg_at_10(df_with_ratings),
                            'ndcg': metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string]),
                            'prec@10': metrics.precision_at_k(df_with_ratings),
                            'ratio_of_ratings': metrics.ratio_of_ratings(df_with_ratings),
                            'selected_weight': best_weight,
                            'predicted_ndcg': ndcg_predictions[best_weight]
                        }
                        all_metrics.append(query_metrics)
                
            except Exception as e:
                logger.warning(f"Evaluation failed for query '{query_string[:50]}...': {e}")
                continue
        
        # Calculate average metrics
        if all_metrics:
            avg_dcg = np.mean([m['dcg'] for m in all_metrics])
            avg_ndcg = np.mean([m['ndcg'] for m in all_metrics])
            avg_precision = np.mean([m['prec@10'] for m in all_metrics])
            avg_ratio = np.mean([m['ratio_of_ratings'] for m in all_metrics])
            avg_predicted_ndcg = np.mean([m['predicted_ndcg'] for m in all_metrics])
        else:
            avg_dcg = avg_ndcg = avg_precision = avg_ratio = avg_predicted_ndcg = 0.0
        
        results = {
            'true_methodology_performance': {
                'avg_dcg': avg_dcg,
                'avg_ndcg': avg_ndcg,
                'avg_precision': avg_precision,
                'avg_ratio_of_ratings': avg_ratio,
                'avg_predicted_ndcg': avg_predicted_ndcg,
                'queries_evaluated': len(all_metrics),
                'weight_selections': {
                    'mean': np.mean(weight_selections) if weight_selections else 0,
                    'std': np.std(weight_selections) if weight_selections else 0,
                    'distribution': pd.Series(weight_selections).value_counts().to_dict() if weight_selections else {}
                }
            },
            'prediction_samples': ndcg_prediction_details[:5],  # Show first 5 for analysis
            'evaluation_config': {
                'test_queries': len(test_queries_with_ratings),
                'model_type': model_dict['model_type'],
                'target': model_dict.get('target', 'unknown'),
                'methodology': model_dict.get('methodology', 'unknown'),
                'features_used': len(model_dict['feature_columns'])
            }
        }
        
        # Compare with static weights if requested
        if compare_with_static:
            logger.info("Comparing with static weight baselines...")
            
            static_weights = [0.1, 0.3, 0.5, 0.7, 0.9]  # Key static weights
            static_results = {}
            
            for static_weight in static_weights:
                logger.info(f"Testing static weight {static_weight}")
                static_metrics = self._evaluate_static_weight(
                    test_queries_with_ratings, reference, static_weight
                )
                static_results[f'static_{static_weight}'] = static_metrics
            
            results['static_baselines'] = static_results
            
            # Calculate improvements
            baseline_ndcg = static_results['static_0.5']['avg_ndcg']
            improvement = ((avg_ndcg - baseline_ndcg) / baseline_ndcg * 100) if baseline_ndcg > 0 else 0
            results['improvement_over_static_0.5'] = improvement
            
            # Find best static weight
            best_static = max(static_results.items(), key=lambda x: x[1]['avg_ndcg'])
            best_static_ndcg = best_static[1]['avg_ndcg']
            improvement_over_best = ((avg_ndcg - best_static_ndcg) / best_static_ndcg * 100) if best_static_ndcg > 0 else 0
            results['improvement_over_best_static'] = improvement_over_best
            results['best_static_weight'] = best_static[0]
            
            logger.info(f"TRUE methodology NDCG: {avg_ndcg:.4f}")
            logger.info(f"Best static NDCG: {best_static_ndcg:.4f} ({best_static[0]})")
            logger.info(f"Improvement: {improvement_over_best:+.2f}%")
        
        return results
    
    def _evaluate_static_weight(self, test_queries: List[str], reference: Dict, static_weight: float) -> Dict:
        """Evaluate static weight baseline"""
        
        all_metrics = []
        lexical_weight = round(1.0 - static_weight, 2)
        
        for query_string in tqdm(test_queries, desc=f"Static {static_weight}", leave=False):
            if query_string not in reference:
                continue
                
            try:
                # Execute search with static weight
                search_results = self._execute_hybrid_search(query_string, lexical_weight, static_weight)
                
                if not search_results.empty:
                    # Merge with ratings and calculate metrics
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
                        # Calculate O19S metrics
                        query_metrics = {
                            'dcg': metrics.dcg_at_10(df_with_ratings),
                            'ndcg': metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string]),
                            'prec@10': metrics.precision_at_k(df_with_ratings),
                            'ratio_of_ratings': metrics.ratio_of_ratings(df_with_ratings)
                        }
                        all_metrics.append(query_metrics)
                
            except Exception as e:
                logger.warning(f"Static evaluation failed for query '{query_string[:50]}...': {e}")
                continue
        
        # Average metrics
        if all_metrics:
            avg_dcg = np.mean([m['dcg'] for m in all_metrics])
            avg_ndcg = np.mean([m['ndcg'] for m in all_metrics])
            avg_precision = np.mean([m['prec@10'] for m in all_metrics])
            avg_ratio = np.mean([m['ratio_of_ratings'] for m in all_metrics])
        else:
            avg_dcg = avg_ndcg = avg_precision = avg_ratio = 0.0
        
        return {
            'avg_dcg': avg_dcg,
            'avg_ndcg': avg_ndcg,
            'avg_precision': avg_precision,
            'avg_ratio_of_ratings': avg_ratio,
            'queries_evaluated': len(all_metrics),
            'static_weight': static_weight
        }
    
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
                "description": "O19S true methodology evaluation",
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
            
            # Convert to DataFrame format
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
            
        # Merge on product_id = docid
        merged = search_results.merge(
            reference_ratings,
            left_on='product_id',
            right_on='docid',
            how='left'
        )
        
        # Fill missing ratings with 0
        merged['rating'] = merged['rating'].fillna(0)
        
        return merged[['position', 'rating', 'product_id', 'relevance']]


def print_evaluation_results(results: Dict):
    """Print O19S TRUE methodology evaluation results"""
    
    print("\n" + "="*70)
    print("O19S TRUE METHODOLOGY EVALUATION RESULTS")
    print("="*70)
    
    config = results['evaluation_config']
    print(f"Test queries evaluated: {config['test_queries']}")
    print(f"Model type: {config['model_type']}")
    print(f"Target: {config['target']}")
    print(f"Methodology: {config['methodology']}")
    print(f"Features used: {config['features_used']}")
    
    true_perf = results['true_methodology_performance']
    print("\nTRUE METHODOLOGY PERFORMANCE:")
    print(f"  Average DCG: {true_perf['avg_dcg']:.2f}")
    print(f"  Average NDCG: {true_perf['avg_ndcg']:.4f}")
    print(f"  Average Precision@10: {true_perf['avg_precision']:.4f}")
    print(f"  Ratio of Ratings: {true_perf['avg_ratio_of_ratings']:.4f}")
    print(f"  Average Predicted NDCG: {true_perf['avg_predicted_ndcg']:.4f}")
    print(f"  Queries evaluated: {true_perf['queries_evaluated']}")
    
    weight_stats = true_perf['weight_selections']
    print("\nWEIGHT SELECTION STATISTICS:")
    print(f"  Mean selected weight: {weight_stats['mean']:.3f}")
    print(f"  Weight std: {weight_stats['std']:.3f}")
    print(f"  Weight distribution:")
    for weight, count in sorted(weight_stats['distribution'].items()):
        print(f"    {weight}: {count} queries")
    
    if 'prediction_samples' in results:
        print("\nSAMPLE PREDICTIONS (first 5 queries):")
        for sample in results['prediction_samples']:
            print(f"  Query: '{sample['query']}...'")
            print(f"    Selected weight: {sample['selected_weight']}")
            print(f"    Predicted NDCG: {sample['predicted_ndcg']:.4f}")
            print(f"    All predictions: {', '.join([f'{w}:{n:.3f}' for w, n in sorted(sample['all_predictions'].items())])}")
    
    if 'static_baselines' in results:
        print("\nSTATIC WEIGHT BASELINES:")
        for name, metrics in sorted(results['static_baselines'].items()):
            weight = metrics['static_weight']
            print(f"  Static {weight}: NDCG {metrics['avg_ndcg']:.4f}")
        
        print(f"\nBest static weight: {results.get('best_static_weight', 'N/A')}")
        print(f"Improvement over static 0.5: {results.get('improvement_over_static_0.5', 0):+.2f}%")
        print(f"Improvement over best static: {results.get('improvement_over_best_static', 0):+.2f}%")
    
    print("="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate O19S TRUE methodology (predict NDCG for all weights, select best)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This implements the TRUE O19S evaluation methodology where:
- For each query, predict NDCG for ALL weights [0.1-0.9]
- Select the weight with highest predicted NDCG
- Execute search with that weight and measure actual performance

Examples:
  # Full evaluation
  python3 %(prog)s --host your-cluster.com --port 80 --model-id MODEL_ID --model-file model.pkl
  
  # Fast development (sampling)
  python3 %(prog)s --host your-cluster.com --port 80 --model-id MODEL_ID --model-file model.pkl --sample-size 100
        """
    )
    
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products', help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--model-file', required=True, help='Path to trained model pkl file')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to O19S ratings.csv file')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of test queries to sample (default: None = use all)')
    parser.add_argument('--no-static-comparison', action='store_true',
                       help='Skip comparison with static weights')
    parser.add_argument('--output', default='o19s_true_methodology_results.json',
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
    model_dict = evaluator.load_model(args.model_file)
    
    # Evaluate with true methodology
    results = evaluator.evaluate_true_methodology(
        model_dict=model_dict,
        o19s_data_path=args.o19s_data,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        compare_with_static=not args.no_static_comparison
    )
    
    # Print results
    print_evaluation_results(results)
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Evaluation results saved to {args.output}")


if __name__ == "__main__":
    main()
