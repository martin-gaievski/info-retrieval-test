#!/usr/bin/env python3
"""
O19S-Compatible Dynamic Weight Predictor Evaluation

Evaluates dynamic weight prediction models using O19S methodology:
1. Loads trained O19S dynamic model from pkl file
2. Uses ALL O19S test split queries (no sampling for O19S compliance)
3. Predicts weights and evaluates using O19S metrics
4. Compares dynamic vs static weight performance

Author: Dynamic Hybrid Search Team
Version: 1.0.0
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

# Evaluation-only feature extractor (avoids API calls)
from feature_extractor_enhanced_evaluation import EnhancedEvaluationFeatureExtractor

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class O19SDynamicWeightEvaluator:
    """Evaluate dynamic weight predictor using O19S methodology exactly"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None):
        """Initialize O19S evaluator"""
        
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
        
        # Initialize enhanced evaluation feature extractor (matches training features)
        self.feature_extractor = EnhancedEvaluationFeatureExtractor()
        
        logger.info(f"Initialized O19S evaluator with evaluation-only features for {host}:{port}/{index_name}")
    
    def load_model(self, model_path: str) -> Dict:
        """Load trained O19S dynamic weight model"""
        
        if not Path(model_path).exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
            
        with open(model_path, 'rb') as f:
            model_dict = pickle.load(f)
            
        logger.info(f"Loaded O19S dynamic model from {model_path}")
        logger.info(f"Model type: {model_dict['model_type']}")
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
    
    def evaluate_dynamic_model(self,
                             model_dict: Dict,
                             o19s_data_path: str,
                             ratings_file: str,
                             sample_size: Optional[int] = None,
                             compare_with_static: bool = True) -> Dict:
        """
        Evaluate dynamic weight model using O19S methodology.
        Uses ALL O19S test queries (no sampling for O19S compliance).
        
        Args:
            model_dict: Loaded model dictionary
            o19s_data_path: Path to O19S data
            ratings_file: Path to O19S ratings
            compare_with_static: Whether to compare with static weights
            
        Returns:
            Evaluation results dictionary
        """
        logger.info("Evaluating dynamic weight model using O19S methodology...")
        
        # Load O19S data
        df_train, df_test, df_ratings = self.load_o19s_data(o19s_data_path, ratings_file)
        
        # Create reference dictionary
        reference = {query: df for query, df in df_ratings.groupby("query")}
        logger.info(f"Created reference for {len(reference)} queries")
        
        # Use ALL O19S test queries by default, or sample for development
        test_queries = df_test['query_string'].tolist()
        if sample_size and sample_size < len(test_queries):
            np.random.seed(42)
            test_queries = np.random.choice(test_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(test_queries)} test queries for development")
            logger.warning("⚠️  Using sampling - results may not match O19S exactly!")
        else:
            logger.info(f"Using all {len(test_queries)} O19S test queries (full O19S compliance)")
        
        # Filter to queries that have ratings
        test_queries_with_ratings = [q for q in test_queries if q in reference]
        logger.info(f"Test queries with ratings: {len(test_queries_with_ratings)}")
        
        # Evaluate dynamic weights
        logger.info("Evaluating dynamic weight predictions...")
        dynamic_metrics = self._evaluate_dynamic_weights(
            test_queries_with_ratings, reference, model_dict
        )
        
        results = {
            'dynamic_performance': dynamic_metrics,
            'evaluation_config': {
                'test_queries': len(test_queries_with_ratings),
                'model_type': model_dict['model_type'],
                'features_used': len(model_dict['feature_columns'])
            }
        }
        
        # Compare with static weights if requested
        if compare_with_static:
            logger.info("Comparing with static weight baselines...")
            
            # Test full O19S static weight range (0.1-0.9)
            static_weights = [round(0.1 + i * 0.1, 1) for i in range(9)]  # 0.1, 0.2, ..., 0.9
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
            improvement = ((dynamic_metrics['avg_ndcg'] - baseline_ndcg) / baseline_ndcg * 100) if baseline_ndcg > 0 else 0
            results['improvement_over_static'] = improvement
            
            logger.info(f"Dynamic vs Static (0.5) NDCG improvement: {improvement:.2f}%")
        
        return results
    
    def _evaluate_dynamic_weights(self, test_queries: List[str], reference: Dict, model_dict: Dict) -> Dict:
        """Evaluate dynamic weight predictions"""
        
        model = model_dict['model']
        scaler = model_dict['scaler']
        feature_columns = model_dict['feature_columns']
        
        all_metrics = []
        queries_evaluated = 0
        weight_predictions = []
        
        for query_string in tqdm(test_queries, desc="Dynamic evaluation"):
            if query_string not in reference:
                continue
                
            try:
                # Extract O19S corpus-aware features (must match training)
                query_features = self.feature_extractor.extract_features(query_string)
                
                # Prepare features for prediction
                feature_values = [query_features.get(col, 0) for col in feature_columns]
                X = np.array([feature_values])
                X_scaled = scaler.transform(X)
                
                # Predict optimal weight
                predicted_weight = float(model.predict(X_scaled)[0])
                predicted_weight = np.clip(predicted_weight, 0.1, 0.9)  # O19S weight range
                
                weight_predictions.append(predicted_weight)
                
                # Evaluate with predicted weight
                lexical_weight = round(1.0 - predicted_weight, 2)
                search_results = self._execute_hybrid_search(query_string, lexical_weight, predicted_weight)
                
                if not search_results.empty:
                    # Merge with ratings and calculate O19S metrics
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
                        queries_evaluated += 1
                
            except Exception as e:
                logger.warning(f"Dynamic evaluation failed for query '{query_string}': {e}")
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
            'queries_evaluated': queries_evaluated,
            'weight_predictions': {
                'mean': np.mean(weight_predictions) if weight_predictions else 0,
                'std': np.std(weight_predictions) if weight_predictions else 0,
                'min': np.min(weight_predictions) if weight_predictions else 0,
                'max': np.max(weight_predictions) if weight_predictions else 0
            }
        }
    
    def _evaluate_static_weight(self, test_queries: List[str], reference: Dict, static_weight: float) -> Dict:
        """Evaluate static weight baseline"""
        
        all_metrics = []
        queries_evaluated = 0
        lexical_weight = round(1.0 - static_weight, 2)
        
        for query_string in tqdm(test_queries, desc=f"Static {static_weight}", leave=False):
            if query_string not in reference:
                continue
                
            try:
                # Execute search with static weight
                search_results = self._execute_hybrid_search(query_string, lexical_weight, static_weight)
                
                if not search_results.empty:
                    # Merge with ratings and calculate O19S metrics
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
                        queries_evaluated += 1
                
            except Exception as e:
                logger.warning(f"Static evaluation failed for query '{query_string}': {e}")
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
            'queries_evaluated': queries_evaluated,
            'static_weight': static_weight
        }
    
    def _extract_query_features(self, query_string: str) -> Dict[str, float]:
        """Extract basic query features (matching training)"""
        words = query_string.split()
        
        return {
            'query_length': len(words),
            'avg_word_length': np.mean([len(w) for w in words]) if words else 0,
            'max_word_length': max([len(w) for w in words]) if words else 0,
            'num_stopwords': sum(1 for w in words if w.lower() in ['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']),
            'has_numbers': float(any(c.isdigit() for c in query_string)),
            'has_quotes': float('"' in query_string or "'" in query_string),
            'has_special_chars': float(any(c in query_string for c in ['!', '?', '$', '%', '&'])),
            'query_specificity': len(set(words)) / len(words) if words else 0
        }
    
    def _execute_hybrid_search(self, query: str, lexical_weight: float, neural_weight: float) -> pd.DataFrame:
        """Execute hybrid search using O19S-compatible approach"""
        
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
            
            # Convert to DataFrame format for O19S metrics
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
        
        # Fill missing ratings with 0
        merged['rating'] = merged['rating'].fillna(0)
        
        return merged[['position', 'rating', 'product_id', 'relevance']]


def print_evaluation_results(results: Dict):
    """Print O19S evaluation results"""
    
    print("\n" + "="*70)
    print("O19S DYNAMIC WEIGHT EVALUATION RESULTS")
    print("="*70)
    
    config = results['evaluation_config']
    print(f"Test queries evaluated: {config['test_queries']}")
    print(f"Model type: {config['model_type']}")
    print(f"Features used: {config['features_used']}")
    
    dynamic = results['dynamic_performance']
    print("\nDYNAMIC WEIGHT PERFORMANCE:")
    print(f"  Average DCG: {dynamic['avg_dcg']:.2f}")
    print(f"  Average NDCG: {dynamic['avg_ndcg']:.4f}")
    print(f"  Average Precision@10: {dynamic['avg_precision']:.4f}")
    print(f"  Ratio of Ratings: {dynamic['avg_ratio_of_ratings']:.4f}")
    print(f"  Queries evaluated: {dynamic['queries_evaluated']}")
    
    weight_pred = dynamic['weight_predictions']
    print("\nWEIGHT PREDICTIONS:")
    print(f"  Mean predicted weight: {weight_pred['mean']:.3f}")
    print(f"  Weight std: {weight_pred['std']:.3f}")
    print(f"  Weight range: [{weight_pred['min']:.2f}, {weight_pred['max']:.2f}]")
    
    if 'static_baselines' in results:
        print("\nSTATIC WEIGHT BASELINES:")
        for name, metrics in results['static_baselines'].items():
            weight = metrics['static_weight']
            print(f"  Static {weight}: NDCG {metrics['avg_ndcg']:.4f}")
        
        print(f"\nIMPROVEMENT OVER STATIC (0.5): {results['improvement_over_static']:+.2f}%")
    
    print("="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate O19S-compatible dynamic weight predictor (uses ALL O19S test queries)",
        formatter_class=argparse.RawDescriptionHelpFormatter
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
                       help='Number of test queries to sample (default: None = use all O19S test queries)')
    parser.add_argument('--no-static-comparison', action='store_true',
                       help='Skip comparison with static weights')
    parser.add_argument('--output', default='o19s_dynamic_evaluation_results.json',
                       help='Output results file')
    
    args = parser.parse_args()
    
    # Initialize evaluator
    evaluator = O19SDynamicWeightEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id
    )
    
    # Load model
    model_dict = evaluator.load_model(args.model_file)
    
    # Evaluate model
    results = evaluator.evaluate_dynamic_model(
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
