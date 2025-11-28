#!/usr/bin/env python3
"""
ESCI Dynamic Weight Predictor Evaluation

Evaluates the trained dynamic weight prediction model on ESCI test set:
1. For each test query, extracts features
2. Tests multiple weights (0.0 to 1.0) and predicts NDCG for each
3. Selects the weight with highest predicted NDCG
4. Executes actual search with that weight and measures real NDCG
5. Compares with static baseline (0.6118)

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
from opensearchpy import OpenSearch

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, '..'))

# Import feature extractor
from esci_feature_extractor import ESCIFeatureExtractor

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s', 
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO)
logger = logging.getLogger(__name__)


class ESCIDynamicEvaluator:
    """Evaluate dynamic weight prediction on ESCI dataset"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None):
        """Initialize evaluator"""
        
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
        
        # Initialize feature extractor
        self.feature_extractor = ESCIFeatureExtractor(
            client=self.client,
            index_name=index_name,
            corpus_field='product_title'
        )
        
        logger.info(f"Initialized ESCI evaluator for {host}:{port}/{index_name}")
    
    def load_model(self, model_path: str) -> Dict:
        """Load trained model and metadata"""
        
        if not Path(model_path).exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
            
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
            
        # Load metadata
        metadata_path = model_path.replace('.pkl', '_metadata.json')
        if Path(metadata_path).exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        else:
            metadata = {}
            
        logger.info(f"Loaded model from {model_path}")
        logger.info(f"Model type: {metadata.get('model_type', 'Ridge')}")
        logger.info(f"Features: {metadata.get('n_features', 20)}")
        logger.info(f"Training samples: {metadata.get('training_samples', 'Unknown')}")
        
        return model_data, metadata
    
    def load_esci_test_data(self, queries_file: str, ratings_file: str) -> Tuple[pd.DataFrame, Dict]:
        """Load ESCI test queries and ratings"""
        
        # Load queries
        df_queries = pd.read_csv(queries_file)
        logger.info(f"Loaded {len(df_queries)} test queries")
        
        # The query column might be named 'query' or 'query_string'
        query_column = 'query_string' if 'query_string' in df_queries.columns else 'query'
        
        # Load ratings - CSV format with columns: query, docid, rating, idx
        df_ratings = pd.read_csv(ratings_file, sep="\t", names=['query', 'docid', 'rating', 'idx'])
        
        # Ratings are already in 0-3 range, no conversion needed
        logger.info(f"Loaded {len(df_ratings)} ratings")
        
        # Create reference dictionary using query text as key
        reference = {}
        for query_text, group in df_ratings.groupby('query'):
            # Make sure this query is in our test set
            if query_text in df_queries[query_column].values:
                reference[query_text] = group[['docid', 'rating']].copy()
                reference[query_text].rename(columns={'docid': 'product_id'}, inplace=True)
        
        logger.info(f"Created reference for {len(reference)} queries with ratings")
        
        return df_queries, reference
    
    def predict_optimal_weight(self, query: str, model_data: Dict) -> Tuple[float, List[Dict]]:
        """
        Predict optimal weight for a query by testing multiple weights
        
        Returns:
            optimal_weight: Weight with highest predicted NDCG
            predictions: List of predictions for all tested weights
        """
        
        model = model_data['model']
        scaler = model_data.get('scaler')
        
        # Define feature names matching training
        feature_names = [
            'f_0_neuralness', 'f_2_query_length', 'f_3_has_numbers', 'f_4_has_special_char', 
            'f_5_has_punctuation_at_end', 'f_6_unique_terms_ratio', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
            'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
            'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
            'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
            'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
        ]
        
        # Test weights from 0.0 to 1.0 in steps of 0.1
        test_weights = [round(i * 0.1, 1) for i in range(11)]  # 0.0, 0.1, ..., 1.0
        predictions = []
        
        for weight in test_weights:
            # Extract features including the weight
            features = self.feature_extractor.extract_features(query, weight)
            
            # Prepare feature vector as a list
            feature_values = [
                features['f_0_neuralness'],  # Weight feature  
                features['f_2_query_length'],
                features['f_3_has_numbers'],
                features['f_4_has_special_char'],
                features['f_5_has_punctuation_at_end'],
                features['f_6_unique_terms_ratio'],
                features['f_7_capital_letters_ratio'],
                features['f_8_stopwords_ratio'],
                features['f_14_max_document_frequency'],
                features['f_15_min_document_frequency'],
                features['f_16_total_document_frequency'],
                features['f_17_average_document_frequency'],
                features['f_18_variance_document_frequency'],
                features['f_19_std_dev_document_frequency'],
                features['f_20_max_inverse_document_frequency'],
                features['f_21_min_inverse_document_frequency'],
                features['f_22_total_inverse_document_frequency'],
                features['f_23_average_inverse_document_frequency'],
                features['f_24_variance_inverse_document_frequency'],
                features['f_25_std_dev_inverse_document_frequency']
            ]
            
            # Apply selective scaling if scaler exists (matching training approach)
            if scaler is not None:
                # Create DataFrame with all features
                feature_df = pd.DataFrame([feature_values], columns=feature_names)
                
                # Separate weight from other features
                weight_value = feature_df['f_0_neuralness'].values[0]
                other_features_df = feature_df.drop('f_0_neuralness', axis=1)
                
                # Scale only the non-weight features
                other_features_scaled = scaler.transform(other_features_df)
                
                # Recombine: unscaled weight + scaled other features
                scaled_values = np.hstack([[weight_value], other_features_scaled[0]])
                
                # Create DataFrame with all features for prediction
                feature_df_final = pd.DataFrame([scaled_values], columns=feature_names)
            else:
                # No scaler, use features as is
                feature_df_final = pd.DataFrame([feature_values], columns=feature_names)
            
            # Predict NDCG
            predicted_ndcg = model.predict(feature_df_final)[0]
            
            predictions.append({
                'weight': weight,
                'predicted_ndcg': predicted_ndcg
            })
        
        # Find weight with highest predicted NDCG
        best_prediction = max(predictions, key=lambda x: x['predicted_ndcg'])
        optimal_weight = best_prediction['weight']
        
        return optimal_weight, predictions
    
    def execute_hybrid_search(self, 
                             query: str, 
                             neural_weight: float, 
                             normalization: str = "l2",
                             combination: str = "arithmetic_mean",
                             size: int = 10) -> pd.DataFrame:
        """
        Execute hybrid search and return results
        
        Args:
            query: Search query
            neural_weight: Weight for neural search (0-1)
            normalization: Normalization technique ('l2' or 'min_max')
            combination: Combination technique ('arithmetic_mean', 'geometric_mean', 'harmonic_mean')
            size: Number of results to retrieve (should match ndcg_k for efficiency)
        """
        
        lexical_weight = round(1.0 - neural_weight, 2)
        
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        # Use dynamic normalization and combination techniques
        payload = {
            "_source": {"excludes": ["title_embedding"]},
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "neural": {
                                "title_embedding": {
                                    "query_text": query,
                                    "model_id": self.model_id,
                                    "k": 200
                                }
                            }
                        },
                        {
                            "multi_match": {
                                "query": query,
                                "type": "best_fields",
                                "operator": "and",
                                "fields": [
                                    "product_id^10",
                                    "product_title"
                                ]
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": f"ESCI evaluation with {normalization}/{combination}",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": normalization},
                            "combination": {
                                "technique": combination,
                                "parameters": {"weights": [neural_weight, lexical_weight]}
                            }
                        }
                    }
                ]
            },
            "size": size
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            
            # Convert to DataFrame
            rows = []
            for position, hit in enumerate(result['hits']['hits']):
                rows.append({
                    'product_id': hit['_id'],
                    'position': position + 1,
                    'score': hit['_score']
                })
            
            return pd.DataFrame(rows) if rows else pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Search failed for query '{query}': {e}")
            return pd.DataFrame()
    
    def calculate_ndcg(self, results: pd.DataFrame, reference: pd.DataFrame, k: int = 10) -> float:
        """Calculate NDCG@k"""
        
        if results.empty or reference.empty:
            return 0.0
        
        # Merge results with reference ratings
        merged = results.merge(reference, on='product_id', how='left')
        merged['rating'] = merged['rating'].fillna(0)
        
        # Calculate DCG
        dcg = 0.0
        for i in range(min(k, len(merged))):
            rel = merged.iloc[i]['rating']
            dcg += (2**rel - 1) / np.log2(i + 2)
        
        # Calculate ideal DCG
        ideal_ratings = reference['rating'].nlargest(k).values
        idcg = 0.0
        for i, rel in enumerate(ideal_ratings):
            idcg += (2**rel - 1) / np.log2(i + 2)
        
        # Return NDCG
        return dcg / idcg if idcg > 0 else 0.0
    
    def evaluate_model(self, 
                       model_data: Dict,
                       queries_file: str,
                       ratings_file: str,
                       sample_size: Optional[int] = None,
                       static_weight: float = 0.7,
                       normalization_techniques: List[str] = None,
                       combination_techniques: List[str] = None,
                       ndcg_k: int = 10) -> Dict:
        """
        Evaluate dynamic weight prediction model
        
        Args:
            model_data: Loaded model and scaler
            queries_file: Path to test queries CSV
            ratings_file: Path to test ratings TSV
            sample_size: Number of queries to evaluate (None = all)
            static_weight: Static baseline weight to compare against
            normalization_techniques: List of normalization techniques to test
            combination_techniques: List of combination techniques to test
            ndcg_k: K value for NDCG@k calculation (default: 10)
            
        Returns:
            Evaluation results dictionary
        """
        
        # Default techniques if not specified
        if normalization_techniques is None:
            normalization_techniques = ["l2", "min_max"]
        if combination_techniques is None:
            combination_techniques = ["arithmetic_mean", "geometric_mean", "harmonic_mean"]
        
        logger.info("Starting ESCI dynamic weight evaluation...")
        logger.info(f"Testing normalization: {normalization_techniques}")
        logger.info(f"Testing combination: {combination_techniques}")
        logger.info(f"Calculating NDCG@{ndcg_k}")
        
        # Load test data
        df_queries, reference = self.load_esci_test_data(queries_file, ratings_file)
        
        # Sample queries if requested
        test_queries = list(reference.keys())
        if sample_size and sample_size < len(test_queries):
            np.random.seed(42)
            test_queries = np.random.choice(test_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(test_queries)} queries for evaluation")
        else:
            logger.info(f"Evaluating all {len(test_queries)} test queries")
        
        # Store results for each technique combination
        technique_results = {}
        
        for norm in normalization_techniques:
            for comb in combination_techniques:
                technique_key = f"{norm}_{comb}"
                logger.info(f"\nEvaluating with {norm} normalization and {comb} combination...")
                
                # Evaluate dynamic predictions
                dynamic_results = []
                static_results = []
                weight_predictions = []
                
                for query in tqdm(test_queries, desc=f"Evaluating {technique_key}"):
                    if query not in reference:
                        continue
                    
                    try:
                        # Predict optimal weight
                        optimal_weight, predictions = self.predict_optimal_weight(query, model_data)
                        weight_predictions.append(optimal_weight)
                        
                        # Execute search with predicted weight (retrieve only ndcg_k results for efficiency)
                        dynamic_search = self.execute_hybrid_search(
                            query, optimal_weight, 
                            normalization=norm, combination=comb,
                            size=ndcg_k
                        )
                        if not dynamic_search.empty:
                            dynamic_ndcg = self.calculate_ndcg(dynamic_search, reference[query], k=ndcg_k)
                            dynamic_results.append(dynamic_ndcg)
                        
                        # Execute search with static weight (retrieve only ndcg_k results for efficiency)
                        static_search = self.execute_hybrid_search(
                            query, static_weight,
                            normalization=norm, combination=comb,
                            size=ndcg_k
                        )
                        if not static_search.empty:
                            static_ndcg = self.calculate_ndcg(static_search, reference[query], k=ndcg_k)
                            static_results.append(static_ndcg)
                            
                    except Exception as e:
                        logger.warning(f"Failed to evaluate query '{query}': {e}")
                        continue
                
                # Store results for this technique combination
                technique_results[technique_key] = {
                    'normalization': norm,
                    'combination': comb,
                    'dynamic_results': dynamic_results,
                    'static_results': static_results,
                    'weight_predictions': weight_predictions
                }
        
        # Find best technique combination
        best_technique = None
        best_dynamic_ndcg = 0.0
        
        results = {
            'technique_combinations': {},
            'best_combination': None,
            'ndcg_k': ndcg_k,
            'esci_baseline_ndcg': 0.268  # From ESCI baseline results
        }
        
        for technique_key, tech_data in technique_results.items():
            dynamic_results = tech_data['dynamic_results']
            static_results = tech_data['static_results']
            weight_predictions = tech_data['weight_predictions']
            
            # Calculate metrics
            avg_dynamic_ndcg = np.mean(dynamic_results) if dynamic_results else 0.0
            avg_static_ndcg = np.mean(static_results) if static_results else 0.0
            
            # Track best combination
            if avg_dynamic_ndcg > best_dynamic_ndcg:
                best_dynamic_ndcg = avg_dynamic_ndcg
                best_technique = technique_key
            
            # Calculate improvement
            improvement = ((avg_dynamic_ndcg - avg_static_ndcg) / avg_static_ndcg * 100) if avg_static_ndcg > 0 else 0
            
            results['technique_combinations'][technique_key] = {
                'normalization': tech_data['normalization'],
                'combination': tech_data['combination'],
                'dynamic_performance': {
                    'avg_ndcg': avg_dynamic_ndcg,
                    'std_ndcg': np.std(dynamic_results) if dynamic_results else 0.0,
                    'queries_evaluated': len(dynamic_results)
                },
                'static_baseline': {
                    'weight': static_weight,
                    'avg_ndcg': avg_static_ndcg,
                    'std_ndcg': np.std(static_results) if static_results else 0.0,
                    'queries_evaluated': len(static_results)
                },
                'weight_predictions': {
                    'mean': np.mean(weight_predictions) if weight_predictions else 0.0,
                    'std': np.std(weight_predictions) if weight_predictions else 0.0,
                    'min': np.min(weight_predictions) if weight_predictions else 0.0,
                    'max': np.max(weight_predictions) if weight_predictions else 0.0,
                    'distribution': np.histogram(weight_predictions, bins=11, range=(0, 1))[0].tolist() if weight_predictions else []
                },
                'improvement': {
                    'percentage': improvement,
                    'absolute': avg_dynamic_ndcg - avg_static_ndcg
                }
            }
        
        # Set best combination
        if best_technique:
            results['best_combination'] = best_technique
        
        return results


def print_results(results: Dict):
    """Print evaluation results"""
    
    print("\n" + "="*70)
    print("ESCI DYNAMIC WEIGHT PREDICTION EVALUATION RESULTS")
    print("="*70)
    
    ndcg_k = results.get('ndcg_k', 10)
    print(f"\nMetric: NDCG@{ndcg_k}")
    
    # Print results for each technique combination
    print("\nRESULTS BY TECHNIQUE COMBINATION:")
    print("-" * 50)
    
    for technique_key, tech_data in results['technique_combinations'].items():
        print(f"\n{technique_key.upper()}:")
        print(f"  Normalization: {tech_data['normalization']}")
        print(f"  Combination: {tech_data['combination']}")
        
        dynamic = tech_data['dynamic_performance']
        static = tech_data['static_baseline']
        
        print(f"  Dynamic NDCG: {dynamic['avg_ndcg']:.4f} ± {dynamic['std_ndcg']:.4f}")
        print(f"  Static NDCG: {static['avg_ndcg']:.4f} ± {static['std_ndcg']:.4f}")
        print(f"  Improvement: {tech_data['improvement']['percentage']:+.2f}%")
    
    # Print best combination
    if results['best_combination']:
        print("\n" + "="*50)
        print("BEST PERFORMING COMBINATION")
        print("="*50)
        
        best_key = results['best_combination']
        best_data = results['technique_combinations'][best_key]
        
        print(f"\nTechnique: {best_key}")
        print(f"  Normalization: {best_data['normalization']}")
        print(f"  Combination: {best_data['combination']}")
        
        dynamic = best_data['dynamic_performance']
        print(f"\nDYNAMIC WEIGHT PERFORMANCE:")
        print(f"  Average NDCG: {dynamic['avg_ndcg']:.4f} ± {dynamic['std_ndcg']:.4f}")
        print(f"  Queries evaluated: {dynamic['queries_evaluated']}")
        
        static = best_data['static_baseline']
        print(f"\nSTATIC BASELINE (weight={static['weight']}):")
        print(f"  Average NDCG: {static['avg_ndcg']:.4f} ± {static['std_ndcg']:.4f}")
        print(f"  Queries evaluated: {static['queries_evaluated']}")
        
        weight_pred = best_data['weight_predictions']
        print(f"\nWEIGHT PREDICTIONS:")
        print(f"  Mean: {weight_pred['mean']:.3f} ± {weight_pred['std']:.3f}")
        print(f"  Range: [{weight_pred['min']:.2f}, {weight_pred['max']:.2f}]")
        
        improvement = best_data['improvement']
        print(f"\nIMPROVEMENT OVER STATIC:")
        print(f"  Relative: {improvement['percentage']:+.2f}%")
        print(f"  Absolute: {improvement['absolute']:+.4f}")
        
        print(f"\nCOMPARISON WITH ESCI BASELINE:")
        esci_baseline = results['esci_baseline_ndcg']
        dynamic_vs_baseline = ((dynamic['avg_ndcg'] - esci_baseline) / esci_baseline * 100)
        print(f"  ESCI Baseline NDCG: {esci_baseline:.4f}")
        print(f"  Dynamic vs Baseline: {dynamic_vs_baseline:+.2f}%")
    
    print("="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate ESCI dynamic weight prediction model"
    )
    
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products', help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--model-file', default='esci_20features_model_small.pkl',
                       help='Path to trained model pkl file')
    parser.add_argument('--queries-file', default='datasets/esci/esci_queries_300.csv',
                       help='Path to test queries CSV')
    parser.add_argument('--ratings-file', default='datasets/esci/esci_ratings_300.tsv',
                       help='Path to test ratings TSV')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of queries to evaluate (default: all)')
    parser.add_argument('--static-weight', type=float, default=0.7,
                       help='Static baseline neural weight (default: 0.7)')
    parser.add_argument('--ndcg-k', type=int, default=10,
                       help='K value for NDCG@k calculation (default: 10)')
    parser.add_argument('--normalization', type=str, default=None,
                       help='Normalization techniques to test (comma-separated: l2,min_max). Default: all techniques')
    parser.add_argument('--combination', type=str, default=None,
                       help='Combination techniques to test (comma-separated: arithmetic_mean,geometric_mean,harmonic_mean). Default: all techniques')
    parser.add_argument('--output', default='esci_dynamic_evaluation_results.json',
                       help='Output results file')
    
    args = parser.parse_args()
    
    # Initialize evaluator
    evaluator = ESCIDynamicEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id
    )
    
    # Load model
    model_data, metadata = evaluator.load_model(args.model_file)
    
    # Parse normalization and combination techniques
    normalization_techniques = None
    combination_techniques = None
    
    if args.normalization:
        normalization_techniques = [n.strip() for n in args.normalization.split(',')]
        logger.info(f"Using specified normalization techniques: {normalization_techniques}")
    
    if args.combination:
        combination_techniques = [c.strip() for c in args.combination.split(',')]
        logger.info(f"Using specified combination techniques: {combination_techniques}")
    
    # Evaluate model
    results = evaluator.evaluate_model(
        model_data=model_data,
        queries_file=args.queries_file,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        static_weight=args.static_weight,
        normalization_techniques=normalization_techniques,
        combination_techniques=combination_techniques,
        ndcg_k=args.ndcg_k
    )
    
    # Print results
    print_results(results)
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
