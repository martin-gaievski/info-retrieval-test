#!/usr/bin/env python3
"""
O19S Query String Model Evaluation

Evaluates the O19S query string features regression model using real OpenSearch cluster:
1. Loads O19S query string model (6 features: 5 query features + weight)
2. Uses O19S methodology: tests all weights 0.1-0.9, selects best predicted NDCG
3. Evaluates on real OpenSearch cluster with hybrid search
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

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class O19SFeatureExtractor:
    """Extract O19S features for both 6-feature and 18-feature models."""
    
    def __init__(self, feature_set: str = "query_string"):
        """
        Initialize feature extractor.
        
        Args:
            feature_set: "query_string" for 6 features or "full" for 18 features
        """
        self.feature_set = feature_set
        
        # O19S feature definitions from training script
        self.query_string_features = [
            'f_0_neuralness', 'f_1_num_of_terms', 'f_2_query_length', 
            'f_3_has_numbers', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio'
        ]
        
        self.full_features = [
            'f_0_neuralness', 'f_2_query_length', 'f_4_has_special_char', 
            'f_5_has_punctuation_at_end', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio', 
            'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency', 
            'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency', 
            'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency', 
            'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
        ]
    
    def extract_features(self, query: str) -> Dict[str, float]:
        """Extract features based on the configured feature set."""
        
        if self.feature_set == "query_string":
            return self._extract_query_string_features(query)
        elif self.feature_set == "full":
            return self._extract_full_features(query)
        else:
            raise ValueError(f"Unknown feature_set: {self.feature_set}")
    
    def _extract_query_string_features(self, query: str) -> Dict[str, float]:
        """Extract 6 query string features (excluding f_0_neuralness which is weight)."""
        words = query.split()
        
        features = {
            'f_1_num_of_terms': len(words),
            'f_2_query_length': len(query),
            'f_3_has_numbers': float(any(c.isdigit() for c in query)),
            'f_7_capital_letters_ratio': sum(1 for c in query if c.isupper()) / len(query) if query else 0,
            'f_8_stopwords_ratio': self._calculate_stopwords_ratio(words)
        }
        
        return features
    
    def _extract_full_features(self, query: str) -> Dict[str, float]:
        """Extract 18 full features (excluding f_0_neuralness which is weight)."""
        words = query.split()
        
        # Start with query features
        features = {
            'f_2_query_length': len(query),
            'f_4_has_special_char': float(any(c in query for c in '!@#$%^&*()_+-=[]{}|;:,.<>?')),
            'f_5_has_punctuation_at_end': float(query.endswith(('.', '!', '?', ';', ':')) if query else False),
            'f_7_capital_letters_ratio': sum(1 for c in query if c.isupper()) / len(query) if query else 0,
            'f_8_stopwords_ratio': self._calculate_stopwords_ratio(words)
        }
        
        # Add document frequency features (mock values - would need corpus analysis)
        # For evaluation, we'll use reasonable defaults based on query characteristics
        doc_freq_features = self._estimate_document_frequency_features(words)
        features.update(doc_freq_features)
        
        # Add inverse document frequency features (mock values)
        idf_features = self._estimate_idf_features(words)
        features.update(idf_features)
        
        return features
    
    def _calculate_stopwords_ratio(self, words: List[str]) -> float:
        """Calculate ratio of stopwords in query."""
        stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were'}
        if not words:
            return 0.0
        stopword_count = sum(1 for w in words if w.lower() in stopwords)
        return stopword_count / len(words)
    
    def _estimate_document_frequency_features(self, words: List[str]) -> Dict[str, float]:
        """Estimate document frequency features (mock implementation)."""
        # In real implementation, these would be calculated from corpus
        # For now, use reasonable estimates based on word characteristics
        
        if not words:
            return {f'f_{i}_': 0.0 for i in range(14, 20)}
        
        # Mock document frequencies based on word length and commonality
        word_lengths = [len(w) for w in words]
        common_words = sum(1 for w in words if len(w) <= 4)  # Short words are often common
        
        return {
            'f_14_max_document_frequency': max(word_lengths) * 1000,  # Mock scaling
            'f_15_min_document_frequency': min(word_lengths) * 100,
            'f_16_total_document_frequency': sum(word_lengths) * 500,
            'f_17_average_document_frequency': np.mean(word_lengths) * 300,
            'f_18_variance_document_frequency': np.var(word_lengths) * 200,
            'f_19_std_dev_document_frequency': np.std(word_lengths) * 150
        }
    
    def _estimate_idf_features(self, words: List[str]) -> Dict[str, float]:
        """Estimate inverse document frequency features (mock implementation)."""
        # In real implementation, these would be calculated from corpus
        
        if not words:
            return {f'f_{i}_': 0.0 for i in range(20, 26)}
        
        # Mock IDF values - typically inverse of document frequency
        word_lengths = [len(w) for w in words]
        
        return {
            'f_20_max_inverse_document_frequency': 10.0 / max(word_lengths) if word_lengths else 1.0,
            'f_21_min_inverse_document_frequency': 10.0 / min(word_lengths) if word_lengths else 1.0,
            'f_22_total_inverse_document_frequency': 50.0 / sum(word_lengths) if word_lengths else 1.0,
            'f_23_average_inverse_document_frequency': 10.0 / np.mean(word_lengths) if word_lengths else 1.0,
            'f_24_variance_inverse_document_frequency': 5.0 / (np.var(word_lengths) + 0.1),
            'f_25_std_dev_inverse_document_frequency': 5.0 / (np.std(word_lengths) + 0.1)
        }
    
    def get_expected_features(self) -> int:
        """Get expected number of features (excluding weight)."""
        if self.feature_set == "query_string":
            return 5  # 6 total - 1 weight = 5 features
        elif self.feature_set == "full":
            return 17  # 18 total - 1 weight = 17 features
        else:
            return 0


class O19SQueryStringEvaluator:
    """Evaluate O19S query string model using real OpenSearch cluster"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None):
        """Initialize O19S query string evaluator"""
        
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
        
        # Will be initialized when we know the model type
        self.feature_extractor = None
        
        logger.info(f"Initialized O19S evaluator for {host}:{port}/{index_name}")
    
    def load_model(self, model_path: str, feature_set: str = "auto"):
        """Load O19S model and configure feature extractor"""
        
        if not Path(model_path).exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
            
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
            
        logger.info(f"Loaded O19S model from {model_path}")
        logger.info(f"Model type: {type(model)}")
        logger.info(f"Expected features: {model.n_features_in_}")
        
        # Auto-detect feature set based on model
        if feature_set == "auto":
            if model.n_features_in_ == 6:
                feature_set = "query_string"
                logger.info("Auto-detected: 6-feature query string model")
            elif model.n_features_in_ == 18:
                feature_set = "full"
                logger.info("Auto-detected: 18-feature full model")
            else:
                raise ValueError(f"Unknown model size: {model.n_features_in_} features")
        
        # Initialize feature extractor
        self.feature_extractor = O19SFeatureExtractor(feature_set=feature_set)
        logger.info(f"Initialized {feature_set} feature extractor")
        
        return model
    
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
    
    def predict_best_weight(self, model, query: str, weights_to_test: List[float] = None) -> Tuple[float, float, Dict[str, float]]:
        """
        Predict best weight using O19S methodology with configurable features.
        
        Args:
            model: O19S model for prediction
            query: Query string
            weights_to_test: List of weights to test (default: [0.1-0.9])
        
        Returns:
            - best_weight: Weight with highest predicted NDCG
            - best_predicted_ndcg: The predicted NDCG for best weight
            - all_predictions: Dict of weight -> predicted NDCG
        """
        if weights_to_test is None:
            weights_to_test = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        
        # Extract query features (excluding weight)
        query_features = self.feature_extractor.extract_features(query)
        
        all_predictions = {}
        best_weight = 0.5
        best_predicted_ndcg = -1
        
        for weight in weights_to_test:
            # Create feature vector based on feature set - MUST match O19S training order
            if self.feature_extractor.feature_set == "query_string":
                # 6-feature model: O19S training order ['f_0_neuralness', 'f_1_num_of_terms', 'f_2_query_length', 'f_3_has_numbers', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio']
                feature_vector = [
                    weight,  # f_0_neuralness (FIRST in O19S training order)
                    query_features['f_1_num_of_terms'],
                    query_features['f_2_query_length'],
                    query_features['f_3_has_numbers'],
                    query_features['f_7_capital_letters_ratio'],
                    query_features['f_8_stopwords_ratio']
                ]
                feature_names = ['f_0_neuralness', 'f_1_num_of_terms', 'f_2_query_length', 'f_3_has_numbers', 
                               'f_7_capital_letters_ratio', 'f_8_stopwords_ratio']
            elif self.feature_extractor.feature_set == "full":
                # 18-feature model: O19S training order
                feature_vector = [
                    weight,  # f_0_neuralness (first in O19S order)
                    query_features['f_2_query_length'],
                    query_features['f_4_has_special_char'],
                    query_features['f_5_has_punctuation_at_end'],
                    query_features['f_7_capital_letters_ratio'],
                    query_features['f_8_stopwords_ratio'],
                    query_features['f_14_max_document_frequency'],
                    query_features['f_15_min_document_frequency'],
                    query_features['f_16_total_document_frequency'],
                    query_features['f_17_average_document_frequency'],
                    query_features['f_18_variance_document_frequency'],
                    query_features['f_19_std_dev_document_frequency'],
                    query_features['f_20_max_inverse_document_frequency'],
                    query_features['f_21_min_inverse_document_frequency'],
                    query_features['f_22_total_inverse_document_frequency'],
                    query_features['f_23_average_inverse_document_frequency'],
                    query_features['f_24_variance_inverse_document_frequency'],
                    query_features['f_25_std_dev_inverse_document_frequency']
                ]
                feature_names = ['f_0_neuralness', 'f_2_query_length', 'f_4_has_special_char', 
                               'f_5_has_punctuation_at_end', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
                               'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
                               'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
                               'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
                               'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency']
            else:
                raise ValueError(f"Unknown feature set: {self.feature_extractor.feature_set}")
            
            # Create DataFrame with feature names
            import pandas as pd
            feature_df = pd.DataFrame([feature_vector], columns=feature_names)
            
            # Predict NDCG for this weight
            predicted_ndcg = model.predict(feature_df)[0]
            all_predictions[weight] = predicted_ndcg
            
            # Track best weight
            if predicted_ndcg > best_predicted_ndcg:
                best_predicted_ndcg = predicted_ndcg
                best_weight = weight
        
        return best_weight, best_predicted_ndcg, all_predictions
    
    def evaluate_query_string_model(self,
                                   model,
                                   o19s_data_path: str,
                                   ratings_file: str,
                                   sample_size: Optional[int] = None,
                                   compare_with_static: bool = True,
                                   custom_weights: Optional[List[float]] = None) -> Dict:
        """
        Evaluate O19S query string model using real OpenSearch cluster.
        
        Args:
            model: Loaded O19S query string model
            o19s_data_path: Path to O19S data
            ratings_file: Path to O19S ratings
            sample_size: Number of queries to sample (None = all)
            compare_with_static: Whether to compare with static weights
            
        Returns:
            Evaluation results dictionary
        """
        logger.info("Evaluating O19S query string model on real OpenSearch cluster...")
        
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
        logger.info("Evaluating O19S query string model predictions...")
        if custom_weights:
            logger.info(f"Using custom weight range: {custom_weights}")
        dynamic_metrics = self._evaluate_dynamic_weights(
            test_queries_with_ratings, reference, model, custom_weights
        )
        
        # Update config based on actual feature extractor
        feature_count = self.feature_extractor.get_expected_features() + 1  # +1 for weight
        model_description = f"O19S {self.feature_extractor.feature_set.title()} Features ({feature_count} features)"
        
        results = {
            'dynamic_performance': dynamic_metrics,
            'evaluation_config': {
                'test_queries': len(test_queries_with_ratings),
                'model_type': model_description,
                'features_used': feature_count,
                'feature_set': self.feature_extractor.feature_set,
                'model_file': os.path.basename(model_path) if 'model_path' in locals() else 'unknown'
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
            
            logger.info(f"O19S Query String vs Static (0.5) NDCG improvement: {improvement:.2f}%")
        
        return results
    
    def _evaluate_dynamic_weights(self, test_queries: List[str], reference: Dict, model, custom_weights: Optional[List[float]] = None) -> Dict:
        """Evaluate O19S query string model predictions"""
        
        all_metrics = []
        queries_evaluated = 0
        weight_predictions = []
        prediction_details = []
        
        for query_string in tqdm(test_queries, desc="O19S Query String evaluation"):
            if query_string not in reference:
                continue
                
            try:
                # Predict optimal weight using O19S methodology
                predicted_weight, predicted_ndcg, all_predictions = self.predict_best_weight(model, query_string, custom_weights)
                
                weight_predictions.append(predicted_weight)
                prediction_details.append({
                    'query': query_string,
                    'predicted_weight': predicted_weight,
                    'predicted_ndcg': predicted_ndcg,
                    'all_predictions': all_predictions
                })
                
                # Evaluate with predicted weight
                lexical_weight = round(1.0 - predicted_weight, 2)
                search_results = self._execute_hybrid_search(query_string, lexical_weight, predicted_weight)
                
                if not search_results.empty:
                    # Merge with ratings and calculate O19S metrics
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
                        # Calculate O19S metrics with original ESCI ratings
                        query_metrics = {
                            'dcg': metrics.dcg_at_10(df_with_ratings),
                            'ndcg': metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string]),
                            'prec@10': metrics.precision_at_k(df_with_ratings),
                            'ratio_of_ratings': metrics.ratio_of_ratings(df_with_ratings)
                        }
                        all_metrics.append(query_metrics)
                        queries_evaluated += 1
                
            except Exception as e:
                logger.warning(f"O19S query string evaluation failed for query '{query_string}': {e}")
                continue
        
        # Average metrics without rounding for more precision
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
                'max': np.max(weight_predictions) if weight_predictions else 0,
                'distribution': {str(w): weight_predictions.count(w) for w in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]}
            },
            'prediction_details': prediction_details[:10]  # Save first 10 for analysis
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
                        # Calculate O19S metrics with original ESCI ratings
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
                                    "k": 100
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "O19S query string model evaluation",
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
            "size": 100
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
    """Print O19S query string model evaluation results"""
    
    print("\n" + "="*70)
    print("O19S QUERY STRING MODEL EVALUATION RESULTS")
    print("="*70)
    
    config = results['evaluation_config']
    print(f"Test queries evaluated: {config['test_queries']}")
    print(f"Model type: {config['model_type']}")
    print(f"Features used: {config['features_used']}")
    print(f"Model file: {config['model_file']}")
    
    dynamic = results['dynamic_performance']
    print("\nO19S QUERY STRING MODEL PERFORMANCE:")
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
    
    print("\nWeight Distribution:")
    for weight, count in weight_pred['distribution'].items():
        percentage = (count / dynamic['queries_evaluated']) * 100 if dynamic['queries_evaluated'] > 0 else 0
        print(f"  Weight {weight}: {count} queries ({percentage:.1f}%)")
    
    if 'static_baselines' in results:
        print("\nSTATIC WEIGHT BASELINES:")
        for name, metrics in results['static_baselines'].items():
            weight = metrics['static_weight']
            print(f"  Static {weight}: NDCG {metrics['avg_ndcg']:.4f}")
        
        print(f"\nIMPROVEMENT OVER STATIC (0.5): {results['improvement_over_static']:+.2f}%")
    
    print("\nNOTE: This model uses only 6 features (5 query string features + weight)")
    print("It's a simplified version compared to the full 26-feature O19S model.")
    print("="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate O19S query string model on real OpenSearch cluster",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products', help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--model-file', 
                       default='models/regression_model-2025_large_qs_query_string_features-08-28.pkl',
                       help='Path to O19S query string model pkl file')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to O19S ratings.csv file')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of test queries to sample (default: None = use all O19S test queries)')
    parser.add_argument('--no-static-comparison', action='store_true',
                       help='Skip comparison with static weights')
    parser.add_argument('--output', default='o19s_query_string_evaluation_results.json',
                       help='Output results file')
    parser.add_argument('--custom-weights', type=str, default=None,
                       help='Custom weights to test (comma-separated, e.g., "0.2,0.4,0.6,0.8" for faster evaluation)')
    
    args = parser.parse_args()
    
    # Parse custom weights if provided
    custom_weights = None
    if args.custom_weights:
        try:
            custom_weights = [float(w.strip()) for w in args.custom_weights.split(',')]
            logger.info(f"Using custom weights: {custom_weights}")
        except ValueError as e:
            logger.error(f"Invalid custom weights format: {e}")
            sys.exit(1)
    
    # Initialize evaluator
    evaluator = O19SQueryStringEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id
    )
    
    # Load model
    model = evaluator.load_model(args.model_file)
    
    # Evaluate model
    results = evaluator.evaluate_query_string_model(
        model=model,
        o19s_data_path=args.o19s_data,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        compare_with_static=not args.no_static_comparison,
        custom_weights=custom_weights
    )
    
    # Print results
    print_evaluation_results(results)
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Evaluation results saved to {args.output}")


if __name__ == "__main__":
    main()
