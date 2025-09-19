#!/usr/bin/env python3
"""
Evaluate O19S Weight Predictor Model

This script evaluates a trained Ridge regression model that predicts optimal weight directly:
1. Uses 17 features (5 query + 12 real-time corpus features) - NO weight as feature
2. Predicts the weight that maximizes NDCG for each query
3. Evaluates both weight prediction accuracy and resulting NDCG performance

Author: Dynamic Hybrid Search Team
Version: 1.0.0 - Weight predictor evaluation
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
import time
import string
import re
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from opensearchpy import OpenSearch
from collections import defaultdict

# Handle sklearn version compatibility
try:
    from sklearn.metrics import root_mean_squared_error
except ImportError:
    def root_mean_squared_error(y_true, y_pred):
        return np.sqrt(mean_squared_error(y_true, y_pred))

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, '..'))

# O19S imports
from dynamic_hybrid.utils import metrics
from beir import LoggingHandler

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class O19SWeightPredictorEvaluator:
    """Evaluate model that predicts optimal weight directly"""
    
    # O19S exact list of common English stopwords
    STOPWORDS = {
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
        "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
        "their", "then", "there", "these", "they", "this", "to", "was", "will",
        "with", "without"
    }
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 corpus_field: str = "product_title"):
        """Initialize weight predictor evaluator"""
        
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
        self.corpus_field = corpus_field
        
        # Cache for corpus statistics
        self._corpus_cache = {}
        self._total_docs = None
        
        # Initialize corpus info
        self._initialize_corpus_info()
        
        logger.info(f"Initialized O19S weight predictor evaluator for {host}:{port}/{index_name}")
        logger.info(f"Corpus field: {corpus_field}")
        logger.info(f"Total documents: {self._total_docs}")
    
    def _initialize_corpus_info(self):
        """Initialize corpus information."""
        try:
            response = self.client.count(index=self.index_name)
            self._total_docs = response['count']
        except Exception as e:
            logger.warning(f"Could not get document count: {e}")
            self._total_docs = 100000
    
    # O19S Exact Query Feature Functions
    
    def has_punctuation_at_end(self, text: str) -> int:
        """Checks if a string ends with a punctuation character."""
        stripped_text = text.strip()
        if not stripped_text:
            return 0
        return 1 if stripped_text[-1] in string.punctuation else 0
    
    def capital_letters_ratio(self, text: str) -> float:
        """Calculates the ratio of capital letters to total characters."""
        if not text:
            return 0.0
        capital_count = sum(1 for char in text if char.isupper())
        return capital_count / len(text)
    
    def stopwords_ratio(self, text: str) -> float:
        """Calculates the ratio of stopwords to total terms."""
        preprocessed_text = text.lower()
        terms = re.findall(r'\b\w+\b', preprocessed_text)
        if not terms:
            return 0.0
        stopword_count = sum(1 for term in terms if term in self.STOPWORDS)
        return stopword_count / len(terms)
    
    def load_model(self, model_path: str) -> object:
        """Load trained model from pickle file."""
        
        logger.info(f"Loading model from: {model_path}")
        
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        # Load metadata if available
        metadata_path = model_path.replace('.pkl', '_metadata.json')
        if Path(metadata_path).exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            logger.info(f"Model metadata:")
            logger.info(f"  Type: {metadata.get('model_type', 'Unknown')}")
            logger.info(f"  Target: {metadata.get('target', 'Unknown')}")
            logger.info(f"  Features: {metadata.get('num_features', 'Unknown')}")
            logger.info(f"  Alpha: {metadata.get('alpha', 'Unknown')}")
        
        return model
    
    def find_best_static_weight(self,
                               o19s_data_path: str,
                               ratings_file: str,
                               sample_size: Optional[int] = 50,
                               weights_to_test: Optional[List[float]] = None) -> Tuple[float, float]:
        """
        Find the best static weight by evaluating all weights.
        
        Returns:
            Tuple of (best_weight, best_ndcg)
        """
        
        if weights_to_test is None:
            weights_to_test = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        
        logger.info("Finding best static weight baseline...")
        logger.info(f"  Testing {len(weights_to_test)} weights")
        logger.info(f"  Sample size: {sample_size}")
        
        # Load test data
        test_file = Path(o19s_data_path) / 'query_test.csv'
        df_test = pd.read_csv(test_file)
        
        # Load ratings
        df_ratings = pd.read_csv(ratings_file, sep='\t', header=None,
                               names=['query_string', 'product_id', 'esci_label', 'query_id'],
                               on_bad_lines='skip')
        
        # Map to ratings
        numeric_to_score = {3: 1.0, 2: 0.1, 1: 0.01, 0: 0.0}
        df_ratings['esci_label_numeric'] = pd.to_numeric(df_ratings['esci_label'], errors='coerce')
        
        if not df_ratings['esci_label_numeric'].isna().all():
            df_ratings['rating'] = df_ratings['esci_label_numeric'].map(numeric_to_score)
        else:
            esci_to_numeric = {'E': 1.0, 'S': 0.1, 'C': 0.01, 'I': 0.0}
            df_ratings['rating'] = df_ratings['esci_label'].map(esci_to_numeric)
        
        # Create reference
        reference = {}
        for query_string, group in df_ratings.groupby('query_string'):
            reference[query_string] = group[['product_id', 'rating']].rename(columns={'product_id': 'docid'})
        
        # Sample queries
        test_queries = df_test['query_string'].tolist()
        if sample_size and sample_size < len(test_queries):
            np.random.seed(42)
            test_queries = np.random.choice(test_queries, size=sample_size, replace=False).tolist()
        
        test_queries_with_ratings = [q for q in test_queries if q in reference]
        
        # Test each static weight
        weight_results = {}
        
        for weight in weights_to_test:
            lexical_weight = round(1.0 - weight, 2)
            ndcg_scores = []
            
            for query_string in test_queries_with_ratings:
                try:
                    # Run search with this weight
                    search_results = self._execute_hybrid_search(query_string, lexical_weight, weight)
                    
                    if not search_results.empty:
                        df_with_ratings = self._merge_results_with_reference(
                            search_results, reference[query_string]
                        )
                        if not df_with_ratings.empty:
                            ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                            ndcg_scores.append(ndcg)
                
                except Exception as e:
                    logger.debug(f"Failed for query '{query_string[:30]}...' with weight {weight}: {e}")
                    continue
            
            if ndcg_scores:
                mean_ndcg = np.mean(ndcg_scores)
                weight_results[weight] = mean_ndcg
                logger.info(f"  Weight {weight:.1f}: NDCG@10 = {mean_ndcg:.4f} ({len(ndcg_scores)} queries)")
        
        # Find best weight
        if weight_results:
            best_weight = max(weight_results, key=weight_results.get)
            best_ndcg = weight_results[best_weight]
            logger.info(f"\nBest static weight: {best_weight:.1f} with NDCG@10 = {best_ndcg:.4f}")
            return best_weight, best_ndcg
        else:
            logger.warning("No valid results for any static weight")
            return 0.5, 0.0
    
    def _calculate_additional_metrics(self, 
                                     search_results: pd.DataFrame, 
                                     reference_ratings: pd.DataFrame,
                                     k: int = 10) -> Dict[str, float]:
        """
        Calculate precision@k, recall@k, and DCG@k.
        
        Returns:
            Dictionary with precision, recall, and DCG values
        """
        
        if search_results.empty or reference_ratings.empty:
            return {'precision': 0.0, 'recall': 0.0, 'dcg': 0.0}
        
        # Get top k results
        top_k_results = search_results.head(k)
        
        # Merge with ratings
        merged = top_k_results.merge(
            reference_ratings,
            left_on='product_id',
            right_on='docid',
            how='left'
        )
        merged['rating'] = merged['rating'].fillna(0)
        
        # Calculate precision@k (fraction of relevant items in top k)
        # Consider items with rating > 0 as relevant
        relevant_in_top_k = (merged['rating'] > 0).sum()
        precision_at_k = relevant_in_top_k / k if k > 0 else 0.0
        
        # Calculate recall@k (fraction of all relevant items retrieved in top k)
        total_relevant = (reference_ratings['rating'] > 0).sum()
        recall_at_k = relevant_in_top_k / total_relevant if total_relevant > 0 else 0.0
        
        # Calculate DCG@k (not normalized)
        # DCG = sum(rating_i / log2(i + 1)) for i = 1 to k
        dcg = 0.0
        for i, row in enumerate(merged.itertuples(), 1):
            if i <= k:
                dcg += row.rating / np.log2(i + 1)
        
        return {
            'precision': float(precision_at_k),
            'recall': float(recall_at_k),
            'dcg': float(dcg)
        }
    
    def evaluate_model(self,
                      model: object,
                      o19s_data_path: str,
                      ratings_file: str,
                      sample_size: Optional[int] = None,
                      use_fixed_queries: bool = False,
                      weights_to_test: Optional[List[float]] = None) -> Dict:
        """
        Evaluate weight predictor model.
        
        Args:
            model: Trained Ridge regression model
            o19s_data_path: Path to O19S data
            ratings_file: Path to ratings file
            sample_size: Number of queries to evaluate
            use_fixed_queries: If True, use fixed order from CSV
            weights_to_test: Weights to test for oracle comparison
            
        Returns:
            Dictionary with evaluation results
        """
        
        if weights_to_test is None:
            weights_to_test = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        
        logger.info(f"Evaluating weight predictor model")
        logger.info(f"  Sample size: {sample_size if sample_size else 'All'}")
        logger.info(f"  Use fixed queries: {use_fixed_queries}")
        
        # Load test data
        test_file = Path(o19s_data_path) / 'query_test.csv'
        if not test_file.exists():
            raise FileNotFoundError(f"O19S test queries not found: {test_file}")
            
        df_test = pd.read_csv(test_file)
        
        # Load ratings
        if not Path(ratings_file).exists():
            raise FileNotFoundError(f"Ratings file not found: {ratings_file}")
            
        logger.info(f"Loading ratings from: {ratings_file}")
        df_ratings = pd.read_csv(ratings_file, sep='\t', header=None, 
                               names=['query_string', 'product_id', 'esci_label', 'query_id'],
                               on_bad_lines='skip')
        
        # Map numeric labels to rating scores
        numeric_to_score = {
            3: 1.0,    # Exact
            2: 0.1,    # Substitute
            1: 0.01,   # Complement
            0: 0.0     # Irrelevant
        }
        
        # Convert to numeric and map
        df_ratings['esci_label_numeric'] = pd.to_numeric(df_ratings['esci_label'], errors='coerce')
        
        if not df_ratings['esci_label_numeric'].isna().all():
            df_ratings['rating'] = df_ratings['esci_label_numeric'].map(numeric_to_score)
            logger.info(f"Using numeric label mapping (0,1,2,3) -> rating scores")
        else:
            # Fall back to ESCI letter mapping
            esci_to_numeric = {
                'E': 1.0,    # Exact
                'S': 0.1,    # Substitute
                'C': 0.01,   # Complement
                'I': 0.0     # Irrelevant
            }
            df_ratings['rating'] = df_ratings['esci_label'].map(esci_to_numeric)
            logger.info(f"Using ESCI letter mapping (E,S,C,I) -> rating scores")
        
        # Create reference dictionary
        reference = {}
        for query_string, group in df_ratings.groupby('query_string'):
            reference[query_string] = group[['product_id', 'rating']].rename(columns={'product_id': 'docid'})
        
        logger.info(f"Created reference for {len(reference)} unique queries")
        
        # Get test queries
        test_queries = df_test['query_string'].tolist()
        
        if use_fixed_queries:
            if sample_size and sample_size < len(test_queries):
                test_queries = test_queries[:sample_size]
                logger.info(f"Using first {sample_size} queries from fixed test set")
            else:
                logger.info(f"Using all {len(test_queries)} queries from fixed test set")
        else:
            if sample_size and sample_size < len(test_queries):
                np.random.seed(42)
                test_queries = np.random.choice(test_queries, size=sample_size, replace=False).tolist()
                logger.info(f"Randomly sampled {sample_size} test queries")
            else:
                logger.info(f"Using all {len(test_queries)} test queries")
        
        # Filter to queries with ratings
        test_queries_with_ratings = [q for q in test_queries if q in reference]
        logger.info(f"Test queries with ratings: {len(test_queries_with_ratings)}")
        
        # Find best static weight first
        # FIXED: Use ALL test queries for baseline calculation to ensure fair comparison
        best_static_weight, best_static_ndcg = self.find_best_static_weight(
            o19s_data_path=o19s_data_path,
            ratings_file=ratings_file,
            sample_size=len(test_queries_with_ratings),  # Use ALL queries for fair comparison
            weights_to_test=weights_to_test
        )
        
        logger.info(f"\nUsing best static weight {best_static_weight:.1f} (NDCG@10={best_static_ndcg:.4f}) as baseline")
        
        # Evaluate predictions
        results = {
            'predicted_weights': [],
            'actual_optimal_weights': [],
            'predicted_ndcg': [],
            'oracle_ndcg': [],
            'static_ndcg': [],  # Best static weight
            'query_strings': [],
            # Additional metrics
            'predicted_precision': [],
            'predicted_recall': [],
            'predicted_dcg': [],
            'oracle_precision': [],
            'oracle_recall': [],
            'oracle_dcg': [],
            'static_precision': [],
            'static_recall': [],
            'static_dcg': []
        }
        
        for query_string in tqdm(test_queries_with_ratings, desc="Evaluating predictions"):
            if query_string not in reference:
                continue
            
            try:
                # Extract features for this query
                features = self._extract_17_features(query_string)
                
                # Predict weight using the model
                if hasattr(model, 'scaler'):
                    # Apply scaler if model has one
                    features_scaled = model.scaler.transform([features])
                    predicted_weight = model.predict(features_scaled)[0]
                else:
                    predicted_weight = model.predict([features])[0]
                
                # Clip to valid range
                predicted_weight = np.clip(predicted_weight, 0.0, 1.0)
                
                # Round to nearest 0.1 for practical use
                predicted_weight_rounded = round(predicted_weight * 10) / 10
                
                # Find actual optimal weight (oracle) by testing all weights
                best_weight = 0.5
                best_ndcg = 0.0
                
                for weight in weights_to_test:
                    lexical_weight = round(1.0 - weight, 2)
                    
                    # Run hybrid search with this weight
                    search_results = self._execute_hybrid_search(query_string, lexical_weight, weight)
                    
                    if not search_results.empty:
                        # Calculate NDCG
                        df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                        
                        if not df_with_ratings.empty:
                            actual_ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                            
                            if actual_ndcg > best_ndcg:
                                best_ndcg = actual_ndcg
                                best_weight = weight
                
                # Get NDCG with predicted weight
                predicted_lexical_weight = round(1.0 - predicted_weight_rounded, 2)
                predicted_search_results = self._execute_hybrid_search(
                    query_string, predicted_lexical_weight, predicted_weight_rounded
                )
                
                predicted_ndcg = 0.0
                if not predicted_search_results.empty:
                    df_with_ratings = self._merge_results_with_reference(
                        predicted_search_results, reference[query_string]
                    )
                    if not df_with_ratings.empty:
                        predicted_ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                
                # Get metrics with best static weight
                static_lexical_weight = round(1.0 - best_static_weight, 2)
                static_search_results = self._execute_hybrid_search(
                    query_string, static_lexical_weight, best_static_weight
                )
                static_ndcg = 0.0
                static_metrics = {'precision': 0.0, 'recall': 0.0, 'dcg': 0.0}
                if not static_search_results.empty:
                    df_with_ratings = self._merge_results_with_reference(
                        static_search_results, reference[query_string]
                    )
                    if not df_with_ratings.empty:
                        static_ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                    static_metrics = self._calculate_additional_metrics(
                        static_search_results, reference[query_string]
                    )
                
                # Calculate additional metrics for predicted weight
                predicted_metrics = {'precision': 0.0, 'recall': 0.0, 'dcg': 0.0}
                if not predicted_search_results.empty:
                    predicted_metrics = self._calculate_additional_metrics(
                        predicted_search_results, reference[query_string]
                    )
                
                # Calculate additional metrics for oracle weight
                oracle_lexical_weight = round(1.0 - best_weight, 2)
                oracle_search_results = self._execute_hybrid_search(
                    query_string, oracle_lexical_weight, best_weight
                )
                oracle_metrics = {'precision': 0.0, 'recall': 0.0, 'dcg': 0.0}
                if not oracle_search_results.empty:
                    oracle_metrics = self._calculate_additional_metrics(
                        oracle_search_results, reference[query_string]
                    )
                
                # Store results
                results['predicted_weights'].append(predicted_weight_rounded)
                results['actual_optimal_weights'].append(best_weight)
                results['predicted_ndcg'].append(predicted_ndcg)
                results['oracle_ndcg'].append(best_ndcg)
                results['static_ndcg'].append(static_ndcg)
                results['query_strings'].append(query_string)
                # Additional metrics
                results['predicted_precision'].append(predicted_metrics['precision'])
                results['predicted_recall'].append(predicted_metrics['recall'])
                results['predicted_dcg'].append(predicted_metrics['dcg'])
                results['oracle_precision'].append(oracle_metrics['precision'])
                results['oracle_recall'].append(oracle_metrics['recall']) 
                results['oracle_dcg'].append(oracle_metrics['dcg'])
                results['static_precision'].append(static_metrics['precision'])
                results['static_recall'].append(static_metrics['recall'])
                results['static_dcg'].append(static_metrics['dcg'])
                
            except Exception as e:
                logger.warning(f"Failed to evaluate query '{query_string}': {e}")
                continue
        
        # Calculate metrics
        evaluation_metrics = self._calculate_evaluation_metrics(results, best_static_weight)
        
        return evaluation_metrics
    
    def _calculate_evaluation_metrics(self, results: Dict, best_static_weight: float) -> Dict:
        """Calculate comprehensive evaluation metrics including additional IR metrics."""
        
        if not results['predicted_weights']:
            logger.warning("No evaluation results to calculate metrics")
            return {}
        
        # Convert to arrays
        predicted_weights = np.array(results['predicted_weights'])
        actual_weights = np.array(results['actual_optimal_weights'])
        predicted_ndcg = np.array(results['predicted_ndcg'])
        oracle_ndcg = np.array(results['oracle_ndcg'])
        static_ndcg = np.array(results['static_ndcg'])
        
        # Additional metrics arrays
        predicted_precision = np.array(results['predicted_precision'])
        predicted_recall = np.array(results['predicted_recall'])
        predicted_dcg = np.array(results['predicted_dcg'])
        oracle_precision = np.array(results['oracle_precision'])
        oracle_recall = np.array(results['oracle_recall'])
        oracle_dcg = np.array(results['oracle_dcg'])
        static_precision = np.array(results['static_precision'])
        static_recall = np.array(results['static_recall'])
        static_dcg = np.array(results['static_dcg'])
        
        # Weight prediction metrics
        weight_mse = mean_squared_error(actual_weights, predicted_weights)
        weight_rmse = root_mean_squared_error(actual_weights, predicted_weights)
        weight_mae = mean_absolute_error(actual_weights, predicted_weights)
        weight_r2 = r2_score(actual_weights, predicted_weights)
        
        # NDCG performance metrics
        mean_predicted_ndcg = np.mean(predicted_ndcg)
        mean_oracle_ndcg = np.mean(oracle_ndcg)
        mean_static_ndcg = np.mean(static_ndcg)
        
        # Additional metrics means
        mean_predicted_precision = np.mean(predicted_precision)
        mean_predicted_recall = np.mean(predicted_recall)
        mean_predicted_dcg = np.mean(predicted_dcg)
        mean_oracle_precision = np.mean(oracle_precision)
        mean_oracle_recall = np.mean(oracle_recall)
        mean_oracle_dcg = np.mean(oracle_dcg)
        mean_static_precision = np.mean(static_precision)
        mean_static_recall = np.mean(static_recall)
        mean_static_dcg = np.mean(static_dcg)
        
        # Relative performance
        relative_to_oracle = (mean_predicted_ndcg / mean_oracle_ndcg * 100) if mean_oracle_ndcg > 0 else 0
        relative_to_static = (mean_predicted_ndcg / mean_static_ndcg * 100) if mean_static_ndcg > 0 else 0
        improvement_over_static = mean_predicted_ndcg - mean_static_ndcg
        
        # Weight distribution analysis
        predicted_dist = pd.Series(predicted_weights).value_counts().sort_index()
        actual_dist = pd.Series(actual_weights).value_counts().sort_index()
        
        # Count perfect predictions
        perfect_predictions = np.sum(predicted_weights == actual_weights)
        perfect_prediction_rate = perfect_predictions / len(predicted_weights) * 100
        
        # Count close predictions (within 0.1)
        close_predictions = np.sum(np.abs(predicted_weights - actual_weights) <= 0.1)
        close_prediction_rate = close_predictions / len(predicted_weights) * 100
        
        # Calculate per-query improvements
        improvements_over_static = predicted_ndcg - static_ndcg
        queries_improved = np.sum(improvements_over_static > 0)
        queries_improved_rate = queries_improved / len(improvements_over_static) * 100
        
        evaluation_metrics = {
            'num_queries': len(predicted_weights),
            'best_static_weight': float(best_static_weight),
            
            # Weight prediction accuracy
            'weight_prediction': {
                'mse': float(weight_mse),
                'rmse': float(weight_rmse),
                'mae': float(weight_mae),
                'r2': float(weight_r2),
                'perfect_predictions': int(perfect_predictions),
                'perfect_prediction_rate': float(perfect_prediction_rate),
                'close_predictions': int(close_predictions),
                'close_prediction_rate': float(close_prediction_rate)
            },
            
            # NDCG@10 performance
            'ndcg_at_10': {
                'mean_predicted': float(mean_predicted_ndcg),
                'mean_oracle': float(mean_oracle_ndcg),
                'mean_static_best': float(mean_static_ndcg),
                'relative_to_oracle': float(relative_to_oracle),
                'relative_to_static': float(relative_to_static),
                'improvement_over_static': float(improvement_over_static),
                'queries_improved': int(queries_improved),
                'queries_improved_rate': float(queries_improved_rate)
            },
            
            # Precision@10 performance
            'precision_at_10': {
                'mean_predicted': float(mean_predicted_precision),
                'mean_oracle': float(mean_oracle_precision),
                'mean_static_best': float(mean_static_precision),
                'relative_to_oracle': (mean_predicted_precision / mean_oracle_precision * 100) if mean_oracle_precision > 0 else 0,
                'relative_to_static': (mean_predicted_precision / mean_static_precision * 100) if mean_static_precision > 0 else 0,
                'improvement_over_static': float(mean_predicted_precision - mean_static_precision)
            },
            
            # Recall@10 performance
            'recall_at_10': {
                'mean_predicted': float(mean_predicted_recall),
                'mean_oracle': float(mean_oracle_recall),
                'mean_static_best': float(mean_static_recall),
                'relative_to_oracle': (mean_predicted_recall / mean_oracle_recall * 100) if mean_oracle_recall > 0 else 0,
                'relative_to_static': (mean_predicted_recall / mean_static_recall * 100) if mean_static_recall > 0 else 0,
                'improvement_over_static': float(mean_predicted_recall - mean_static_recall)
            },
            
            # DCG@10 performance
            'dcg_at_10': {
                'mean_predicted': float(mean_predicted_dcg),
                'mean_oracle': float(mean_oracle_dcg),
                'mean_static_best': float(mean_static_dcg),
                'relative_to_oracle': (mean_predicted_dcg / mean_oracle_dcg * 100) if mean_oracle_dcg > 0 else 0,
                'relative_to_static': (mean_predicted_dcg / mean_static_dcg * 100) if mean_static_dcg > 0 else 0,
                'improvement_over_static': float(mean_predicted_dcg - mean_static_dcg)
            },
            
            # Weight distributions
            'weight_distributions': {
                'predicted': predicted_dist.to_dict(),
                'actual_optimal': actual_dist.to_dict()
            },
            
            # Raw results for further analysis
            'raw_results': {
                'predicted_weights': results['predicted_weights'][:10],  # First 10 for inspection
                'actual_optimal_weights': results['actual_optimal_weights'][:10],
                'predicted_ndcg': results['predicted_ndcg'][:10],
                'oracle_ndcg': results['oracle_ndcg'][:10],
                'static_ndcg': results['static_ndcg'][:10]
            }
        }
        
        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("EVALUATION RESULTS SUMMARY")
        logger.info("=" * 80)
        
        logger.info(f"\nEvaluated on {len(predicted_weights)} queries")
        logger.info(f"Best static weight: {best_static_weight:.1f}")
        
        logger.info("\nWeight Prediction Accuracy:")
        logger.info(f"  RMSE: {weight_rmse:.4f}")
        logger.info(f"  MAE: {weight_mae:.4f}")
        logger.info(f"  R²: {weight_r2:.4f}")
        logger.info(f"  Perfect predictions: {perfect_predictions}/{len(predicted_weights)} ({perfect_prediction_rate:.1f}%)")
        logger.info(f"  Close predictions (±0.1): {close_predictions}/{len(predicted_weights)} ({close_prediction_rate:.1f}%)")
        
        logger.info("\n" + "=" * 60)
        logger.info("RETRIEVAL METRICS SUMMARY")
        logger.info("=" * 60)
        
        logger.info("\nNDCG@10 Performance:")
        logger.info(f"  Predicted:    {mean_predicted_ndcg:.4f}")
        logger.info(f"  Oracle:       {mean_oracle_ndcg:.4f}")
        logger.info(f"  Static Best:  {mean_static_ndcg:.4f} (weight={best_static_weight:.1f})")
        logger.info(f"  Relative to Oracle: {relative_to_oracle:.1f}%")
        logger.info(f"  Relative to Static: {relative_to_static:.1f}%")
        logger.info(f"  Improvement: {improvement_over_static:+.4f}")
        
        logger.info("\nPrecision@10 Performance:")
        logger.info(f"  Predicted:    {mean_predicted_precision:.4f}")
        logger.info(f"  Oracle:       {mean_oracle_precision:.4f}")
        logger.info(f"  Static Best:  {mean_static_precision:.4f}")
        logger.info(f"  Improvement: {mean_predicted_precision - mean_static_precision:+.4f}")
        
        logger.info("\nRecall@10 Performance:")
        logger.info(f"  Predicted:    {mean_predicted_recall:.4f}")
        logger.info(f"  Oracle:       {mean_oracle_recall:.4f}")
        logger.info(f"  Static Best:  {mean_static_recall:.4f}")
        logger.info(f"  Improvement: {mean_predicted_recall - mean_static_recall:+.4f}")
        
        logger.info("\nDCG@10 Performance:")
        logger.info(f"  Predicted:    {mean_predicted_dcg:.4f}")
        logger.info(f"  Oracle:       {mean_oracle_dcg:.4f}")
        logger.info(f"  Static Best:  {mean_static_dcg:.4f}")
        logger.info(f"  Improvement: {mean_predicted_dcg - mean_static_dcg:+.4f}")
        
        logger.info("\nQuery-level Analysis:")
        logger.info(f"  Queries improved over static: {queries_improved}/{len(improvements_over_static)} ({queries_improved_rate:.1f}%)")
        
        logger.info("\nPredicted Weight Distribution:")
        for weight, count in predicted_dist.items():
            percentage = count / len(predicted_weights) * 100
            logger.info(f"  Weight {weight:.1f}: {count} ({percentage:.1f}%)")
        
        logger.info("\nActual Optimal Weight Distribution:")
        for weight, count in actual_dist.items():
            percentage = count / len(actual_weights) * 100
            logger.info(f"  Weight {weight:.1f}: {count} ({percentage:.1f}%)")
        
        logger.info("=" * 80)
        
        return evaluation_metrics
    
    def _extract_17_features(self, query: str) -> List[float]:
        """Extract 17 features (NO weight feature)."""
        
        # Extract query features using O19S exact functions
        query_features = {
            'f_2_query_length': len(query),
            'f_4_has_special_char': float(any(c in query for c in '!@#$%^&*()_+-=[]{}|;:,.<>?')),
            'f_5_has_punctuation_at_end': float(self.has_punctuation_at_end(query)),
            'f_7_capital_letters_ratio': self.capital_letters_ratio(query),
            'f_8_stopwords_ratio': self.stopwords_ratio(query)
        }
        
        # Extract real-time corpus features using O19S method
        corpus_features = self._collect_corpus_features_o19s_method(query)
        
        # Create 17-feature vector (NO weight at index 0)
        feature_vector = [
            # NO weight feature - we're predicting this!
            query_features['f_2_query_length'],
            query_features['f_4_has_special_char'],
            query_features['f_5_has_punctuation_at_end'],
            query_features['f_7_capital_letters_ratio'],
            query_features['f_8_stopwords_ratio'],
            corpus_features['max_document_frequency'],
            corpus_features['min_document_frequency'],
            corpus_features['total_document_frequency'],
            corpus_features['average_document_frequency'],
            corpus_features['variance_document_frequency'],
            corpus_features['std_dev_document_frequency'],
            corpus_features['max_inverse_document_frequency'],
            corpus_features['min_inverse_document_frequency'],
            corpus_features['total_inverse_document_frequency'],
            corpus_features['average_inverse_document_frequency'],
            corpus_features['variance_inverse_document_frequency'],
            corpus_features['std_dev_inverse_document_frequency']
        ]
        
        return feature_vector
    
    def _collect_corpus_features_o19s_method(self, query_string: str) -> Dict[str, float]:
        """Collect corpus features using O19S exact termvectors method."""
        
        # Check cache first
        cache_key = f"query:{query_string}"
        if cache_key in self._corpus_cache:
            return self._corpus_cache[cache_key]
        
        try:
            # O19S exact termvectors implementation
            import math
            
            # Get total document count
            doc_count_response = self.client.count(index=self.index_name)
            total_docs = doc_count_response['count']

            # O19S termvectors API call
            body = {
                "doc": {
                    self.corpus_field: query_string
                },
                "fields": [self.corpus_field],
                "term_statistics": True,
                "field_statistics": True,
            }

            term_vectors_response = self.client.termvectors(index=self.index_name, body=body)

            # Process response to extract term statistics
            doc_freqs = []
            idfs = []

            if 'term_vectors' in term_vectors_response and self.corpus_field in term_vectors_response['term_vectors']:
                terms = term_vectors_response['term_vectors'][self.corpus_field]['terms']

                for term, stats in terms.items():
                    doc_freq = stats.get('doc_freq', 0)
                    idf = 0.0
                    if doc_freq > 0:
                        idf = math.log(total_docs / doc_freq)  # O19S uses natural log

                    doc_freqs.append(doc_freq)
                    idfs.append(idf)

            # Calculate O19S summary statistics
            if doc_freqs:
                # Document Frequency Stats
                max_df = max(doc_freqs)
                min_df = min(doc_freqs)
                sum_df = sum(doc_freqs)
                avg_df = sum_df / len(doc_freqs)
                
                variance_df = sum([(x - avg_df) ** 2 for x in doc_freqs]) / len(doc_freqs)
                std_dev_df = math.sqrt(variance_df)

                # IDF Stats  
                max_idf = max(idfs)
                min_idf = min(idfs)
                sum_idf = sum(idfs)
                avg_idf = sum_idf / len(idfs)

                variance_idf = sum([(x - avg_idf) ** 2 for x in idfs]) / len(idfs)
                std_dev_idf = math.sqrt(variance_idf)

                corpus_features = {
                    'max_document_frequency': max_df,
                    'min_document_frequency': min_df,
                    'total_document_frequency': sum_df,
                    'average_document_frequency': avg_df,
                    'variance_document_frequency': variance_df,
                    'std_dev_document_frequency': std_dev_df,
                    'max_inverse_document_frequency': max_idf,
                    'min_inverse_document_frequency': min_idf,
                    'total_inverse_document_frequency': sum_idf,
                    'average_inverse_document_frequency': avg_idf,
                    'variance_inverse_document_frequency': variance_idf,
                    'std_dev_inverse_document_frequency': std_dev_idf
                }
            else:
                # No terms found - use zeros
                corpus_features = {
                    'max_document_frequency': 0.0,
                    'min_document_frequency': 0.0,
                    'total_document_frequency': 0.0,
                    'average_document_frequency': 0.0,
                    'variance_document_frequency': 0.0,
                    'std_dev_document_frequency': 0.0,
                    'max_inverse_document_frequency': 0.0,
                    'min_inverse_document_frequency': 0.0,
                    'total_inverse_document_frequency': 0.0,
                    'average_inverse_document_frequency': 0.0,
                    'variance_inverse_document_frequency': 0.0,
                    'std_dev_inverse_document_frequency': 0.0
                }
            
            # Cache results
            self._corpus_cache[cache_key] = corpus_features
            
            return corpus_features
            
        except Exception as e:
            logger.warning(f"Failed to get corpus features for '{query_string}': {e}")
            # Return zeros on failure
            return {
                'max_document_frequency': 0.0,
                'min_document_frequency': 0.0,
                'total_document_frequency': 0.0,
                'average_document_frequency': 0.0,
                'variance_document_frequency': 0.0,
                'std_dev_document_frequency': 0.0,
                'max_inverse_document_frequency': 0.0,
                'min_inverse_document_frequency': 0.0,
                'total_inverse_document_frequency': 0.0,
                'average_inverse_document_frequency': 0.0,
                'variance_inverse_document_frequency': 0.0,
                'std_dev_inverse_document_frequency': 0.0
            }
    
    def _execute_hybrid_search(self, query: str, lexical_weight: float, neural_weight: float) -> pd.DataFrame:
        """Execute O19S hybrid search."""
        
        # Build payload with l2 normalization and arithmetic mean (most common)
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
                "description": "O19S weight predictor evaluation",
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
            # Execute search
            response = self.client.search(index=self.index_name, body=payload)
            
            # Parse results
            results = []
            for position, hit in enumerate(response['hits']['hits']):
                results.append({
                    'product_id': hit['_source'].get('product_id', hit['_id']),
                    'score': hit['_score'],
                    'rank': len(results) + 1
                })
            
            return pd.DataFrame(results) if results else pd.DataFrame()
            
        except Exception as e:
            logger.debug(f"Search failed for query '{query[:30]}...': {e}")
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
        
        # Rename 'rank' to 'position' for metrics.ndcg_at_10() compatibility
        merged['position'] = merged['rank']
        
        # Return with correct column names expected by metrics.ndcg_at_10()
        return merged[['position', 'rating', 'product_id']]
    
    def save_results(self, evaluation_metrics: Dict, output_path: str):
        """Save evaluation results to JSON file."""
        
        with open(output_path, 'w') as f:
            json.dump(evaluation_metrics, f, indent=2)
        
        logger.info(f"Evaluation results saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Evaluate O19S Weight Predictor Model')
    
    # OpenSearch connection
    parser.add_argument('--host', type=str, default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('--index-name', type=str, default='esci-products',
                       help='Index name')
    parser.add_argument('--model-id', type=str, 
                       default='huggingface/sentence-transformers/all-MiniLM-L6-v2',
                       help='Neural model ID')
    
    # Model and data paths
    parser.add_argument('--model-file', type=str, required=True,
                       help='Path to trained weight predictor model')
    parser.add_argument('--o19s-data-path', type=str, 
                       default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', type=str,
                       default='dynamic_hybrid/data/ratings.csv',
                       help='Path to ratings file')
    
    # Evaluation parameters
    parser.add_argument('--sample-size', type=int, default=100,
                       help='Number of queries for evaluation')
    parser.add_argument('--weights', type=str, default='0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0',
                       help='Comma-separated weights to test for oracle')
    parser.add_argument('--use-fixed-queries', action='store_true',
                       help='Use fixed query order from CSV')
    
    # Output
    parser.add_argument('--output-results', type=str, 
                       default='o19s_weight_predictor_evaluation.json',
                       help='Output file for evaluation results')
    
    args = parser.parse_args()
    
    # Parse weights
    weights_to_test = [float(w) for w in args.weights.split(',')]
    
    # Initialize evaluator
    evaluator = O19SWeightPredictorEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index_name,
        model_id=args.model_id
    )
    
    logger.info("=" * 80)
    logger.info("O19S Weight Predictor Model Evaluation")
    logger.info("=" * 80)
    logger.info(f"Model: {args.model_file}")
    logger.info(f"Sample size: {args.sample_size}")
    logger.info(f"Weights to test: {weights_to_test}")
    logger.info("=" * 80)
    
    # Load model
    model = evaluator.load_model(args.model_file)
    
    # Evaluate model
    evaluation_metrics = evaluator.evaluate_model(
        model=model,
        o19s_data_path=args.o19s_data_path,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        use_fixed_queries=args.use_fixed_queries,
        weights_to_test=weights_to_test
    )
    
    # Save results
    if evaluation_metrics:
        evaluator.save_results(evaluation_metrics, args.output_results)
    
    logger.info("\n" + "=" * 80)
    logger.info("Evaluation Complete!")
    logger.info("=" * 80)
    logger.info(f"Results saved to: {args.output_results}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
