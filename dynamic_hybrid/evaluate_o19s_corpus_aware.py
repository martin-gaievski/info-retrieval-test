#!/usr/bin/env python3
"""
O19S Corpus-Aware Evaluation Script

This implements the true O19S approach for corpus-aware models:
O19S runs queries to OpenSearch during search time to collect 
corpus-aware features (document frequencies, term statistics).

Focus: 18+ feature models with real-time corpus feature collection only.

Author: Dynamic Hybrid Search Team
Version: 2.0.1 - Simplified corpus-aware only
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
import time

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, '..'))

# Import required modules for feature extraction
import string
import re

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


class O19SCorpusAwareFeatureExtractor:
    """Extract O19S corpus-aware features with real-time OpenSearch queries."""
    
    # O19S exact list of common English stopwords (must match training script)
    STOPWORDS = {
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
        "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
        "their", "then", "there", "these", "they", "this", "to", "was", "will",
        "with", "without"
    }
    
    def __init__(self, 
                 opensearch_client: OpenSearch,
                 index_name: str,
                 corpus_field: str = "product_title"):
        """
        Initialize corpus-aware feature extractor.
        
        Args:
            opensearch_client: OpenSearch client for corpus queries
            index_name: Index name to query for corpus statistics
            corpus_field: Field to analyze for corpus statistics
        """
        self.client = opensearch_client
        self.index_name = index_name
        self.corpus_field = corpus_field
        
        # Cache for corpus statistics to avoid repeated API calls
        self._corpus_cache = {}
        self._total_docs = None
        
        # Initialize corpus info
        self._initialize_corpus_info()
        
        logger.info(f"Initialized O19S corpus-aware feature extractor:")
        logger.info(f"  Index: {index_name}")
        logger.info(f"  Corpus field: {corpus_field}")
        logger.info(f"  Total documents: {self._total_docs}")
    
    def _initialize_corpus_info(self):
        """Initialize corpus information like total document count."""
        try:
            response = self.client.count(index=self.index_name)
            self._total_docs = response['count']
            logger.info(f"Corpus contains {self._total_docs} documents")
        except Exception as e:
            logger.warning(f"Could not get document count: {e}. Using default value.")
            self._total_docs = 100000  # Default fallback
    
    def extract_features(self, query: str, weight: float) -> Dict[str, float]:
        """Extract corpus-aware features for O19S 18-feature model."""
        
        # Extract query features using exact O19S methods from training script
        query_features = {
            'f_2_query_length': len(query),
            'f_4_has_special_char': float(any(c in query for c in '!@#$%^&*()_+-=[]{}|;:,.<>?')),
            'f_5_has_punctuation_at_end': float(self.has_punctuation_at_end(query)),
            'f_7_capital_letters_ratio': self.capital_letters_ratio(query),
            'f_8_stopwords_ratio': self.stopwords_ratio(query)
        }
        
        # Extract real-time corpus features (12 features)
        # For corpus features, we use the full query string not split words
        corpus_features = self._collect_realtime_corpus_features([query])
        
        # Combine all features including weight
        all_features = {
            'f_0_neuralness': weight,  # Weight feature (FIRST in O19S order)
            **query_features,
            **corpus_features
        }
        
        return all_features
    
    # O19S Exact Query Feature Functions (must match training script exactly)
    
    def has_punctuation_at_end(self, text: str) -> int:
        """
        Checks if a string ends with a punctuation character.
        O19S exact implementation from training script.
        
        Args:
            text: The input string.
        
        Returns:
            1 if the string ends with punctuation, 0 otherwise.
        """
        import string
        
        # Check for empty or whitespace-only strings
        stripped_text = text.strip()
        if not stripped_text:
            return 0
        
        # Get the last character of the stripped string and check if it's in the punctuation set
        return 1 if stripped_text[-1] in string.punctuation else 0
    
    def capital_letters_ratio(self, text: str) -> float:
        """
        Calculates the ratio of capital letters to the total number of characters in a string.
        O19S exact implementation from training script.
        
        Args:
            text: The input string.
        
        Returns:
            The ratio of capital letters. Returns 0.0 if the string is empty.
        """
        if not text:
            return 0.0
        
        capital_count = sum(1 for char in text if char.isupper())
        return capital_count / len(text)
    
    def stopwords_ratio(self, text: str) -> float:
        """
        Calculates the ratio of stopwords to the total number of terms in a string.
        O19S exact implementation from training script.
        
        The string is preprocessed to handle case and punctuation.
        
        Args:
            text: The input string.
        
        Returns:
            The ratio of stopwords. Returns 0.0 if the string has no terms.
        """
        import re
        
        # Preprocess the text to get a list of terms
        preprocessed_text = text.lower()
        terms = re.findall(r'\b\w+\b', preprocessed_text)
        
        # Handle the case of an empty string or a string with no words
        if not terms:
            return 0.0
        
        stopword_count = sum(1 for term in terms if term in self.STOPWORDS)
        
        return stopword_count / len(terms)
    
    def _collect_realtime_corpus_features(self, query_terms: List[str]) -> Dict[str, float]:
        """
        Collect corpus features using O19S exact method with termvectors API.
        
        This matches O19S's actual implementation exactly.
        """
        if not query_terms:
            return self._get_zero_corpus_features()
        
        # Use O19S exact method: termvectors API with full query string
        query_string = " ".join(query_terms)
        
        # Check cache for this exact query string
        cache_key = f"query:{query_string}"
        if cache_key in self._corpus_cache:
            cached = self._corpus_cache[cache_key]
            logger.debug(f"Using cached corpus features for query: {query_string}")
            return cached
        
        try:
            # O19S exact method using termvectors API
            statistics = self._get_query_term_statistics_o19s_method(query_string)
            
            if 'summary_statistics' in statistics and 'max_document_frequency' in statistics['summary_statistics']:
                summary = statistics['summary_statistics']
                
                corpus_features = {
                    'f_14_max_document_frequency': float(summary['max_document_frequency']),
                    'f_15_min_document_frequency': float(summary['min_document_frequency']),
                    'f_16_total_document_frequency': float(summary['total_document_frequency']),
                    'f_17_average_document_frequency': float(summary['average_document_frequency']),
                    'f_18_variance_document_frequency': float(summary['variance_document_frequency']),
                    'f_19_std_dev_document_frequency': float(summary['std_dev_document_frequency']),
                    'f_20_max_inverse_document_frequency': float(summary['max_inverse_document_frequency']),
                    'f_21_min_inverse_document_frequency': float(summary['min_inverse_document_frequency']),
                    'f_22_total_inverse_document_frequency': float(summary['total_inverse_document_frequency']),
                    'f_23_average_inverse_document_frequency': float(summary['average_inverse_document_frequency']),
                    'f_24_variance_inverse_document_frequency': float(summary['variance_inverse_document_frequency']),
                    'f_25_std_dev_inverse_document_frequency': float(summary['std_dev_inverse_document_frequency'])
                }
                
                # Cache the results for this query
                self._corpus_cache[cache_key] = corpus_features
                
                logger.debug(f"O19S termvectors corpus features for query: {query_string}")
                logger.debug(f"  Terms found: {len(statistics.get('query_terms', []))}")
                
                return corpus_features
            else:
                logger.warning(f"No valid corpus statistics returned for query: {query_string}")
                return self._get_zero_corpus_features()
                
        except Exception as e:
            logger.warning(f"Could not get corpus features for query '{query_string}': {e}")
            return self._get_zero_corpus_features()
    
    def _get_query_term_statistics_o19s_method(self, query_string: str) -> Dict:
        """
        Get query term statistics using O19S exact method.
        
        This is the exact O19S implementation using termvectors API.
        """
        import math
        
        statistics = {}

        try:
            # Get the total number of documents in the index for IDF calculation
            doc_count_response = self.client.count(index=self.index_name)
            total_docs = doc_count_response['count']

            # Construct the body for the _termvectors API (O19S exact method)
            body = {
                "doc": {
                    self.corpus_field: query_string
                },
                "fields": [self.corpus_field],
                "term_statistics": True,  # This is crucial to get doc_freq
                "field_statistics": True, # This gives us total doc counts for the field
            }

            # Make the API call to get term vectors
            term_vectors_response = self.client.termvectors(index=self.index_name, body=body)

            # Process the response to extract term statistics
            terms_data = []
            doc_freqs = []
            idfs = []

            if 'term_vectors' in term_vectors_response and self.corpus_field in term_vectors_response['term_vectors']:
                terms = term_vectors_response['term_vectors'][self.corpus_field]['terms']

                for term, stats in terms.items():
                    doc_freq = stats.get('doc_freq', 0)
                    idf = 0.0
                    if doc_freq > 0:
                        idf = math.log(total_docs / doc_freq)  # O19S uses natural log

                    term_data = {
                        'term': term,
                        'document_frequency': doc_freq,
                        'inverse_document_frequency': idf
                    }

                    terms_data.append(term_data)
                    doc_freqs.append(doc_freq)
                    idfs.append(idf)

            # Calculate summary statistics for both Document Frequency (DF) and Inverse Document Frequency (IDF)
            if doc_freqs:
                # Document Frequency (DF) Stats (O19S exact calculation)
                max_df = max(doc_freqs)
                min_df = min(doc_freqs)
                sum_df = sum(doc_freqs)
                avg_df = sum_df / len(doc_freqs)
                
                variance_df = sum([(x - avg_df) ** 2 for x in doc_freqs]) / len(doc_freqs)
                std_dev_df = math.sqrt(variance_df)

                # Inverse Document Frequency (IDF) Stats (O19S exact calculation)
                max_idf = max(idfs)
                min_idf = min(idfs)
                sum_idf = sum(idfs)
                avg_idf = sum_idf / len(idfs)

                variance_idf = sum([(x - avg_idf) ** 2 for x in idfs]) / len(idfs)
                std_dev_idf = math.sqrt(variance_idf)

                statistics = {
                    'query_terms': terms_data,
                    'summary_statistics': {
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
                }
            else:
                statistics = {
                    'query_terms': [],
                    'summary_statistics': {
                        'message': 'No terms found or no document frequencies could be retrieved.'
                    }
                }

        except Exception as e:
            logger.warning(f"O19S termvectors API call failed for query '{query_string}': {e}")
            statistics = {
                'query_terms': [],
                'summary_statistics': {
                    'message': f'Error: {str(e)}'
                }
            }

        return statistics
    
    
    def _get_zero_corpus_features(self) -> Dict[str, float]:
        """Return zero values for all corpus features when no terms available."""
        return {
            'f_14_max_document_frequency': 0.0,
            'f_15_min_document_frequency': 0.0,
            'f_16_total_document_frequency': 0.0,
            'f_17_average_document_frequency': 0.0,
            'f_18_variance_document_frequency': 0.0,
            'f_19_std_dev_document_frequency': 0.0,
            'f_20_max_inverse_document_frequency': 0.0,
            'f_21_min_inverse_document_frequency': 0.0,
            'f_22_total_inverse_document_frequency': 0.0,
            'f_23_average_inverse_document_frequency': 0.0,
            'f_24_variance_inverse_document_frequency': 0.0,
            'f_25_std_dev_inverse_document_frequency': 0.0
        }
    
    def clear_cache(self):
        """Clear the corpus statistics cache."""
        self._corpus_cache.clear()
        logger.info("Cleared corpus statistics cache")
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics for monitoring."""
        return {
            "cached_terms": len(self._corpus_cache),
            "cache_size_mb": sys.getsizeof(self._corpus_cache) / 1024 / 1024
        }


class O19SCorpusAwareEvaluator:
    """Evaluate O19S corpus-aware models using real-time feature collection"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 corpus_field: str = "product_title"):
        """Initialize O19S corpus-aware evaluator"""
        
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
        
        # Initialize feature extractor
        self.feature_extractor = O19SCorpusAwareFeatureExtractor(
            opensearch_client=self.client,
            index_name=index_name,
            corpus_field=corpus_field
        )
        
        self.model = None
        
        logger.info(f"Initialized O19S corpus-aware evaluator for {host}:{port}/{index_name}")
    
    def load_model(self, model_path: str):
        """Load O19S corpus-aware model."""
        if not Path(model_path).exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
            
        with open(model_path, 'rb') as f:
            loaded_data = pickle.load(f)
            
        # Handle both new dictionary format and old direct model format
        if isinstance(loaded_data, dict):
            # New format with model, scaler, and normalization_stats
            self.model = loaded_data['model']
            self.scaler = loaded_data.get('scaler', None)
            self.normalization_stats = loaded_data.get('normalization_stats', None)
            logger.info(f"Loaded O19S corpus-aware model from {model_path} (new format with scaler)")
            if self.normalization_stats:
                logger.info("Normalization statistics available - model trained with all 6 combinations")
                # Log which combinations were selected most often during training
                total_selections = sum(sum(comb.values()) for comb in self.normalization_stats.values())
                if total_selections > 0:
                    logger.info("Training combination selection statistics:")
                    for norm in self.normalization_stats:
                        for comb in self.normalization_stats[norm]:
                            count = self.normalization_stats[norm][comb]
                            percentage = (count / total_selections * 100) if total_selections > 0 else 0
                            if count > 0:
                                logger.info(f"  {norm}/{comb}: {count} times ({percentage:.1f}%)")
        else:
            # Old format - just the model directly
            self.model = loaded_data
            self.scaler = None
            self.normalization_stats = None
            logger.info(f"Loaded O19S corpus-aware model from {model_path} (legacy format)")
            
        logger.info(f"Model type: {type(self.model)}")
        logger.info(f"Expected features: {self.model.n_features_in_}")
        
        if self.model.n_features_in_ < 18:
            logger.warning(f"Model expects {self.model.n_features_in_} features, but corpus-aware model should have 18+")
        
        return self.model
    
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
    
    def predict_best_weight(self, query: str, weights_to_test: List[float] = None) -> Tuple[float, float, Dict[str, float]]:
        """Predict best weight using O19S methodology with real-time corpus features."""
        if weights_to_test is None:
            weights_to_test = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        
        all_predictions = {}
        best_weight = 0.5
        best_predicted_ndcg = -1
        
        for weight in weights_to_test:
            # Extract corpus-aware features including weight
            features_dict = self.feature_extractor.extract_features(query, weight)
            
            # Convert to feature vector in O19S training order
            feature_vector = [
                features_dict['f_0_neuralness'],  # Weight (FIRST)
                features_dict['f_2_query_length'],
                features_dict['f_4_has_special_char'],
                features_dict['f_5_has_punctuation_at_end'],
                features_dict['f_7_capital_letters_ratio'],
                features_dict['f_8_stopwords_ratio'],
                features_dict['f_14_max_document_frequency'],
                features_dict['f_15_min_document_frequency'],
                features_dict['f_16_total_document_frequency'],
                features_dict['f_17_average_document_frequency'],
                features_dict['f_18_variance_document_frequency'],
                features_dict['f_19_std_dev_document_frequency'],
                features_dict['f_20_max_inverse_document_frequency'],
                features_dict['f_21_min_inverse_document_frequency'],
                features_dict['f_22_total_inverse_document_frequency'],
                features_dict['f_23_average_inverse_document_frequency'],
                features_dict['f_24_variance_inverse_document_frequency'],
                features_dict['f_25_std_dev_inverse_document_frequency']
            ]
            
            # Create DataFrame with feature names for sklearn compatibility
            feature_names = [
                'f_0_neuralness', 'f_2_query_length', 'f_4_has_special_char', 
                'f_5_has_punctuation_at_end', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
                'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
                'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
                'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
                'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
            ]
            
            feature_df = pd.DataFrame([feature_vector], columns=feature_names)
            
            # Apply scaler if available (from new training format)
            if hasattr(self, 'scaler') and self.scaler is not None:
                # Apply scaler to ALL features (no selective scaling - all features scaled identically)
                feature_scaled = self.scaler.transform(feature_df)
                feature_df = pd.DataFrame(feature_scaled, columns=feature_names)
            
            # Predict NDCG for this weight
            predicted_ndcg = self.model.predict(feature_df)[0]
            all_predictions[weight] = predicted_ndcg
            # print(f"Predicted NDCG for weight {weight}: {predicted_ndcg}")
            
            # Track best weight
            if predicted_ndcg > best_predicted_ndcg:
                best_predicted_ndcg = predicted_ndcg
                best_weight = weight
        
        return best_weight, best_predicted_ndcg, all_predictions
    
    def evaluate(self,
                 o19s_data_path: str,
                 ratings_file: str,
                 sample_size: Optional[int] = None,
                 compare_with_static: bool = True,
                 custom_weights: Optional[List[float]] = None) -> Dict:
        """Evaluate O19S corpus-aware model using real-time feature collection."""
        logger.info("Evaluating O19S corpus-aware model with real-time corpus features...")
        
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
            logger.info(f"Sampled {len(test_queries)} test queries for development")
        else:
            logger.info(f"Using all {len(test_queries)} O19S test queries")
        
        # Filter to queries that have ratings
        test_queries_with_ratings = [q for q in test_queries if q in reference]
        logger.info(f"Test queries with ratings: {len(test_queries_with_ratings)}")
        
        # Evaluate dynamic weights with real-time corpus features
        logger.info("Evaluating O19S corpus-aware model predictions...")
        if custom_weights:
            logger.info(f"Using custom weight range: {custom_weights}")
            
        dynamic_metrics = self._evaluate_dynamic_weights(
            test_queries_with_ratings, reference, custom_weights
        )
        
        # Create results
        results = {
            'dynamic_performance': dynamic_metrics,
            'evaluation_config': {
                'test_queries': len(test_queries_with_ratings),
                'model_type': f"O19S Corpus-Aware ({self.model.n_features_in_} features)",
                'features_used': self.model.n_features_in_,
                'corpus_field': self.corpus_field,
                'realtime_corpus_queries': True
            }
        }
        
        # Compare with static weights if requested
        if compare_with_static:
            logger.info("Comparing with static weight baselines...")
            
            static_weights = [round(0.1 + i * 0.1, 1) for i in range(9)]
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
            
            logger.info(f"O19S corpus-aware vs Static (0.5) NDCG improvement: {improvement:.2f}%")
        
        # Add corpus statistics summary
        cache_stats = self.feature_extractor.get_cache_stats()
        results['corpus_statistics'] = cache_stats
        
        return results
    
    def _evaluate_dynamic_weights(self, test_queries: List[str], reference: Dict, custom_weights: Optional[List[float]] = None) -> Dict:
        """Evaluate with dynamic weight prediction using real-time corpus features"""
        
        all_metrics = []
        queries_evaluated = 0
        weight_predictions = []
        prediction_details = []
        corpus_query_count = 0
        
        start_time = time.time()
        
        for query_string in tqdm(test_queries, desc="O19S corpus-aware evaluation"):
            if query_string not in reference:
                continue
                
            try:
                # Track corpus queries made
                pre_cache_size = len(self.feature_extractor._corpus_cache)
                
                # Predict optimal weight using real-time corpus features
                predicted_weight, predicted_ndcg, all_predictions = self.predict_best_weight(query_string, custom_weights)
                
                # Track corpus queries made
                post_cache_size = len(self.feature_extractor._corpus_cache)
                corpus_query_count += (post_cache_size - pre_cache_size)
                
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
                    # Merge with ratings and calculate metrics
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
                        query_metrics = {
                            'dcg': metrics.dcg_at_10(df_with_ratings),
                            'ndcg': metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string]),
                            'prec@10': metrics.precision_at_k(df_with_ratings),
                            'ratio_of_ratings': metrics.ratio_of_ratings(df_with_ratings)
                        }
                        all_metrics.append(query_metrics)
                        queries_evaluated += 1
                
            except Exception as e:
                logger.warning(f"Evaluation failed for query '{query_string}': {e}")
                continue
        
        evaluation_time = time.time() - start_time
        
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
            'evaluation_time_seconds': evaluation_time,
            'corpus_queries_made': corpus_query_count,
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
                    # Merge with ratings and calculate metrics
                    df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                    
                    if not df_with_ratings.empty:
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
                "description": "O19S corpus-aware evaluation",
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
    """Print evaluation results with corpus statistics"""
    
    print("\n" + "="*80)
    print("O19S CORPUS-AWARE EVALUATION RESULTS")
    print("="*80)
    
    config = results['evaluation_config']
    print(f"Test queries evaluated: {config['test_queries']}")
    print(f"Model type: {config['model_type']}")
    print(f"Features used: {config['features_used']}")
    print(f"Corpus field: {config['corpus_field']}")
    print("Real-time corpus queries: TRUE")
    
    dynamic = results['dynamic_performance']
    print(f"\nCORPUS-AWARE MODEL PERFORMANCE:")
    print(f"  Average DCG: {dynamic['avg_dcg']:.2f}")
    print(f"  Average NDCG: {dynamic['avg_ndcg']:.4f}")
    print(f"  Average Precision@10: {dynamic['avg_precision']:.4f}")
    print(f"  Ratio of Ratings: {dynamic['avg_ratio_of_ratings']:.4f}")
    print(f"  Queries evaluated: {dynamic['queries_evaluated']}")
    print(f"  Evaluation time: {dynamic['evaluation_time_seconds']:.1f}s")
    print(f"  Corpus queries made: {dynamic['corpus_queries_made']}")
    
    weight_pred = dynamic['weight_predictions']
    print("\nWEIGHT PREDICTIONS:")
    print(f"  Mean predicted weight: {weight_pred['mean']:.3f}")
    print(f"  Weight std: {weight_pred['std']:.3f}")
    print(f"  Weight range: [{weight_pred['min']:.2f}, {weight_pred['max']:.2f}]")
    
    print("\nWeight Distribution:")
    for weight, count in weight_pred['distribution'].items():
        percentage = (count / dynamic['queries_evaluated']) * 100 if dynamic['queries_evaluated'] > 0 else 0
        print(f"  Weight {weight}: {count} queries ({percentage:.1f}%)")
    
    if 'corpus_statistics' in results:
        corpus_stats = results['corpus_statistics']
        print(f"\nCORPUS STATISTICS CACHE:")
        print(f"  Cached terms: {corpus_stats.get('cached_terms', 0)}")
        print(f"  Cache size: {corpus_stats.get('cache_size_mb', 0):.2f} MB")
    
    if 'static_baselines' in results:
        print("\nSTATIC WEIGHT BASELINES:")
        for name, metrics in results['static_baselines'].items():
            weight = metrics['static_weight']
            print(f"  Static {weight}: NDCG {metrics['avg_ndcg']:.4f}")
        
        print(f"\nIMPROVEMENT OVER STATIC (0.5): {results['improvement_over_static']:+.2f}%")
    
    print("\n" + "="*80)
    print("✅ REAL-TIME CORPUS FEATURES: This evaluation uses actual OpenSearch")
    print("   queries to collect document frequencies and term statistics,")
    print("   matching O19S's true implementation approach.")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate O19S corpus-aware models with real-time feature collection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test 18-feature corpus-aware model
  python evaluate_o19s_corpus_aware.py \\
    --model-file models/regression_model-2025_large_qs-08-28.pkl \\
    --model-id huggingface/sentence-transformers/all-MiniLM-L6-v2 \\
    --sample-size 100

  # Full evaluation with custom corpus field  
  python evaluate_o19s_corpus_aware.py \\
    --model-file models/regression_model-2025_large_qs-08-28.pkl \\
    --model-id huggingface/sentence-transformers/all-MiniLM-L6-v2 \\
    --corpus-field product_title \\
    --custom-weights "0.2,0.4,0.6,0.8"
        """
    )
    
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products', help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--model-file', 
                       default='models/regression_model-2025_large_qs-08-28.pkl',
                       help='Path to O19S corpus-aware model pkl file')
    parser.add_argument('--corpus-field', default='product_title',
                       help='Field to analyze for corpus statistics')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to O19S ratings.csv file')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of test queries to sample (default: None = use all)')
    parser.add_argument('--no-static-comparison', action='store_true',
                       help='Skip comparison with static weights')
    parser.add_argument('--output', default='o19s_corpus_aware_evaluation_results.json',
                       help='Output results file')
    parser.add_argument('--custom-weights', type=str, default=None,
                       help='Custom weights to test (comma-separated, e.g., "0.2,0.4,0.6,0.8")')
    
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
    evaluator = O19SCorpusAwareEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        corpus_field=args.corpus_field
    )
    
    # Load model
    model = evaluator.load_model(args.model_file)
    
    # Log what we're about to do
    logger.info("🔍 REAL-TIME CORPUS FEATURES: This will query OpenSearch to collect")
    logger.info("   document frequencies and term statistics during evaluation.")
    logger.info("   This matches O19S's actual implementation approach.")
    
    # Run evaluation
    start_time = time.time()
    results = evaluator.evaluate(
        o19s_data_path=args.o19s_data,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        compare_with_static=not args.no_static_comparison,
        custom_weights=custom_weights
    )
    total_time = time.time() - start_time
    
    # Add timing information
    results['total_evaluation_time_seconds'] = total_time
    
    # Print results
    print_evaluation_results(results)
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Evaluation results saved to {args.output}")
    
    # Summary
    dynamic_perf = results['dynamic_performance']
    logger.info(f"\n🎯 FINAL RESULTS SUMMARY:")
    logger.info(f"   NDCG@10: {dynamic_perf['avg_ndcg']:.4f}")
    logger.info(f"   Queries evaluated: {dynamic_perf['queries_evaluated']}")
    logger.info(f"   Corpus queries made: {dynamic_perf['corpus_queries_made']}")
    logger.info(f"   Terms cached: {results['corpus_statistics']['cached_terms']}")
    logger.info(f"   Total time: {total_time:.1f}s")


if __name__ == "__main__":
    main()
