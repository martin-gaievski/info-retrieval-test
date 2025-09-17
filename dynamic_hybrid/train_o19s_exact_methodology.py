#!/usr/bin/env python3
"""
Train O19S Exact Methodology Model

This script trains a Ridge regression model using the EXACT O19S approach:
1. For each query, tests ALL 66 combinations (6 techniques × 11 weights)
2. Selects the SINGLE BEST result across all combinations
3. Creates ONE training sample per query with optimal weight
4. Model learns: Query features → Optimal weight for best NDCG
5. Evaluation uses fixed l2/arithmetic_mean with predicted weight

This is the true O19S methodology where the model predicts the optimal weight.

Author: Dynamic Hybrid Search Team  
Version: 3.0.0 - O19S EXACT global optimization methodology
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
import string
import re
import itertools
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split, ShuffleSplit, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, make_scorer
from sklearn.preprocessing import StandardScaler

# Handle sklearn version compatibility for root_mean_squared_error
try:
    from sklearn.metrics import root_mean_squared_error
except ImportError:
    # For older sklearn versions, define it manually
    def root_mean_squared_error(y_true, y_pred):
        return np.sqrt(mean_squared_error(y_true, y_pred))

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


class O19SExactTrainer:
    """Train O19S models using EXACT global optimization methodology"""
    
    # O19S exact list of common English stopwords
    STOPWORDS = {
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
        "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
        "their", "then", "there", "these", "they", "this", "to", "was", "will",
        "with", "without"
    }
    
    # O19S normalization and combination techniques
    NORMALIZATION_TECHNIQUES = ['min_max', 'l2']
    COMBINATION_TECHNIQUES = ['arithmetic_mean', 'harmonic_mean', 'geometric_mean']
    # O19S uses 11 weights from 0.0 to 1.0
    WEIGHT_VALUES = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 corpus_field: str = "product_title"):
        """Initialize O19S exact trainer"""
        
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
        
        # Statistics tracking
        self.optimal_weights_distribution = []  # Track which weights are optimal
        self.optimal_techniques_distribution = []  # Track which techniques are optimal
        self.total_combinations_tested = 0
        
        # Initialize corpus info
        self._initialize_corpus_info()
        
        logger.info(f"Initialized O19S EXACT trainer for {host}:{port}/{index_name}")
        logger.info(f"Corpus field: {corpus_field}")
        logger.info(f"Total documents: {self._total_docs}")
        logger.info(f"Will test {len(self.NORMALIZATION_TECHNIQUES) * len(self.COMBINATION_TECHNIQUES) * len(self.WEIGHT_VALUES)} = 66 combinations per query")
    
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
        """
        Checks if a string ends with a punctuation character.
        O19S exact implementation.
        """
        stripped_text = text.strip()
        if not stripped_text:
            return 0
        return 1 if stripped_text[-1] in string.punctuation else 0
    
    def unique_terms_ratio(self, text: str) -> float:
        """
        Calculates the ratio of unique terms to the total number of terms in a string.
        O19S exact implementation.
        """
        preprocessed_text = text.lower()
        terms = re.findall(r'\b\w+\b', preprocessed_text)
        
        if not terms:
            return 0.0
        
        unique_terms = set(terms)
        return len(unique_terms) / len(terms)
    
    def capital_letters_ratio(self, text: str) -> float:
        """
        Calculates the ratio of capital letters to the total number of characters in a string.
        O19S exact implementation.
        """
        if not text:
            return 0.0
        
        capital_count = sum(1 for char in text if char.isupper())
        return capital_count / len(text)
    
    def stopwords_ratio(self, text: str) -> float:
        """
        Calculates the ratio of stopwords to the total number of terms in a string.
        O19S exact implementation.
        """
        preprocessed_text = text.lower()
        terms = re.findall(r'\b\w+\b', preprocessed_text)
        
        if not terms:
            return 0.0
        
        stopword_count = sum(1 for term in terms if term in self.STOPWORDS)
        return stopword_count / len(terms)
    
    def collect_training_data_exact(self, 
                                   o19s_data_path: str,
                                   ratings_file: str,
                                   sample_size: Optional[int] = None,
                                   use_fixed_queries: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Collect training data using O19S EXACT methodology.
        
        For each query in training set:
        1. Test ALL 66 combinations (6 techniques × 11 weights)
        2. Select the SINGLE BEST result across all combinations
        3. Extract features using the optimal weight from best combination
        4. Create ONE training sample per query: features -> best_NDCG
        
        Args:
            o19s_data_path: Path to O19S data
            ratings_file: Path to ratings file
            sample_size: Number of queries to use for training
            use_fixed_queries: If True, use fixed order from CSV
            
        Returns:
            (X_features, y_ndcg): Training features and target NDCG values
        """
        
        logger.info(f"Collecting training data using O19S EXACT methodology")
        logger.info(f"Testing ALL 66 combinations per query (6 techniques × 11 weights)")
        
        # Load O19S data
        train_file = Path(o19s_data_path) / 'query_train.csv'
        if not train_file.exists():
            raise FileNotFoundError(f"O19S train queries not found: {train_file}")
            
        df_train = pd.read_csv(train_file)
        
        # Load ratings
        if not Path(ratings_file).exists():
            raise FileNotFoundError(f"Ratings file not found: {ratings_file}")
            
        df_ratings = pd.read_csv(ratings_file, sep="\t", names=['query', 'docid', 'rating', 'idx'])
        
        # Create reference dictionary
        reference = {query: df for query, df in df_ratings.groupby("query")}
        logger.info(f"Created reference for {len(reference)} queries")
        
        # Get training queries
        train_queries = df_train['query_string'].tolist()
        
        if use_fixed_queries:
            # Use fixed queries from CSV in order (no random sampling)
            if sample_size and sample_size < len(train_queries):
                train_queries = train_queries[:sample_size]
                logger.info(f"Using first {sample_size} queries from fixed dataset")
            else:
                logger.info(f"Using all {len(train_queries)} queries from fixed dataset")
        else:
            # Random sampling
            if sample_size and sample_size < len(train_queries):
                np.random.seed(42)
                train_queries = np.random.choice(train_queries, size=sample_size, replace=False).tolist()
                logger.info(f"Randomly sampled {sample_size} training queries")
            else:
                logger.info(f"Using all {len(train_queries)} training queries")
        
        # Filter to queries with ratings
        train_queries_with_ratings = [q for q in train_queries if q in reference]
        logger.info(f"Training queries with ratings: {len(train_queries_with_ratings)}")
        
        # Collect training samples - ONE per query
        X_features = []
        y_ndcg = []
        
        logger.info(f"Expected training samples: {len(train_queries_with_ratings)} (one per query)")
        
        for query_idx, query_string in enumerate(tqdm(train_queries_with_ratings, desc="Collecting training data")):
            if query_string not in reference:
                continue
            
            # Test ALL 66 combinations for this query
            best_result = {
                'ndcg': 0.0,
                'weight': 0.5,  # Default
                'normalization': 'l2',
                'combination': 'arithmetic_mean'
            }
            
            for weight in self.WEIGHT_VALUES:
                neural_weight = weight
                lexical_weight = round(1.0 - weight, 2)
                
                for normalization, combination in itertools.product(self.NORMALIZATION_TECHNIQUES, 
                                                                   self.COMBINATION_TECHNIQUES):
                    try:
                        # Run hybrid search with this specific configuration
                        search_results = self._execute_hybrid_search(
                            query_string, lexical_weight, neural_weight, 
                            normalization, combination
                        )
                        
                        if not search_results.empty:
                            # Calculate actual NDCG for this configuration
                            df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                            
                            if not df_with_ratings.empty:
                                actual_ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                                
                                # Track if this is the globally best configuration
                                if actual_ndcg > best_result['ndcg']:
                                    best_result = {
                                        'ndcg': actual_ndcg,
                                        'weight': weight,
                                        'normalization': normalization,
                                        'combination': combination
                                    }
                                
                                self.total_combinations_tested += 1
                    
                    except Exception as e:
                        logger.debug(f"Failed {weight}/{normalization}/{combination}: {e}")
                        continue
            
            # Create ONE training sample with the optimal configuration
            if best_result['ndcg'] > 0:
                # Extract features using the OPTIMAL weight
                features = self._extract_o19s_18_features(query_string, best_result['weight'])
                
                # Add training sample
                X_features.append(features)
                y_ndcg.append(best_result['ndcg'])
                
                # Track statistics
                self.optimal_weights_distribution.append(best_result['weight'])
                self.optimal_techniques_distribution.append(f"{best_result['normalization']}/{best_result['combination']}")
                
                logger.debug(f"Query '{query_string}': Best weight={best_result['weight']:.1f}, "
                           f"technique={best_result['normalization']}/{best_result['combination']}, "
                           f"NDCG={best_result['ndcg']:.4f}")
        
        logger.info(f"Collected {len(X_features)} training samples (one per query)")
        logger.info(f"Total combinations tested: {self.total_combinations_tested}")
        
        # Report optimal weight distribution
        if self.optimal_weights_distribution:
            weight_counts = pd.Series(self.optimal_weights_distribution).value_counts().sort_index()
            logger.info("\nOptimal weight distribution:")
            for weight, count in weight_counts.items():
                percentage = (count / len(self.optimal_weights_distribution)) * 100
                logger.info(f"  Weight {weight:.1f}: {count} queries ({percentage:.1f}%)")
        
        # Report optimal technique distribution
        if self.optimal_techniques_distribution:
            technique_counts = pd.Series(self.optimal_techniques_distribution).value_counts()
            logger.info("\nOptimal technique distribution:")
            for technique, count in technique_counts.items():
                percentage = (count / len(self.optimal_techniques_distribution)) * 100
                logger.info(f"  {technique}: {count} queries ({percentage:.1f}%)")
        
        if len(X_features) == 0:
            raise ValueError("No training samples collected")
            
        return np.array(X_features), np.array(y_ndcg)
    
    def _extract_o19s_18_features(self, query: str, weight: float) -> List[float]:
        """Extract 18 features using O19S exact methodology."""
        
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
        
        # Create 18-feature vector in O19S training order
        feature_vector = [
            weight,  # f_0_neuralness (neural_search_weight) - THIS IS THE OPTIMAL WEIGHT
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
            
            logger.debug(f"Collected corpus features for: {query_string}")
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
    
    def train_model(self,
                   X_features: np.ndarray,
                   y_ndcg: np.ndarray,
                   alpha: float = 10.0,
                   test_size: float = 0.2,
                   use_cross_validation: bool = True) -> Ridge:
        """
        Train Ridge regression model using O19S exact methodology.
        
        Note: Since we have ONE sample per query, we don't need query-based splitting.
        """
        
        logger.info(f"Training Ridge regression model (O19S EXACT methodology)...")
        logger.info(f"  Training samples: {len(X_features)} (one per query)")
        logger.info(f"  Features: {X_features.shape[1]}")
        logger.info(f"  Alpha (regularization): {alpha}")
        logger.info(f"  Test split: {test_size}")
        logger.info(f"  Cross-validation: {use_cross_validation}")
        
        # Create feature names
        feature_names = [
            'f_0_neuralness', 'f_2_query_length', 'f_4_has_special_char', 
            'f_5_has_punctuation_at_end', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
            'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
            'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
            'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
            'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
        ]
        
        # Convert to DataFrame for sklearn compatibility
        X_df = pd.DataFrame(X_features, columns=feature_names)
        
        # Add feature normalization (O19S likely uses this for better numerical stability)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_df)
        X_df = pd.DataFrame(X_scaled, columns=feature_names)
        
        # Simple train/test split (one sample per query)
        X_train, X_test, y_train, y_test = train_test_split(
            X_df, y_ndcg, test_size=test_size, random_state=0
        )
        
        logger.info(f"  Train samples: {len(X_train)}")
        logger.info(f"  Test samples: {len(X_test)}")
        
        # Train Ridge regression model
        model = Ridge(alpha=alpha, solver='auto', random_state=0)
        
        if use_cross_validation:
            # O19S uses ShuffleSplit cross-validation with 5 splits
            logger.info("Performing ShuffleSplit cross-validation (O19S method)")
            cv = ShuffleSplit(n_splits=5, test_size=test_size, random_state=0)
            
            # Define scorer for RMSE (O19S uses this)
            rmse_scorer = make_scorer(root_mean_squared_error, greater_is_better=False)
            
            # Perform cross-validation
            cv_scores = cross_val_score(model, X_df, y_ndcg, cv=cv, scoring=rmse_scorer)
            logger.info(f"  Cross-validation RMSE: {-cv_scores.mean():.6f} (+/- {cv_scores.std() * 2:.6f})")
            
            # Also evaluate with R² scoring
            cv_r2_scores = cross_val_score(model, X_df, y_ndcg, cv=cv, scoring='r2')
            logger.info(f"  Cross-validation R²: {cv_r2_scores.mean():.6f} (+/- {cv_r2_scores.std() * 2:.6f})")
        
        # Fit final model on training data
        model.fit(X_train, y_train)
        
        # Validate model
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        train_mse = mean_squared_error(y_train, y_pred_train)
        test_mse = mean_squared_error(y_test, y_pred_test)
        train_rmse = root_mean_squared_error(y_train, y_pred_train)
        test_rmse = root_mean_squared_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        logger.info(f"\nFinal model performance:")
        logger.info(f"  Train MSE: {train_mse:.6f}, RMSE: {train_rmse:.6f}")
        logger.info(f"  Test MSE: {test_mse:.6f}, RMSE: {test_rmse:.6f}")
        logger.info(f"  Train R²: {train_r2:.6f}")
        logger.info(f"  Test R²: {test_r2:.6f}")
        
        # Analyze feature coefficients (on original scale for interpretability)
        logger.info(f"\nFeature coefficients:")
        for i, (name, coef) in enumerate(zip(feature_names, model.coef_)):
            # Adjust coefficient for scaled features
            original_scale_coef = coef / scaler.scale_[i] if scaler.scale_[i] != 0 else coef
            logger.info(f"  {name}: {original_scale_coef:.6f}")
        logger.info(f"  Intercept: {model.intercept_:.6f}")
        
        # Check weight sensitivity - CRITICAL for O19S methodology
        weight_coef = model.coef_[0] / scaler.scale_[0] if scaler.scale_[0] != 0 else model.coef_[0]
        logger.info(f"\nWeight (f_0_neuralness) coefficient: {weight_coef:.6f}")
        logger.info("NOTE: Weight is an INPUT feature. Model predicts NDCG scores!")
        logger.info("Negative coefficient suggests lower weights produce higher NDCG.")
        
        # Store scaler with model for use during inference
        self.scaler = scaler
        self.model = model
        
        return model
    
    def _execute_hybrid_search(self, query: str, lexical_weight: float, neural_weight: float, 
                               normalization: str = 'l2', combination: str = 'arithmetic_mean') -> pd.DataFrame:
        """Execute hybrid search with specified normalization and combination technique."""
        
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
                "description": f"O19S EXACT with {normalization}/{combination}",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": normalization},
                            "combination": {
                                "technique": combination,
                                "parameters": {"weights": [lexical_weight, neural_weight]}
                            }
                        }
                    }
                ]
            },
            "size": 100
        }
        
        try:
            # Use OpenSearch client which preserves hostname case
            result = self.client.search(index=self.index_name, body=payload)
            
            # Parse results into DataFrame
            hits = result.get('hits', {}).get('hits', [])
            
            if not hits:
                return pd.DataFrame()
            
            # Extract relevant fields
            data = []
            for hit in hits:
                doc = {
                    'product_id': hit['_source'].get('product_id', ''),
                    'score': hit.get('_score', 0.0)
                }
                data.append(doc)
            
            df = pd.DataFrame(data)
            return df
            
        except Exception as e:
            logger.debug(f"Search failed: {e}")
            return pd.DataFrame()
    
    def _merge_results_with_reference(self, search_results: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
        """Merge search results with reference ratings."""
        
        if search_results.empty:
            return pd.DataFrame()
        
        # Rename columns for consistency
        search_results = search_results.rename(columns={'product_id': 'docid'})
        
        # Add position column (required for NDCG calculation)
        search_results['position'] = range(1, len(search_results) + 1)
        
        # Merge with reference ratings
        merged = pd.merge(
            search_results,
            reference[['docid', 'rating']],
            on='docid',
            how='left'
        )
        
        # Fill missing ratings with 0
        merged['rating'] = merged['rating'].fillna(0)
        
        return merged
    
    def save_model(self, model: Ridge, output_path: str):
        """Save trained model and configuration."""
        
        # Prepare model package
        model_data = {
            'model': model,
            'scaler': self.scaler,
            'feature_names': [
                'f_0_neuralness', 'f_2_query_length', 'f_4_has_special_char', 
                'f_5_has_punctuation_at_end', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
                'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
                'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
                'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
                'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
            ],
            'training_stats': {
                'optimal_weights_distribution': self.optimal_weights_distribution,
                'optimal_techniques_distribution': self.optimal_techniques_distribution,
                'total_combinations_tested': self.total_combinations_tested
            },
            'methodology': 'o19s_exact_global_optimization',
            'version': '3.0.0'
        }
        
        # Save model
        with open(output_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"Model saved to {output_path}")
        
        # Also save statistics summary
        stats_path = output_path.replace('.pkl', '_stats.json')
        
        # Calculate weight distribution stats
        weight_counts = {}
        if self.optimal_weights_distribution:
            for w in self.optimal_weights_distribution:
                weight_counts[str(w)] = weight_counts.get(str(w), 0) + 1
        
        # Calculate technique distribution stats
        technique_counts = {}
        if self.optimal_techniques_distribution:
            for t in self.optimal_techniques_distribution:
                technique_counts[t] = technique_counts.get(t, 0) + 1
        
        stats_summary = {
            'training_samples': len(self.optimal_weights_distribution),
            'total_combinations_tested': self.total_combinations_tested,
            'optimal_weight_distribution': weight_counts,
            'optimal_technique_distribution': technique_counts,
            'methodology': 'o19s_exact_global_optimization'
        }
        
        with open(stats_path, 'w') as f:
            json.dump(stats_summary, f, indent=2)
        
        logger.info(f"Training statistics saved to {stats_path}")


def main():
    """Main training function for O19S EXACT methodology."""
    
    parser = argparse.ArgumentParser(description='Train O19S model using EXACT global optimization methodology')
    parser.add_argument('--host', type=str, default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('--index-name', type=str, default='esci-products',
                       help='Index name')
    parser.add_argument('--model-id', type=str, required=True,
                       help='Neural model ID')
    parser.add_argument('--o19s-data-path', type=str, default='dynamic_hybrid/data',
                       help='Path to O19S data')
    parser.add_argument('--ratings-file', type=str, default='dynamic_hybrid/data/esci_dataset_ratings.tsv',
                       help='Path to ratings file')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of queries to use for training')
    parser.add_argument('--use-fixed-queries', action='store_true',
                       help='Use fixed queries from CSV in order')
    parser.add_argument('--alpha', type=float, default=10.0,
                       help='Ridge regression regularization parameter')
    parser.add_argument('--test-size', type=float, default=0.2,
                       help='Test set size')
    parser.add_argument('--use-cross-validation', action='store_true', default=True,
                       help='Use cross-validation')
    parser.add_argument('--output-model', type=str, default='o19s_exact_methodology_model.pkl',
                       help='Output model file path')
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("O19S EXACT Methodology Training")
    logger.info("=" * 80)
    logger.info(f"Global optimization: Test ALL 66 combinations per query")
    logger.info(f"Target: Model predicts optimal weight for best NDCG")
    logger.info("=" * 80)
    
    # Initialize trainer
    trainer = O19SExactTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index_name,
        model_id=args.model_id
    )
    
    # Collect training data using EXACT methodology
    logger.info("\n" + "=" * 80)
    logger.info("Phase 1: Collecting Training Data (O19S EXACT)")
    logger.info("=" * 80)
    
    X_features, y_ndcg = trainer.collect_training_data_exact(
        o19s_data_path=args.o19s_data_path,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        use_fixed_queries=args.use_fixed_queries
    )
    
    # Train model
    logger.info("\n" + "=" * 80)
    logger.info("Phase 2: Training Ridge Regression Model")
    logger.info("=" * 80)
    
    model = trainer.train_model(
        X_features=X_features,
        y_ndcg=y_ndcg,
        alpha=args.alpha,
        test_size=args.test_size,
        use_cross_validation=args.use_cross_validation
    )
    
    # Save model
    logger.info("\n" + "=" * 80)
    logger.info("Phase 3: Saving Model")
    logger.info("=" * 80)
    
    trainer.save_model(model, args.output_model)
    
    logger.info("\n" + "=" * 80)
    logger.info("Training Complete!")
    logger.info("=" * 80)
    logger.info(f"Model saved to: {args.output_model}")
    logger.info(f"Use with evaluate_o19s_exact_methodology.py for evaluation")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
