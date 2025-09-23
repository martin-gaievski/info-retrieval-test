#!/usr/bin/env python3
"""
Train O19S Weight Predictor with Linear Regression (No Regularization)

This script trains a simple Linear Regression model (no regularization) to predict optimal weight:
1. Uses 17 features (5 query + 12 real-time corpus features) - NO weight as feature
2. For each query, finds the weight that produces highest NDCG
3. Trains model to predict this optimal weight given the 17 features
4. Target is the weight itself (not NDCG)

This version uses LinearRegression instead of Ridge to test if regularization is the problem.

Author: Dynamic Hybrid Search Team  
Version: 1.0.0 - Linear Regression (no regularization) version
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
from sklearn.linear_model import LinearRegression  # Using LinearRegression instead of Ridge
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


class O19SLinearRegressionTrainer:
    """Train model to predict optimal weight using Linear Regression (no regularization)"""
    
    # O19S exact list of common English stopwords
    STOPWORDS = {
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
        "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
        "their", "then", "there", "these", "they", "this", "to", "was", "will",
        "with", "without"
    }
    
    # O19S normalization and combination techniques
    NORMALIZATION_TECHNIQUES = ['l2']
    COMBINATION_TECHNIQUES = ['arithmetic_mean']
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 corpus_field: str = "product_title"):
        """Initialize linear regression trainer"""
        
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
        self.normalization_stats = {norm: {comb: 0 for comb in self.COMBINATION_TECHNIQUES} 
                                   for norm in self.NORMALIZATION_TECHNIQUES}
        self.total_combinations_tested = 0
        
        # Initialize corpus info
        self._initialize_corpus_info()
        
        logger.info(f"Initialized O19S LINEAR REGRESSION trainer for {host}:{port}/{index_name}")
        logger.info(f"Using LinearRegression (NO REGULARIZATION)")
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
    
    def collect_training_data(self, 
                             o19s_data_path: str,
                             ratings_file: str,
                             sample_size: Optional[int] = None,
                             weights_to_test: Optional[List[float]] = None,
                             use_fixed_queries: bool = False) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Collect training data for weight prediction.
        
        For each query in training set:
        1. Extract 17 features (NO weight feature)
        2. Test multiple weights and find which produces highest NDCG
        3. Create training sample: 17 features -> best_weight
        
        Args:
            o19s_data_path: Path to O19S data
            ratings_file: Path to ratings file
            sample_size: Number of queries to use for training
            weights_to_test: Weights to test per query
            use_fixed_queries: If True, use fixed order from CSV
            
        Returns:
            (X_features, y_weights, query_ids): Training features (17), target weights, and query IDs
        """
        
        if weights_to_test is None:
            weights_to_test = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        
        logger.info(f"Collecting training data with weights: {weights_to_test}")
        logger.info(f"Predicting optimal weight (not NDCG) from 17 features")
        logger.info(f"Using LINEAR REGRESSION (no regularization)")
        
        # Load O19S data
        train_file = Path(o19s_data_path) / 'query_train.csv'
        if not train_file.exists():
            raise FileNotFoundError(f"O19S train queries not found: {train_file}")
            
        df_train = pd.read_csv(train_file)
        
        # Load ratings - Handle tab-delimited file with no headers
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
        
        # First try to map as numeric
        df_ratings['esci_label_numeric'] = pd.to_numeric(df_ratings['esci_label'], errors='coerce')
        
        # If numeric conversion worked, use numeric mapping
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
        
        logger.info(f"Loaded {len(df_ratings)} rating entries")
        
        # Log label distribution after mapping
        rating_counts = df_ratings['rating'].value_counts().sort_index()
        logger.info("Rating distribution after mapping:")
        for rating, count in rating_counts.items():
            percentage = (count / len(df_ratings)) * 100
            logger.info(f"  Rating {rating}: {count} entries ({percentage:.1f}%)")
        
        # Check for any unmapped ratings (NaN)
        nan_count = df_ratings['rating'].isna().sum()
        if nan_count > 0:
            logger.warning(f"Found {nan_count} unmapped ratings (NaN values)")
            unmapped_samples = df_ratings[df_ratings['rating'].isna()].head(5)
            logger.warning(f"Sample unmapped labels: {unmapped_samples['esci_label'].tolist()}")
        
        # Create reference dictionary grouped by query
        reference = {}
        for query_string, group in df_ratings.groupby('query_string'):
            reference[query_string] = group[['product_id', 'rating']].rename(columns={'product_id': 'docid'})
        
        logger.info(f"Created reference for {len(reference)} unique queries")
        
        # Get training queries
        train_queries = df_train['query_string'].tolist()
        
        if use_fixed_queries:
            if sample_size and sample_size < len(train_queries):
                train_queries = train_queries[:sample_size]
                logger.info(f"Using first {sample_size} queries from fixed dataset")
            else:
                logger.info(f"Using all {len(train_queries)} queries from fixed dataset")
        else:
            if sample_size and sample_size < len(train_queries):
                np.random.seed(42)
                train_queries = np.random.choice(train_queries, size=sample_size, replace=False).tolist()
                logger.info(f"Randomly sampled {sample_size} training queries")
            else:
                logger.info(f"Using all {len(train_queries)} training queries")
        
        # Filter to queries with ratings
        train_queries_with_ratings = [q for q in train_queries if q in reference]
        logger.info(f"Training queries with ratings: {len(train_queries_with_ratings)}")
        
        # Collect training samples
        X_features = []
        y_weights = []  # Target is weight, not NDCG
        query_ids = []
        
        logger.info(f"Expected training samples: {len(train_queries_with_ratings)} (one per query)")
        
        samples_collected = 0
        
        for query_idx, query_string in enumerate(tqdm(train_queries_with_ratings, desc="Collecting weight prediction data")):
            if query_string not in reference:
                continue
            
            try:
                # Extract 17 features (NO weight) for this query
                features = self._extract_17_features(query_string)
                
                # Test all weights to find which produces best NDCG
                best_weight = 0.5  # Default
                best_ndcg = 0.0
                best_combination = ('l2', 'arithmetic_mean')
                
                for weight in weights_to_test:
                    lexical_weight = round(1.0 - weight, 2)
                    
                    for normalization, combination in itertools.product(self.NORMALIZATION_TECHNIQUES, 
                                                                       self.COMBINATION_TECHNIQUES):
                        try:
                            # Run hybrid search with this specific normalization/combination
                            search_results = self._execute_hybrid_search(
                                query_string, lexical_weight, weight, 
                                normalization, combination
                            )
                            
                            if not search_results.empty:
                                # Calculate actual NDCG for this combination
                                df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])

                                if not df_with_ratings.empty:
                                    actual_ndcg = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                                    
                                    # Track if this is the best weight for this query
                                    if actual_ndcg > best_ndcg:
                                        best_ndcg = actual_ndcg
                                        best_weight = weight
                                        best_combination = (normalization, combination)
                                    
                                    self.total_combinations_tested += 1
                        
                        except Exception as e:
                            logger.debug(f"Failed combination {normalization}/{combination} for weight {weight}: {e}")
                            continue
                
                # Add training sample: 17 features -> best weight
                if best_ndcg > 0:
                    X_features.append(features)
                    y_weights.append(best_weight)  # Target is weight, not NDCG
                    query_ids.append(query_string)
                    samples_collected += 1
                    
                    # Track which combination was best
                    self.normalization_stats[best_combination[0]][best_combination[1]] += 1
                    
                    logger.debug(f"Query '{query_string[:30]}...': best weight={best_weight:.1f}, best NDCG={best_ndcg:.4f}")
                
            except Exception as e:
                logger.warning(f"Failed to collect sample for query '{query_string}': {e}")
                continue
        
        logger.info(f"Collected {samples_collected} training samples (one per query)")
        logger.info(f"Unique queries in training data: {len(np.unique(query_ids))}")
        logger.info(f"Total combinations tested: {self.total_combinations_tested}")
        
        # Report weight distribution in training data (EXACT VALUES)
        if y_weights:
            weight_series = pd.Series(y_weights)
            weight_counts = weight_series.value_counts().sort_index()
            logger.info("\nOptimal weight distribution in training data (EXACT VALUES):")
            for weight, count in weight_counts.items():
                percentage = (count / len(y_weights)) * 100
                logger.info(f"  Weight {weight:.1f}: {count} queries ({percentage:.1f}%)")
            
            # Add summary statistics
            logger.info(f"\nTraining weights summary:")
            logger.info(f"  Mean: {weight_series.mean():.3f}, Std: {weight_series.std():.3f}")
            logger.info(f"  Min: {weight_series.min():.3f}, Max: {weight_series.max():.3f}")
            logger.info(f"  Unique values: {len(weight_counts)}")
            
            # Show distribution at extremes
            extreme_low = (weight_series <= 0.1).sum()
            extreme_high = (weight_series >= 0.9).sum()
            middle = ((weight_series > 0.1) & (weight_series < 0.9)).sum()
            logger.info(f"\nWeight clustering analysis:")
            logger.info(f"  Weights 0.0-0.1: {extreme_low} ({extreme_low/len(y_weights)*100:.1f}%)")
            logger.info(f"  Weights 0.2-0.8: {middle} ({middle/len(y_weights)*100:.1f}%)")
            logger.info(f"  Weights 0.9-1.0: {extreme_high} ({extreme_high/len(y_weights)*100:.1f}%)")
        
        if samples_collected == 0:
            raise ValueError("No training samples collected")
            
        return np.array(X_features), np.array(y_weights), np.array(query_ids)
    
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
                   y_weights: np.ndarray,
                   query_ids: np.ndarray = None,
                   test_size: float = 0.2,
                   use_cross_validation: bool = True) -> LinearRegression:
        """
        Train Linear Regression model to predict optimal weight.
        
        Args:
            X_features: Feature matrix (samples x 17 features)
            y_weights: Target weight values (what we're predicting)
            query_ids: Query identifiers for each sample
            test_size: Fraction for validation split
            use_cross_validation: If True, use ShuffleSplit cross-validation
            
        Returns:
            Trained Linear Regression model with feature scaler
        """
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Training LINEAR REGRESSION model (NO REGULARIZATION)")
        logger.info(f"{'='*60}")
        logger.info(f"  Training samples: {len(X_features)}")
        logger.info(f"  Features: {X_features.shape[1]} (no weight feature)")
        logger.info(f"  Target: optimal weight (not NDCG)")
        logger.info(f"  Model: LinearRegression (no alpha/regularization)")
        logger.info(f"  Test split: {test_size}")
        logger.info(f"  Cross-validation: {use_cross_validation}")
        
        # Create feature names (NO weight feature)
        feature_names = [
            'f_2_query_length', 'f_4_has_special_char', 
            'f_5_has_punctuation_at_end', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
            'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
            'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
            'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
            'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
        ]
        
        # Convert to DataFrame for sklearn compatibility
        X_df = pd.DataFrame(X_features, columns=feature_names)
        
        # Apply StandardScaler to ALL features
        logger.info("Applying StandardScaler to all 17 features...")
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_df)
        X_df = pd.DataFrame(X_scaled, columns=feature_names)
        
        if query_ids is not None and len(np.unique(query_ids)) > 1:
            # Query-based splitting: keep all samples from same query together
            logger.info("Using query-based train/test split")
            unique_queries = np.unique(query_ids)
            n_test_queries = int(len(unique_queries) * test_size)
            
            # Shuffle queries and split
            np.random.seed(0)
            shuffled_queries = np.random.permutation(unique_queries)
            test_queries = shuffled_queries[:n_test_queries]
            train_queries = shuffled_queries[n_test_queries:]
            
            # Create masks for train/test samples
            train_mask = np.isin(query_ids, train_queries)
            test_mask = np.isin(query_ids, test_queries)
            
            X_train = X_df[train_mask]
            X_test = X_df[test_mask]
            y_train = y_weights[train_mask]
            y_test = y_weights[test_mask]
            
            logger.info(f"  Train queries: {len(train_queries)}, samples: {len(X_train)}")
            logger.info(f"  Test queries: {len(test_queries)}, samples: {len(X_test)}")
        else:
            # Fallback to sample-based splitting
            logger.info("Using sample-based train/test split")
            X_train, X_test, y_train, y_test = train_test_split(
                X_df, y_weights, test_size=test_size, random_state=0
            )
        
        # Check if we have enough samples for train/test split
        min_samples_for_split = 5  # Minimum samples needed for meaningful split
        
        # Train Linear Regression model (NO REGULARIZATION)
        model = LinearRegression()
        logger.info("\n⚠️  Using LinearRegression - NO regularization penalty")
        
        if len(X_train) < min_samples_for_split or len(X_test) == 0:
            # Too few samples for proper evaluation, train on all data
            logger.warning(f"Too few samples for train/test split (train: {len(X_train)}, test: {len(X_test)})")
            logger.warning("Training on full dataset without test evaluation")
            
            # Refit on all data
            model.fit(X_df, y_weights)
            
            # Evaluate on training data only
            y_pred_all = model.predict(X_df)
            y_pred_all = np.clip(y_pred_all, 0.0, 1.0)
            
            all_mse = mean_squared_error(y_weights, y_pred_all)
            all_rmse = root_mean_squared_error(y_weights, y_pred_all)
            all_r2 = r2_score(y_weights, y_pred_all)
            
            logger.info(f"\nModel performance on full dataset (no test split):")
            logger.info(f"  MSE: {all_mse:.6f}, RMSE: {all_rmse:.6f}")
            logger.info(f"  R²: {all_r2:.6f}")
            
            # Set test variables for later analysis (use train values)
            y_test = y_weights
            y_pred_test = y_pred_all
            
        else:
            # Normal train/test evaluation
            if use_cross_validation and len(X_df) >= 5:
                # ShuffleSplit cross-validation with 5 splits
                logger.info("Performing ShuffleSplit cross-validation")
                cv = ShuffleSplit(n_splits=min(5, len(X_df)), test_size=test_size, random_state=0)
                
                # Define scorer for RMSE
                rmse_scorer = make_scorer(root_mean_squared_error, greater_is_better=False)
                
                # Perform cross-validation
                try:
                    cv_scores = cross_val_score(model, X_df, y_weights, cv=cv, scoring=rmse_scorer)
                    logger.info(f"  Cross-validation RMSE: {-cv_scores.mean():.6f} (+/- {cv_scores.std() * 2:.6f})")
                except Exception as e:
                    logger.warning(f"Cross-validation failed: {e}")
            
            # Fit the model
            model.fit(X_train, y_train)
            
            # Predictions
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Clip predictions to [0, 1] range
            y_pred_train = np.clip(y_pred_train, 0.0, 1.0)
            y_pred_test = np.clip(y_pred_test, 0.0, 1.0)
            
            # Calculate metrics
            train_mse = mean_squared_error(y_train, y_pred_train)
            train_rmse = root_mean_squared_error(y_train, y_pred_train)
            train_r2 = r2_score(y_train, y_pred_train)
            
            test_mse = mean_squared_error(y_test, y_pred_test)
            test_rmse = root_mean_squared_error(y_test, y_pred_test)
            test_r2 = r2_score(y_test, y_pred_test)
            
            logger.info(f"\nModel performance (LinearRegression - NO regularization):")
            logger.info(f"  Train - MSE: {train_mse:.6f}, RMSE: {train_rmse:.6f}, R²: {train_r2:.6f}")
            logger.info(f"  Test  - MSE: {test_mse:.6f}, RMSE: {test_rmse:.6f}, R²: {test_r2:.6f}")
        
        # Feature importance analysis (coefficients)
        coefficients = model.coef_
        feature_importance = pd.DataFrame({
            'feature': feature_names,
            'coefficient': coefficients,
            'abs_coefficient': np.abs(coefficients)
        }).sort_values('abs_coefficient', ascending=False)
        
        logger.info("\nTop feature coefficients (LinearRegression):")
        for idx, row in feature_importance.head(10).iterrows():
            logger.info(f"  {row['feature']}: {row['coefficient']:.6f}")
        
        # Analyze prediction distribution
        logger.info("\n" + "="*60)
        logger.info("PREDICTION ANALYSIS (LinearRegression)")
        logger.info("="*60)
        
        if 'y_test' in locals() and len(y_test) > 0:
            # Analyze exact weight values (rounded to 1 decimal place for clarity)
            test_actual_weights = pd.Series(y_test).round(1)
            test_pred_weights = pd.Series(y_pred_test).round(2)  # Round predictions to 2 decimals for more detail
            
            logger.info("\nTest set weight distributions (EXACT VALUES):")
            logger.info("\nActual optimal weights:")
            actual_counts = test_actual_weights.value_counts().sort_index()
            for weight, count in actual_counts.items():
                percentage = (count / len(y_test)) * 100
                logger.info(f"  Weight {weight:.1f}: {count} samples ({percentage:.1f}%)")
            
            logger.info(f"\nActual weights summary:")
            logger.info(f"  Mean: {y_test.mean():.3f}, Std: {y_test.std():.3f}")
            logger.info(f"  Min: {y_test.min():.3f}, Max: {y_test.max():.3f}")
            logger.info(f"  Unique values: {len(test_actual_weights.unique())}")
            
            logger.info("\nPredicted weights (LinearRegression):")
            # Group predictions into ranges for readability (0.1 intervals)
            pred_rounded = (test_pred_weights * 10).round() / 10  # Round to nearest 0.1
            pred_counts = pred_rounded.value_counts().sort_index()
            for weight, count in pred_counts.items():
                percentage = (count / len(y_pred_test)) * 100
                # Show actual range of predictions for this rounded value
                mask = (pred_rounded == weight)
                actual_range = test_pred_weights[mask]
                logger.info(f"  Weight ~{weight:.1f}: {count} samples ({percentage:.1f}%) [actual range: {actual_range.min():.3f}-{actual_range.max():.3f}]")
            
            logger.info(f"\nPredicted weights summary:")
            logger.info(f"  Mean: {y_pred_test.mean():.3f}, Std: {y_pred_test.std():.3f}")
            logger.info(f"  Min: {y_pred_test.min():.3f}, Max: {y_pred_test.max():.3f}")
            logger.info(f"  Unique values (rounded to 0.1): {len(pred_rounded.unique())}")
            
            # Check for mean collapse problem
            pred_std = np.std(y_pred_test)
            actual_std = np.std(y_test)
            logger.info(f"\nStandard deviation comparison:")
            logger.info(f"  Actual weights: {actual_std:.4f}")
            logger.info(f"  Predicted weights: {pred_std:.4f}")
            logger.info(f"  Ratio (predicted/actual): {pred_std/actual_std:.2f}")
            
            if pred_std/actual_std < 0.5:
                logger.warning("⚠️  MEAN COLLAPSE DETECTED: Predictions have much lower variance than actual!")
                logger.warning("    LinearRegression (like Ridge) is collapsing to mean values")
        
        # Store model with scaler
        model.scaler = scaler
        model.feature_names = feature_names
        
        return model
    
    def _execute_hybrid_search(self, query: str, lexical_weight: float, neural_weight: float,
                               normalization: str, combination: str) -> pd.DataFrame:
        """Execute hybrid search with specific normalization/combination."""
        
        # Build hybrid query
        hybrid_query = {
            "_source": {"exclude": ["title_embedding"]},
            "size": 100,
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "multi_match": {
                                "query": query,
                                "type": "best_fields",
                                "operator": "and",
                                "fields": [
                                    "product_title^10",
                                    "product_bullet_points^3", 
                                    "product_description",
                                    "product_brand^5",
                                    "product_color^2"
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
            }
        }
        
        # Add normalization and combination technique
        hybrid_query["search_pipeline"] = {
            "phase_results_processors": [
                {
                    "normalization-processor": {
                        "normalization": {
                            "technique": normalization
                        },
                        "combination": {
                            "technique": combination,
                            "parameters": {
                                "weights": [lexical_weight, neural_weight]
                            }
                        }
                    }
                }
            ]
        }
        
        try:
            response = self.client.search(index=self.index_name, body=hybrid_query)
            
            # Extract results
            results = []
            for hit in response['hits']['hits']:
                results.append({
                    'docid': hit['_source'].get('product_id', hit['_id']),
                    'score': hit['_score']
                })
            
            return pd.DataFrame(results)
            
        except Exception as e:
            logger.debug(f"Hybrid search failed: {e}")
            return pd.DataFrame()
    
    def _merge_results_with_reference(self, search_results: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
        """Merge search results with reference ratings."""
        
        if search_results.empty or reference.empty:
            return pd.DataFrame()
        
        # Merge on docid
        merged = search_results.merge(reference, on='docid', how='left')
        
        # Fill missing ratings with 0
        merged['rating'] = merged['rating'].fillna(0.0)
        
        # Add position column (1-based) for metrics.ndcg_at_10
        merged['position'] = range(1, len(merged) + 1)
        
        return merged[['docid', 'score', 'rating', 'position']]
    
    def save_model(self, model, output_path: str):
        """Save trained model and metadata."""
        
        # Save model
        with open(output_path, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"Saved LinearRegression model to: {output_path}")
        
        # Save metadata
        metadata = {
            'model_type': 'LinearRegression',
            'regularization': 'None',
            'n_features': len(model.feature_names),
            'feature_names': model.feature_names,
            'coefficients': model.coef_.tolist(),
            'intercept': float(model.intercept_),
            'host': self.host,
            'port': self.port,
            'index_name': self.index_name,
            'model_id': self.model_id
        }
        
        metadata_path = output_path.replace('.pkl', '_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved metadata to: {metadata_path}")


def main():
    """Main training function."""
    
    parser = argparse.ArgumentParser(description='Train O19S Linear Regression Weight Predictor')
    
    # Connection parameters
    parser.add_argument('--host', default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('--index-name', default='esci-products',
                       help='Index name')
    parser.add_argument('--model-id', required=True,
                       help='Neural model ID for hybrid search')
    
    # Training parameters
    parser.add_argument('--o19s-data-path', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to ESCI ratings file')
    parser.add_argument('--sample-size', type=int, default=100,
                       help='Number of queries for training')
    parser.add_argument('--weights', type=str, default='0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0',
                       help='Comma-separated weights to test')
    parser.add_argument('--test-size', type=float, default=0.2,
                       help='Test split ratio')
    parser.add_argument('--use-fixed-queries', action='store_true',
                       help='Use fixed query order from CSV')
    parser.add_argument('--output-path', default='o19s_linear_regression_model.pkl',
                       help='Output path for trained model')
    
    args = parser.parse_args()
    
    # Parse weights
    weights_to_test = [float(w) for w in args.weights.split(',')]
    
    # Initialize trainer
    trainer = O19SLinearRegressionTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index_name,
        model_id=args.model_id
    )
    
    # Collect training data
    logger.info("\n" + "="*60)
    logger.info("COLLECTING TRAINING DATA FOR LINEAR REGRESSION")
    logger.info("="*60)
    
    X_features, y_weights, query_ids = trainer.collect_training_data(
        o19s_data_path=args.o19s_data_path,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        weights_to_test=weights_to_test,
        use_fixed_queries=args.use_fixed_queries
    )
    
    # Train model
    logger.info("\n" + "="*60)
    logger.info("TRAINING LINEAR REGRESSION MODEL")
    logger.info("="*60)
    
    model = trainer.train_model(
        X_features=X_features,
        y_weights=y_weights,
        query_ids=query_ids,
        test_size=args.test_size,
        use_cross_validation=True
    )
    
    # Save model
    trainer.save_model(model, args.output_path)
    
    logger.info("\n" + "="*60)
    logger.info("LINEAR REGRESSION TRAINING COMPLETE")
    logger.info("="*60)
    logger.info(f"Model saved to: {args.output_path}")
    logger.info("\nCompare with Ridge regression results:")
    logger.info("  - Check if removing regularization improves R² scores")
    logger.info("  - Check if prediction distribution is less collapsed")
    logger.info("  - Evaluate with evaluate_o19s_weight_predictor.py")


if __name__ == "__main__":
    main()
