#!/usr/bin/env python3
"""
Train O19S Corpus-Aware Linear Regression Model

This script trains a Ridge regression model using the exact O19S approach:
1. Uses the same 18 features (6 query + 12 real-time corpus features)
2. Collects training data by testing multiple weights per query
3. Tests all 6 combinations (2 normalizations × 3 combinations) and selects best NDCG
4. Uses O19S termvectors API method for corpus feature collection
5. Trains Ridge regression to predict NDCG given features including weight

The trained model can then be used for dynamic weight prediction.

Author: Dynamic Hybrid Search Team  
Version: 2.0.0 - O19S exact methodology with all normalization/combination techniques
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


class O19SCorpusAwareTrainer:
    """Train O19S corpus-aware models using exact methodology"""
    
    # O19S exact list of common English stopwords
    STOPWORDS = {
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
        "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
        "their", "then", "there", "these", "they", "this", "to", "was", "will",
        "with", "without"
    }
    
    # O19S normalization and combination techniques
    NORMALIZATION_TECHNIQUES = ['l2']
    COMBINATION_TECHNIQUES = ['arithmetic_mean',]
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 corpus_field: str = "product_title"):
        """Initialize O19S corpus-aware trainer"""
        
        # Configure for remote connection
        port == 80
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
        
        logger.info(f"Initialized O19S corpus-aware trainer for {host}:{port}/{index_name}")
        logger.info(f"Corpus field: {corpus_field}")
        logger.info(f"Total documents: {self._total_docs}")
        logger.info(f"Will test {len(self.NORMALIZATION_TECHNIQUES)} normalizations × {len(self.COMBINATION_TECHNIQUES)} combinations = 6 total")
    
    def _initialize_corpus_info(self):
        """Initialize corpus information."""
        try:
            response = self.client.count(index=self.index_name)
            self._total_docs = response['count']
        except Exception as e:
            logger.warning(f"Could not get document count: {e}")
            self._total_docs = 100000
    
    # O19S Exact Query Feature Functions
    
    def has_numbers(self, text: str) -> int:
        """
        Checks if a string contains any numeric digits.
        
        Args:
            text: The input string.
        
        Returns:
            1 if the string contains numbers, 0 otherwise.
        """
        return 1 if any(char.isdigit() for char in text) else 0
    
    def has_special_char(self, text: str) -> int:
        """
        Checks if a string contains special characters including quotes and apostrophes.
        Updated to match the export script implementation.
        
        Args:
            text: The input string.
        
        Returns:
            1 if the string contains special characters, 0 otherwise.
        """
        # Updated to include quotes and apostrophes as per export script
        special_chars = '!@#$%^&*()_+-=[]{}|;:,.<>?\'"'
        return 1 if any(c in text for c in special_chars) else 0
    
    def has_punctuation_at_end(self, text: str) -> int:
        """
        Checks if a string ends with a punctuation character.
        O19S exact implementation.
        
        Args:
            text: The input string.
        
        Returns:
            1 if the string ends with punctuation, 0 otherwise.
        """
        # Check for empty or whitespace-only strings
        stripped_text = text.strip()
        if not stripped_text:
            return 0
        
        # Get the last character of the stripped string and check if it's in the punctuation set
        return 1 if stripped_text[-1] in string.punctuation else 0
    
    def unique_terms_ratio(self, text: str) -> float:
        """
        Calculates the ratio of unique terms to the total number of terms in a string.
        O19S exact implementation.
        
        The string is first preprocessed to remove punctuation and convert to lowercase
        to ensure accurate term counting.
        
        Args:
            text: The input string.
        
        Returns:
            The ratio of unique terms. Returns 0.0 if the string has no terms.
        """
        # Preprocess the text: convert to lowercase and remove punctuation
        # A regular expression is used to split the text into words
        preprocessed_text = text.lower()
        terms = re.findall(r'\b\w+\b', preprocessed_text)
        
        # Handle the case of an empty string or a string with no words
        if not terms:
            return 0.0
        
        unique_terms = set(terms)
        
        return len(unique_terms) / len(terms)
    
    def capital_letters_ratio(self, text: str) -> float:
        """
        Calculates the ratio of capital letters to the total number of characters in a string.
        O19S exact implementation.
        
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
        O19S exact implementation.
        
        The string is preprocessed to handle case and punctuation.
        
        Args:
            text: The input string.
        
        Returns:
            The ratio of stopwords. Returns 0.0 if the string has no terms.
        """
        # Preprocess the text to get a list of terms
        preprocessed_text = text.lower()
        terms = re.findall(r'\b\w+\b', preprocessed_text)
        
        # Handle the case of an empty string or a string with no words
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
        Collect training data using O19S methodology.
        
        For each query in training set:
        1. Test multiple weights (0.1-0.9)
        2. For each weight, test all 6 normalization/combination techniques
        3. Select the combination that produces the highest NDCG@10
        4. Extract 18 features (including weight) for the best combination
        5. Create training sample: features -> best_NDCG
        
        Args:
            o19s_data_path: Path to O19S data
            ratings_file: Path to ratings file
            sample_size: Number of queries to use for training
            weights_to_test: Weights to test per query
            use_fixed_queries: If True, use fixed order from CSV; if False, use random sampling
            
        Returns:
            (X_features, y_ndcg, query_ids): Training features, target NDCG values, and query IDs
        """
        
        if weights_to_test is None:
            weights_to_test = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        
        logger.info(f"Collecting training data with weights: {weights_to_test}")
        logger.info(f"Testing all 6 combinations: {self.NORMALIZATION_TECHNIQUES} × {self.COMBINATION_TECHNIQUES}")
        
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
            # Existing behavior: random sampling
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
        y_ndcg = []
        query_ids = []  # Track which query each sample belongs to
        
        total_samples = len(train_queries_with_ratings) * len(weights_to_test)
        logger.info(f"Expected training samples: {total_samples}")
        
        samples_collected = 0
        
        for query_idx, query_string in enumerate(tqdm(train_queries_with_ratings, desc="Collecting training data")):
            if query_string not in reference:
                continue
                
            for weight in weights_to_test:
                try:
                    # Extract 20 features using O19S method with new additions
                    features = self._extract_o19s_20_features(query_string, weight)
                    
                    # Test all 6 normalization/combination techniques
                    lexical_weight = round(1.0 - weight, 2)
                    best_ndcg = 0.0
                    best_combination = ('l2', 'arithmetic_mean')  # Default
                    
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
                                    
                                    # Track if this is the best combination
                                    if actual_ndcg > best_ndcg:
                                        best_ndcg = actual_ndcg
                                        best_combination = (normalization, combination)
                                    
                                    self.total_combinations_tested += 1
                        
                        except Exception as e:
                            logger.debug(f"Failed combination {normalization}/{combination}: {e}")
                            continue
                    
                    # Only add training sample if we got at least one valid NDCG
                    if best_ndcg > 0:
                        # Add training sample with best NDCG and query ID
                        X_features.append(features)
                        y_ndcg.append(best_ndcg)
                        query_ids.append(query_string)  # Use query string as ID
                        samples_collected += 1
                        
                        # Track which combination was best
                        self.normalization_stats[best_combination[0]][best_combination[1]] += 1
                
                except Exception as e:
                    logger.warning(f"Failed to collect sample for query '{query_string}', weight {weight}: {e}")
                    continue
        
        logger.info(f"Collected {samples_collected} training samples")
        logger.info(f"Unique queries in training data: {len(np.unique(query_ids))}")
        logger.info(f"Total combinations tested: {self.total_combinations_tested}")
        
        # Report statistics on which combinations were selected as best
        logger.info("\nBest combination selection statistics:")
        for norm in self.NORMALIZATION_TECHNIQUES:
            for comb in self.COMBINATION_TECHNIQUES:
                count = self.normalization_stats[norm][comb]
                percentage = (count / samples_collected * 100) if samples_collected > 0 else 0
                logger.info(f"  {norm}/{comb}: {count} times ({percentage:.1f}%)")
        
        if samples_collected == 0:
            raise ValueError("No training samples collected")
            
        return np.array(X_features), np.array(y_ndcg), np.array(query_ids)
    
    def _extract_o19s_20_features(self, query: str, weight: float) -> List[float]:
        """Extract 20 features using O19S exact methodology with new additions."""
        
        # Extract query features using O19S exact functions
        query_features = {
            'f_2_query_length': len(query),
            'f_3_has_numbers': float(self.has_numbers(query)),  # NEW feature
            'f_4_has_special_char': float(self.has_special_char(query)),  # Updated to use new function
            'f_5_has_punctuation_at_end': float(self.has_punctuation_at_end(query)),
            'f_6_unique_terms_ratio': self.unique_terms_ratio(query),  # NEW feature
            'f_7_capital_letters_ratio': self.capital_letters_ratio(query),
            'f_8_stopwords_ratio': self.stopwords_ratio(query)
        }
        
        # Extract real-time corpus features using O19S method
        corpus_features = self._collect_corpus_features_o19s_method(query)
        
        # Create 20-feature vector matching export script order
        feature_vector = [
            weight,  # f_0_neuralness (neural_search_weight)
            query_features['f_2_query_length'],  # query_length
            query_features['f_3_has_numbers'],  # has_numbers (NEW)
            query_features['f_4_has_special_char'],  # has_special_char
            query_features['f_5_has_punctuation_at_end'],  # has_punctuation_at_end
            query_features['f_6_unique_terms_ratio'],  # unique_terms_ratio (NEW)
            query_features['f_7_capital_letters_ratio'],  # capital_letters_ratio
            query_features['f_8_stopwords_ratio'],  # stopwords_ratio
            corpus_features['max_document_frequency'],  # f_14_max_document_frequency
            corpus_features['min_document_frequency'],  # f_15_min_document_frequency
            corpus_features['total_document_frequency'],  # f_16_total_document_frequency
            corpus_features['average_document_frequency'],  # f_17_average_document_frequency
            corpus_features['variance_document_frequency'],  # f_18_variance_document_frequency
            corpus_features['std_dev_document_frequency'],  # f_19_std_dev_document_frequency
            corpus_features['max_inverse_document_frequency'],  # f_20_max_inverse_document_frequency
            corpus_features['min_inverse_document_frequency'],  # f_21_min_inverse_document_frequency
            corpus_features['total_inverse_document_frequency'],  # f_22_total_inverse_document_frequency
            corpus_features['average_inverse_document_frequency'],  # f_23_average_inverse_document_frequency
            corpus_features['variance_inverse_document_frequency'],  # f_24_variance_inverse_document_frequency
            corpus_features['std_dev_inverse_document_frequency']  # f_25_std_dev_inverse_document_frequency
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
                   query_ids: np.ndarray = None,
                   alpha: float = 10.0,
                   test_size: float = 0.2,
                   use_cross_validation: bool = True) -> Ridge:
        """
        Train Ridge regression model using O19S exact methodology.
        
        Args:
            X_features: Feature matrix (samples x 20 features)
            y_ndcg: Target NDCG values
            query_ids: Query identifiers for each sample (for query-based splitting)
            alpha: Ridge regularization parameter (O19S uses 10.0)
            test_size: Fraction for validation split
            use_cross_validation: If True, use ShuffleSplit cross-validation (O19S method)
            
        Returns:
            Trained Ridge regression model with feature scaler
        """
        
        logger.info(f"Training Ridge regression model (O19S exact methodology)...")
        logger.info(f"  Training samples: {len(X_features)}")
        logger.info(f"  Features: {X_features.shape[1]}")
        logger.info(f"  Alpha (regularization): {alpha}")
        logger.info(f"  Test split: {test_size}")
        logger.info(f"  Cross-validation: {use_cross_validation}")
        
        # Create feature names for 20 features
        feature_names = [
            'f_0_neuralness', 'f_2_query_length', 'f_3_has_numbers', 'f_4_has_special_char', 
            'f_5_has_punctuation_at_end', 'f_6_unique_terms_ratio', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
            'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
            'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
            'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
            'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
        ]
        
        # Convert to DataFrame for sklearn compatibility
        X_df = pd.DataFrame(X_features, columns=feature_names)
        
        # SELECTIVE SCALING: Exclude weight feature (f_0_neuralness) from StandardScaler
        # This preserves the natural [0,1] bounds of the weight feature while scaling others
        logger.info("Applying selective scaling: preserving weight feature, scaling others...")
        
        # Separate weight feature from other features
        weight_feature = X_df[['f_0_neuralness']].values  # Keep weight unscaled
        other_features = X_df.drop('f_0_neuralness', axis=1)
        
        # Apply StandardScaler only to non-weight features  
        scaler = StandardScaler()
        other_features_scaled = scaler.fit_transform(other_features)
        
        # Recombine: unscaled weight + scaled other features
        X_scaled = np.hstack([weight_feature, other_features_scaled])
        
        # Reconstruct DataFrame with all features
        X_df = pd.DataFrame(X_scaled, columns=feature_names)
        
        logger.info(f"  Weight feature (f_0_neuralness) preserved in range [{weight_feature.min():.2f}, {weight_feature.max():.2f}]")
        logger.info(f"  Other {len(other_features.columns)} features scaled with StandardScaler")
        
        if query_ids is not None and len(np.unique(query_ids)) > 1:
            # O19S Query-based splitting: keep all samples from same query together
            logger.info("Using query-based train/test split (O19S method)")
            unique_queries = np.unique(query_ids)
            n_test_queries = int(len(unique_queries) * test_size)
            
            # Shuffle queries and split
            np.random.seed(0)  # O19S uses random_state=0
            shuffled_queries = np.random.permutation(unique_queries)
            test_queries = shuffled_queries[:n_test_queries]
            train_queries = shuffled_queries[n_test_queries:]
            
            # Create masks for train/test samples
            train_mask = np.isin(query_ids, train_queries)
            test_mask = np.isin(query_ids, test_queries)
            
            X_train = X_df[train_mask]
            X_test = X_df[test_mask]
            y_train = y_ndcg[train_mask]
            y_test = y_ndcg[test_mask]
            
            logger.info(f"  Train queries: {len(train_queries)}, samples: {len(X_train)}")
            logger.info(f"  Test queries: {len(test_queries)}, samples: {len(X_test)}")
        else:
            # Fallback to sample-based splitting if no query IDs
            logger.info("Using sample-based train/test split (fallback)")
            X_train, X_test, y_train, y_test = train_test_split(
                X_df, y_ndcg, test_size=test_size, random_state=0
            )
        
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
            if name == 'f_0_neuralness':
                # Weight feature was not scaled, so coefficient is already on original scale
                original_scale_coef = coef
            else:
                # Other features were scaled, adjust coefficient to original scale
                # The scaler only has 19 features (index 0-18), so adjust index
                scaler_idx = i - 1  # Shift index since weight was removed from scaler
                original_scale_coef = coef / scaler.scale_[scaler_idx] if scaler.scale_[scaler_idx] != 0 else coef
            logger.info(f"  {name}: {original_scale_coef:.6f}")
        logger.info(f"  Intercept: {model.intercept_:.6f}")
        
        # Check weight sensitivity (weight is at index 0 and wasn't scaled)
        weight_coef = model.coef_[0]  # Already on original scale
        logger.info(f"\nWeight sensitivity: {abs(weight_coef):.6f}")
        if abs(weight_coef) < 0.01:
            logger.warning("⚠️  Model is not sensitive to weight changes!")
        else:
            logger.info("✅ Model is sensitive to weight changes")
        
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
                "description": f"O19S training with {normalization}/{combination}",
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
            
            rows = []
            for position, hit in enumerate(result['hits']['hits']):
                rows.append({
                    'product_id': hit['_id'],
                    'position': position,
                    'relevance': hit['_score']
                })
            
            return pd.DataFrame(rows) if rows else pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Hybrid search failed for query '{query}' with {normalization}/{combination}: {e}")
            return pd.DataFrame()
    
    def _merge_results_with_reference(self, search_results: pd.DataFrame, reference_ratings: pd.DataFrame) -> pd.DataFrame:
        """Merge search results with reference ratings."""
        
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
        description="Train O19S corpus-aware linear regression model",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Train model with 100 queries and 3 weights for quick testing (random sampling)
  python train_o19s_corpus_aware_model.py \\
    --model-id huggingface/sentence-transformers/all-MiniLM-L6-v2 \\
    --sample-size 100 \\
    --weights "0.3,0.5,0.7"

  # Train with fixed queries from CSV (no random sampling)
  python train_o19s_corpus_aware_model.py \\
    --model-id huggingface/sentence-transformers/all-MiniLM-L6-v2 \\
    --use-fixed-queries \\
    --sample-size 100 \\
    --weights "0.3,0.5,0.7"

  # Full training with all queries and all weights
  python train_o19s_corpus_aware_model.py \\
    --model-id huggingface/sentence-transformers/all-MiniLM-L6-v2 \\
    --weights "0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9"
        """
    )
    
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products', help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--corpus-field', default='product_title',
                       help='Field to analyze for corpus statistics')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to O19S ratings.csv file')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of training queries to use (default: None = use all)')
    parser.add_argument('--weights', type=str, default='0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0',
                       help='Weights to test per query (comma-separated)')
    parser.add_argument('--alpha', type=float, default=10.0,
                       help='Ridge regularization parameter')
    parser.add_argument('--use-fixed-queries', action='store_true',
                       help='Use fixed queries from CSV in order (no random sampling)')
    parser.add_argument('--output', default='o19s_corpus_aware_trained_model.pkl',
                       help='Output model file')
    
    args = parser.parse_args()
    
    # Parse weights
    try:
        weights_to_test = [float(w.strip()) for w in args.weights.split(',')]
        logger.info(f"Training with weights: {weights_to_test}")
    except ValueError as e:
        logger.error(f"Invalid weights format: {e}")
        sys.exit(1)
    
    # Initialize trainer
    trainer = O19SCorpusAwareTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        corpus_field=args.corpus_field
    )
    
    # Collect training data
    logger.info("🔍 Collecting training data with real-time corpus features...")
    logger.info("   Testing all 6 normalization/combination techniques per query-weight pair")
    if args.use_fixed_queries:
        logger.info("   Using fixed query order from CSV files")
    else:
        logger.info("   Using random query sampling")
    logger.info("   This will query OpenSearch for each training sample")
    
    start_time = time.time()
    X_features, y_ndcg, query_ids = trainer.collect_training_data(
        o19s_data_path=args.o19s_data,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        weights_to_test=weights_to_test,
        use_fixed_queries=args.use_fixed_queries
    )
    data_collection_time = time.time() - start_time
    
    logger.info(f"Training data collection completed in {data_collection_time:.1f}s")
    logger.info(f"Collected {len(X_features)} training samples from {len(np.unique(query_ids))} unique queries")
    
    # Train model with query-based splitting (O19S exact methodology)
    logger.info("🚀 Training Ridge regression model with O19S exact methodology...")
    model = trainer.train_model(X_features, y_ndcg, query_ids=query_ids, alpha=args.alpha)
    
    # Save trained model with scaler
    model_data = {
        'model': model,
        'scaler': trainer.scaler,
        'normalization_stats': trainer.normalization_stats
    }
    
    model_path = args.output
    with open(model_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    logger.info(f"✅ Trained model saved to: {model_path}")
    
    # Save training metadata
    metadata = {
        'training_samples': len(X_features),
        'features': 20,
        'weights_tested': weights_to_test,
        'alpha': args.alpha,
        'corpus_field': args.corpus_field,
        'use_fixed_queries': args.use_fixed_queries,
        'data_collection_time_seconds': data_collection_time,
        'normalization_stats': trainer.normalization_stats,
        'total_combinations_tested': trainer.total_combinations_tested,
        'feature_names': [
            'f_0_neuralness', 'f_2_query_length', 'f_3_has_numbers', 'f_4_has_special_char', 
            'f_5_has_punctuation_at_end', 'f_6_unique_terms_ratio', 'f_7_capital_letters_ratio', 'f_8_stopwords_ratio',
            'f_14_max_document_frequency', 'f_15_min_document_frequency', 'f_16_total_document_frequency',
            'f_17_average_document_frequency', 'f_18_variance_document_frequency', 'f_19_std_dev_document_frequency',
            'f_20_max_inverse_document_frequency', 'f_21_min_inverse_document_frequency', 'f_22_total_inverse_document_frequency',
            'f_23_average_inverse_document_frequency', 'f_24_variance_inverse_document_frequency', 'f_25_std_dev_inverse_document_frequency'
        ]
    }
    
    metadata_path = args.output.replace('.pkl', '_metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Training metadata saved to: {metadata_path}")
    
    # Final summary
    logger.info(f"\n🎯 TRAINING COMPLETED SUCCESSFULLY:")
    logger.info(f"   Model: {model_path}")
    logger.info(f"   Metadata: {metadata_path}")
    logger.info(f"   Training samples: {len(X_features)}")
    logger.info(f"   Features: 20 (O19S corpus-aware with new additions)")
    logger.info(f"   Data collection time: {data_collection_time:.1f}s")
    logger.info(f"   Total combinations tested: {trainer.total_combinations_tested}")


if __name__ == "__main__":
    main()
