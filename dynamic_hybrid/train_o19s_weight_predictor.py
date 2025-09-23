#!/usr/bin/env python3
"""
Train O19S Weight Predictor Model

This script trains a Ridge regression model to predict optimal weight directly:
1. Uses 17 features (5 query + 12 real-time corpus features) - NO weight as feature
2. For each query, finds the weight that produces highest NDCG
3. Trains model to predict this optimal weight given the 17 features
4. Target is the weight itself (not NDCG)

This is different from original O19S which uses weight as input feature to predict NDCG.
Here we predict the weight that maximizes NDCG.

Author: Dynamic Hybrid Search Team  
Version: 1.0.0 - Weight prediction model
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


class O19SWeightPredictorTrainer:
    """Train model to predict optimal weight directly (not NDCG)"""
    
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
        """Initialize weight predictor trainer"""
        
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
        
        logger.info(f"Initialized O19S weight predictor trainer for {host}:{port}/{index_name}")
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
                             use_fixed_queries: bool = False,
                             data_source: str = 'csv') -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Collect training data for weight prediction.
        
        For each query in training set:
        1. Extract 17 features (NO weight feature)
        2. Test multiple weights and find which produces highest NDCG
        3. Create training sample: 17 features -> best_weight
        
        Args:
            o19s_data_path: Path to O19S data (for CSV) or base directory (for parquet)
            ratings_file: Path to ratings file (for CSV mode)
            sample_size: Number of queries to use for training
            weights_to_test: Weights to test per query
            use_fixed_queries: If True, use fixed order from CSV
            data_source: 'csv' for CSV files or 'parquet' for parquet files
            
        Returns:
            (X_features, y_weights, query_ids): Training features (17), target weights, and query IDs
        """
        
        if weights_to_test is None:
            weights_to_test = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        
        logger.info(f"Collecting training data with weights: {weights_to_test}")
        logger.info(f"Predicting optimal weight (not NDCG) from 17 features")
        logger.info(f"Data source: {data_source}")
        logger.info(f"Testing all 6 combinations: {self.NORMALIZATION_TECHNIQUES} × {self.COMBINATION_TECHNIQUES}")
        
        if data_source == 'parquet':
            # Load from parquet files
            logger.info("Loading data from parquet files...")
            
            # Load ESCI examples with query-product pairs and labels
            #examples_file = 'esci_data/shopping_queries_dataset_examples_us_small.parquet'
            examples_file = 'esci_data/shopping_queries_dataset_examples.parquet'
            if not Path(examples_file).exists():
                # Try larger file if small doesn't exist
                examples_file = 'esci_data/shopping_queries_dataset_examples.parquet'
                if not Path(examples_file).exists():
                    raise FileNotFoundError(f"ESCI examples parquet not found")
            
            df_examples = pd.read_parquet(examples_file)
            logger.info(f"Loaded {len(df_examples)} examples from {examples_file}")
            
            # Filter to training split if available
            if 'split' in df_examples.columns:
                # Check what splits are available
                available_splits = df_examples['split'].unique()
                logger.info(f"Available splits in parquet: {available_splits.tolist()}")
                
                # Try to use 'train' split first
                df_train_examples = df_examples[df_examples['split'] == 'train']
                if len(df_train_examples) == 0:
                    # If no train split, try 'test' split
                    df_train_examples = df_examples[df_examples['split'] == 'test']
                    if len(df_train_examples) > 0:
                        logger.info(f"No 'train' split found, using 'test' split with {len(df_train_examples)} examples")
                    else:
                        # If neither train nor test, use all data
                        df_train_examples = df_examples
                        logger.info(f"No standard splits found, using all {len(df_train_examples)} examples")
                else:
                    logger.info(f"Using 'train' split with {len(df_train_examples)} examples")
            else:
                df_train_examples = df_examples
                logger.info(f"No split column found, using all {len(df_train_examples)} examples")
            
            # Create ratings dataframe from examples
            df_ratings = df_train_examples[['query', 'product_id', 'esci_label']].copy()
            df_ratings.columns = ['query_string', 'product_id', 'esci_label']
            
            # Get unique queries for training
            train_queries = df_ratings['query_string'].unique().tolist()
            logger.info(f"Found {len(train_queries)} unique queries in parquet data")
            
        else:
            # Original CSV loading logic
            # Load O19S data
            train_file = Path(o19s_data_path) / 'query_train.csv'
            if not train_file.exists():
                raise FileNotFoundError(f"O19S train queries not found: {train_file}")
                
            df_train = pd.read_csv(train_file)
            train_queries = df_train['query_string'].tolist()
            
            # Load ratings - Handle CSV file with headers
            if not Path(ratings_file).exists():
                raise FileNotFoundError(f"Ratings file not found: {ratings_file}")
                
            logger.info(f"Loading ratings from: {ratings_file}")
            # Check if file has headers by reading first line
            with open(ratings_file, 'r') as f:
                first_line = f.readline().strip()
                has_header = 'query' in first_line.lower() or 'product' in first_line.lower()
            
            if has_header:
                # File has headers - read normally
                df_ratings = pd.read_csv(ratings_file)
                # Rename columns to match expected format
                if 'query' in df_ratings.columns:
                    df_ratings = df_ratings.rename(columns={'query': 'query_string'})
            else:
                # File has no headers - assume tab-delimited format
                df_ratings = pd.read_csv(ratings_file, sep='\t', header=None, 
                                       names=['query_string', 'product_id', 'esci_label', 'query_id'],
                                       on_bad_lines='skip')
        
        # Map labels to rating scores
        if data_source == 'parquet':
            # Parquet files use ESCI letter labels
            esci_to_numeric = {
                'E': 1.0,    # Exact
                'S': 0.1,    # Substitute
                'C': 0.01,   # Complement
                'I': 0.0     # Irrelevant
            }
            df_ratings['rating'] = df_ratings['esci_label'].map(esci_to_numeric)
            logger.info(f"Using ESCI letter mapping (E,S,C,I) -> rating scores")
        else:
            # CSV files may already have 'rating' column with numeric values
            if 'rating' in df_ratings.columns:
                # Rating column already exists - just map to NDCG scores
                numeric_to_score = {
                    3: 1.0,    # Exact
                    2: 0.1,    # Substitute
                    1: 0.01,   # Complement
                    0: 0.0     # Irrelevant
                }
                # Convert rating to numeric if it's not already
                df_ratings['rating'] = pd.to_numeric(df_ratings['rating'], errors='coerce')
                # Map to NDCG scores
                df_ratings['rating'] = df_ratings['rating'].map(numeric_to_score)
                logger.info(f"Using existing numeric ratings (0,1,2,3) -> NDCG scores")
            elif 'esci_label' in df_ratings.columns:
                # Try to map from esci_label column
                # First try numeric mapping
                numeric_to_score = {
                    3: 1.0,    # Exact
                    2: 0.1,    # Substitute
                    1: 0.01,   # Complement
                    0: 0.0     # Irrelevant
                }
                
                # Try to convert to numeric
                df_ratings['esci_label_numeric'] = pd.to_numeric(df_ratings['esci_label'], errors='coerce')
                
                # If numeric conversion worked, use numeric mapping
                if not df_ratings['esci_label_numeric'].isna().all():
                    df_ratings['rating'] = df_ratings['esci_label_numeric'].map(numeric_to_score)
                    logger.info(f"Using numeric label mapping (0,1,2,3) -> NDCG scores")
                else:
                    # Fall back to ESCI letter mapping
                    esci_to_numeric = {
                        'E': 1.0,    # Exact
                        'S': 0.1,    # Substitute
                        'C': 0.01,   # Complement
                        'I': 0.0     # Irrelevant
                    }
                    df_ratings['rating'] = df_ratings['esci_label'].map(esci_to_numeric)
                    logger.info(f"Using ESCI letter mapping (E,S,C,I) -> NDCG scores")
            else:
                raise ValueError("No 'rating' or 'esci_label' column found in ratings file")
        
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
            # Show some examples of unmapped labels
            unmapped_samples = df_ratings[df_ratings['rating'].isna()].head(5)
            logger.warning(f"Sample unmapped labels: {unmapped_samples['esci_label'].tolist()}")
        
        # Create reference dictionary grouped by query
        reference = {}
        for query_string, group in df_ratings.groupby('query_string'):
            reference[query_string] = group[['product_id', 'rating']].rename(columns={'product_id': 'docid'})
        
        logger.info(f"Created reference for {len(reference)} unique queries")
        
        # Sample queries based on parameters
        if data_source == 'parquet':
            # For parquet, train_queries already contains unique queries
            if sample_size and sample_size < len(train_queries):
                if use_fixed_queries:
                    # Take first N queries
                    train_queries = train_queries[:sample_size]
                    logger.info(f"Using first {sample_size} queries from parquet dataset")
                else:
                    # Random sample
                    np.random.seed(42)
                    train_queries = np.random.choice(train_queries, size=sample_size, replace=False).tolist()
                    logger.info(f"Randomly sampled {sample_size} queries from parquet")
            else:
                logger.info(f"Using all {len(train_queries)} queries from parquet")
        else:
            # Original CSV logic
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
        y_weights = []  # Target is now weight, not NDCG
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
        
        # Store total queries for later reporting
        self.total_queries_used = len(np.unique(query_ids)) if query_ids else 0
        
        logger.info(f"Collected {samples_collected} training samples (one per query)")
        logger.info(f"Total unique queries used for training: {self.total_queries_used}")
        logger.info(f"Total combinations tested: {self.total_combinations_tested}")
        
        # Report weight distribution in training data
        if y_weights:
            weight_counts = pd.Series(y_weights).value_counts().sort_index()
            logger.info("\nOptimal weight distribution in training data:")
            for weight, count in weight_counts.items():
                percentage = (count / len(y_weights)) * 100
                logger.info(f"  Weight {weight:.1f}: {count} queries ({percentage:.1f}%)")
        
        # Report statistics on which combinations were selected as best
        logger.info("\nBest combination selection statistics:")
        for norm in self.NORMALIZATION_TECHNIQUES:
            for comb in self.COMBINATION_TECHNIQUES:
                count = self.normalization_stats[norm][comb]
                percentage = (count / samples_collected * 100) if samples_collected > 0 else 0
                logger.info(f"  {norm}/{comb}: {count} times ({percentage:.1f}%)")
        
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
                   alpha: float = 1.0,
                   test_size: float = 0.2,
                   use_cross_validation: bool = True) -> Ridge:
        """
        Train Ridge regression model to predict optimal weight.
        
        Args:
            X_features: Feature matrix (samples x 17 features)
            y_weights: Target weight values (what we're predicting)
            query_ids: Query identifiers for each sample
            alpha: Ridge regularization parameter (lower than O19S since different target)
            test_size: Fraction for validation split
            use_cross_validation: If True, use ShuffleSplit cross-validation
            
        Returns:
            Trained Ridge regression model with feature scaler
        """
        
        logger.info(f"Training Ridge regression model to predict optimal weight...")
        logger.info(f"  Training samples: {len(X_features)}")
        logger.info(f"  Features: {X_features.shape[1]} (no weight feature)")
        logger.info(f"  Target: optimal weight (not NDCG)")
        logger.info(f"  Alpha (regularization): {alpha}")
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
        
        # Apply StandardScaler to ALL features (no selective scaling needed since no weight feature)
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
        
        # Train Ridge regression model
        model = Ridge(alpha=alpha, solver='auto', random_state=0)
        
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
                    
                    # Also evaluate with R² scoring
                    cv_r2_scores = cross_val_score(model, X_df, y_weights, cv=cv, scoring='r2')
                    logger.info(f"  Cross-validation R²: {cv_r2_scores.mean():.6f} (+/- {cv_r2_scores.std() * 2:.6f})")
                except Exception as e:
                    logger.warning(f"Cross-validation failed: {e}")
            
            # Fit final model on training data
            model.fit(X_train, y_train)
            
            # Validate model
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Clip predictions to valid weight range [0, 1]
            y_pred_train = np.clip(y_pred_train, 0.0, 1.0)
            y_pred_test = np.clip(y_pred_test, 0.0, 1.0)
            
            train_mse = mean_squared_error(y_train, y_pred_train)
            test_mse = mean_squared_error(y_test, y_pred_test)
            train_rmse = root_mean_squared_error(y_train, y_pred_train)
            test_rmse = root_mean_squared_error(y_test, y_pred_test)
            train_r2 = r2_score(y_train, y_pred_train)
            test_r2 = r2_score(y_test, y_pred_test)
            
            logger.info(f"\nFinal model performance (predicting weight):")
            logger.info(f"  Train MSE: {train_mse:.6f}, RMSE: {train_rmse:.6f}")
            logger.info(f"  Test MSE: {test_mse:.6f}, RMSE: {test_rmse:.6f}")
            logger.info(f"  Train R²: {train_r2:.6f}")
            logger.info(f"  Test R²: {test_r2:.6f}")
        
        # Analyze feature importance via coefficients
        logger.info(f"\nFeature coefficients (importance for weight prediction):")
        feature_importance = []
        for i, (name, coef) in enumerate(zip(feature_names, model.coef_)):
            # Adjust coefficient to original scale
            original_scale_coef = coef / scaler.scale_[i] if scaler.scale_[i] != 0 else coef
            feature_importance.append((name, abs(original_scale_coef)))
            logger.info(f"  {name}: {coef:.6f} (scaled), {original_scale_coef:.6f} (original scale)")
        
        # Sort by importance (absolute value)
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        logger.info(f"\nTop features for weight prediction (by importance):")
        for i, (name, importance) in enumerate(feature_importance[:10], 1):
            logger.info(f"  {i}. {name}: {importance:.6f}")
        
        # Analyze prediction distribution (only if we have test data)
        if len(y_pred_test) > 0:
            logger.info(f"\nPredicted weight distribution on evaluation set:")
            predicted_weights = np.round(y_pred_test, 1)  # Round to nearest 0.1
            weight_counts = pd.Series(predicted_weights).value_counts().sort_index()
            for weight, count in weight_counts.items():
                percentage = (count / len(y_pred_test)) * 100
                logger.info(f"  Weight {weight:.1f}: {count} predictions ({percentage:.1f}%)")
            
            # Compare with actual distribution
            logger.info(f"\nActual weight distribution in evaluation set:")
            actual_weights = np.round(y_test, 1)
            actual_counts = pd.Series(actual_weights).value_counts().sort_index()
            for weight, count in actual_counts.items():
                percentage = (count / len(y_test)) * 100
                logger.info(f"  Weight {weight:.1f}: {count} samples ({percentage:.1f}%)")
        
        # Store scaler with model
        model.scaler = scaler
        model.feature_names = feature_names
        
        return model
    
    def save_model(self, model: Ridge, output_path: str):
        """Save trained model and metadata."""
        
        # Save model with pickle
        with open(output_path, 'wb') as f:
            pickle.dump(model, f)
        
        # Save metadata
        metadata = {
            'model_type': 'Ridge',
            'target': 'weight',
            'features': model.feature_names if hasattr(model, 'feature_names') else [],
            'num_features': len(model.coef_),
            'alpha': model.alpha,
            'intercept': float(model.intercept_),
            'coefficients': model.coef_.tolist(),
            'scaler_params': {
                'mean': model.scaler.mean_.tolist() if hasattr(model, 'scaler') else [],
                'scale': model.scaler.scale_.tolist() if hasattr(model, 'scaler') else []
            }
        }
        
        metadata_path = output_path.replace('.pkl', '_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Model saved to: {output_path}")
        logger.info(f"Metadata saved to: {metadata_path}")
    
    def _execute_hybrid_search(self, query: str, lexical_weight: float, neural_weight: float,
                              normalization_technique: str, combination_technique: str) -> pd.DataFrame:
        """Execute O19S hybrid search with specific normalization/combination."""
        
        # Build payload with the ACTUAL normalization and combination parameters
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
                "description": f"O19S weight predictor with {normalization_technique}/{combination_technique}",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": normalization_technique},  # USE PARAMETER
                            "combination": {
                                "technique": combination_technique,  # USE PARAMETER
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
                    #'docid': hit['_source'].get('product_id', hit['_id']),
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


def main():
    parser = argparse.ArgumentParser(description='Train O19S Weight Predictor Model')
    
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
    
    # Data paths
    parser.add_argument('--o19s-data-path', type=str, 
                       default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', type=str,
                       default='dynamic_hybrid/data/ratings.csv',
                       help='Path to ratings file')
    
    # Training parameters
    parser.add_argument('--sample-size', type=int,
                       help='Number of queries for training (None = use all)')
    parser.add_argument('--weights', type=str, default='0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0',
                       help='Comma-separated weights to test')
    parser.add_argument('--alpha', type=float, default=0.1,
                       help='Ridge regularization parameter (lower for weight prediction)')
    parser.add_argument('--test-size', type=float, default=0.2,
                       help='Test set fraction')
    parser.add_argument('--use-cross-validation', action='store_true',
                       help='Use cross-validation')
    parser.add_argument('--use-fixed-queries', action='store_true',
                       help='Use fixed query order from CSV/parquet')
    parser.add_argument('--data-source', type=str, default='csv', choices=['csv', 'parquet'],
                       help='Data source: csv (O19S CSV files) or parquet (ESCI parquet files)')
    
    # Output
    parser.add_argument('--output-model', type=str, 
                       default='o19s_weight_predictor_model.pkl',
                       help='Output model file')
    
    args = parser.parse_args()
    
    # Parse weights
    weights_to_test = [float(w) for w in args.weights.split(',')]
    
    # Initialize trainer
    trainer = O19SWeightPredictorTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index_name,
        model_id=args.model_id
    )
    
    logger.info("=" * 80)
    logger.info("O19S Weight Predictor Model Training")
    logger.info("=" * 80)
    logger.info(f"Target: Predict optimal weight directly (not NDCG)")
    logger.info(f"Features: 17 (5 query + 12 corpus, NO weight feature)")
    logger.info(f"Model: Ridge regression with StandardScaler")
    logger.info(f"Alpha: {args.alpha}")
    logger.info(f"Data source: {args.data_source}")
    logger.info("=" * 80)
    
    # Collect training data
    X_features, y_weights, query_ids = trainer.collect_training_data(
        o19s_data_path=args.o19s_data_path,
        ratings_file=args.ratings_file,
        sample_size=args.sample_size,
        weights_to_test=weights_to_test,
        use_fixed_queries=args.use_fixed_queries,
        data_source=args.data_source
    )
    
    logger.info(f"\nTraining data collected:")
    logger.info(f"  Samples: {len(X_features)}")
    logger.info(f"  Features: {X_features.shape[1]}")
    logger.info(f"  Target range: [{y_weights.min():.2f}, {y_weights.max():.2f}]")
    
    # Train model
    model = trainer.train_model(
        X_features=X_features,
        y_weights=y_weights,
        query_ids=query_ids,
        alpha=args.alpha,
        test_size=args.test_size,
        use_cross_validation=args.use_cross_validation
    )
    
    # Save model
    trainer.save_model(model, args.output_model)
    
    logger.info("\n" + "=" * 80)
    logger.info("Training Complete - Summary")
    logger.info("=" * 80)
    logger.info(f"Total unique queries used for training: {trainer.total_queries_used}")
    logger.info(f"Total training samples collected: {len(X_features)}")
    logger.info(f"Model saved to: {args.output_model}")
    logger.info(f"Remote OpenSearch: {args.host}:{args.port}")
    logger.info(f"Index: {args.index_name}")
    logger.info(f"Model ID: {args.model_id}")
    logger.info("Use the corresponding evaluation script to test the model")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
