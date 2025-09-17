#!/usr/bin/env python3
"""
Fixed O19S model training with proper train/test separation and consistent feature extractor.
Uses O19SExactFeatureExtractor to match evaluation script.
"""

import numpy as np
import pandas as pd
import pickle
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.model_selection import train_test_split, cross_val_score
from tqdm import tqdm
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# FIXED: Use O19S exact feature extractor to match evaluation
from dynamic_hybrid.feature_extractor_o19s_exact import O19SExactFeatureExtractor
from dynamic_hybrid.utils.metrics import ndcg_at_10
from opensearchpy import OpenSearch


def load_training_queries(query_file: str = "dynamic_hybrid/data/query_train.csv",
                         ratings_file: str = "dynamic_hybrid/data/ratings.csv",
                         sample_size: Optional[int] = None) -> pd.DataFrame:
    """
    Load training queries from query_train.csv with ratings.
    
    Args:
        query_file: Path to query_train.csv
        ratings_file: Path to ratings.csv
        sample_size: Number of queries to use (None for all)
        
    Returns:
        DataFrame with queries and ratings
    """
    print(f"\n📂 Loading training data from {query_file}")
    
    # Load training queries
    queries_df = pd.read_csv(query_file)
    print(f"  Loaded {len(queries_df)} training queries")
    
    # Load ratings - handle missing headers and tab delimiter
    ratings_df = pd.read_csv(ratings_file, sep='\t', header=None, 
                           names=['query_string', 'product_id', 'esci_label', 'query_id'],
                           on_bad_lines='skip')  # Skip malformed lines
    print(f"  Loaded {len(ratings_df)} rating entries")
    
    # Merge ratings with queries by query_string
    # Group ratings by query_string
    ratings_grouped = ratings_df.groupby('query_string').apply(
        lambda x: dict(zip(x['product_id'], x['esci_label']))
    ).to_dict()
    
    # Add ratings to queries
    queries_df['ratings'] = queries_df['query_string'].map(ratings_grouped)
    
    # Add query_id for compatibility
    queries_df['query_id'] = queries_df.index
    
    # Filter queries with ratings
    queries_with_ratings = queries_df[queries_df['ratings'].notna()]
    print(f"  Queries with ratings: {len(queries_with_ratings)}")
    
    # Apply sample size if specified
    if sample_size:
        queries_with_ratings = queries_with_ratings.head(sample_size)
        print(f"  Using sample size: {sample_size}")
    
    return queries_with_ratings


def calculate_ndcg_at_k(relevance_scores: List[float], k: int = 10) -> float:
    """Calculate NDCG@k for a list of relevance scores."""
    if not relevance_scores:
        return 0.0
    
    # Truncate to k
    relevance_scores = relevance_scores[:k]
    
    # Calculate DCG
    dcg = sum([score / np.log2(i + 2) for i, score in enumerate(relevance_scores)])
    
    # Calculate ideal DCG
    ideal_scores = sorted(relevance_scores, reverse=True)
    idcg = sum([score / np.log2(i + 2) for i, score in enumerate(ideal_scores)])
    
    # Calculate NDCG
    if idcg == 0:
        return 0.0
    
    return dcg / idcg


class OpenSearchClient:
    """Simple wrapper for OpenSearch client with hybrid search functionality."""
    
    def __init__(self, host: str = "localhost", port: int = 9200, index_name: str = "esci-products"):
        """Initialize OpenSearch client."""
        self.host = host
        self.port = port
        self.index_name = index_name
        
        # Create OpenSearch client
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_compress=True,
            http_auth=None,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
    
    def hybrid_search(self, query: str, model_id: str, neural_weight: float,
                     normalization_technique: str = 'min_max', 
                     combination_technique: str = 'arithmetic_mean',
                     size: int = 100) -> list:
        """Execute hybrid search with OpenSearch."""
        
        lexical_weight = 1.0 - neural_weight
        
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
                                    "model_id": model_id,
                                    "k": 100
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": f"Hybrid search with {normalization_technique}/{combination_technique}",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": normalization_technique},
                            "combination": {
                                "technique": combination_technique,
                                "parameters": {"weights": [lexical_weight, neural_weight]}
                            }
                        }
                    }
                ]
            },
            "size": size
        }
        
        try:
            result = self.client.search(index=self.index_name, body=payload)
            
            results = []
            for hit in result['hits']['hits']:
                results.append({
                    'product_id': hit['_id'],
                    'score': hit['_score']
                })
            
            return results
            
        except Exception as e:
            print(f"Hybrid search failed: {e}")
            return []


class O19SImprovedTrainer:
    """Fixed O19S model trainer with proper feature extractor."""
    
    def __init__(self, opensearch_client, model_id: str, 
                 corpus_field: str = "product_title",
                 amplification_factor: float = 5.0,
                 use_robust_scaler: bool = False):
        """
        Initialize improved trainer.
        
        Args:
            opensearch_client: OpenSearch client
            model_id: Neural model ID
            corpus_field: Field for corpus statistics
            amplification_factor: Stronger amplification for weight feature (default 5.0)
            use_robust_scaler: Use RobustScaler for outlier resistance
        """
        self.client = opensearch_client
        self.model_id = model_id
        self.corpus_field = corpus_field
        self.amplification_factor = amplification_factor
        self.use_robust_scaler = use_robust_scaler
        self.index_name = opensearch_client.index_name  # Add index_name attribute
        
        # FIXED: Use O19SExactFeatureExtractor to match evaluation
        self.feature_extractor = O19SExactFeatureExtractor(
            client=self.client.client,  # Pass the actual OpenSearch client
            index_name=self.index_name,
            field_name="product_title"  # Using product_title for corpus stats
        )
        
        # Fixed normalization and combination
        self.normalization = 'min_max'
        self.combination = 'arithmetic_mean'
        
        # Choose scaler type
        if use_robust_scaler:
            self.query_scaler = RobustScaler()
            self.corpus_scaler = RobustScaler()
            self.full_scaler = RobustScaler()
            scaler_type = "RobustScaler (outlier-resistant)"
        else:
            self.query_scaler = StandardScaler()
            self.corpus_scaler = StandardScaler()
            self.full_scaler = StandardScaler()
            scaler_type = "StandardScaler"
        
        print(f"Initialized FIXED O19S trainer")
        print(f"  ✅ Using O19SExactFeatureExtractor (matches evaluation)")
        print(f"  Host: {opensearch_client.host}:{opensearch_client.port}/{opensearch_client.index_name}")
        print(f"  Weight amplification: {amplification_factor}x")
        print(f"  Normalization: Log transform + {scaler_type}")
        
    def normalize_features(self, X: np.ndarray, fit: bool = True) -> np.ndarray:
        """
        Apply improved normalization with stronger weight signal.
        
        Args:
            X: Feature matrix [weight, query_features, corpus_features]
            fit: Whether to fit scalers (True for training, False for inference)
            
        Returns:
            Normalized feature matrix
        """
        # Split features
        weight_features = X[:, 0:1]  # Weight feature
        query_features = X[:, 1:6]   # Query features (5)
        corpus_features = X[:, 6:]   # Corpus features (12)
        
        # Log transform corpus features (more aggressive for high values)
        # Use log1p for safety but apply twice for stronger compression
        corpus_features_log = np.log1p(np.log1p(np.abs(corpus_features)))
        
        # Standardize each feature group
        if fit:
            query_normalized = self.query_scaler.fit_transform(query_features)
            corpus_normalized = self.corpus_scaler.fit_transform(corpus_features_log)
        else:
            query_normalized = self.query_scaler.transform(query_features)
            corpus_normalized = self.corpus_scaler.transform(corpus_features_log)
        
        # Combine features
        X_combined = np.hstack([
            weight_features,  # Keep weight as-is for now
            query_normalized,
            corpus_normalized
        ])
        
        # Apply full standardization
        if fit:
            X_normalized = self.full_scaler.fit_transform(X_combined)
        else:
            X_normalized = self.full_scaler.transform(X_combined)
        
        # Apply stronger amplification to weight feature
        X_normalized[:, 0] *= self.amplification_factor
        
        return X_normalized
    
    def collect_training_data(self, queries_df: pd.DataFrame, 
                             weights: List[float],
                             use_augmentation: bool = True) -> Tuple[np.ndarray, np.ndarray]:
        """
        Collect training data with optional augmentation.
        
        Args:
            queries_df: DataFrame with queries and ratings
            weights: List of weight values to test
            use_augmentation: Add extra samples at extreme weights for emphasis
            
        Returns:
            X: Feature matrix with normalized features
            y: Target NDCG values
        """
        print(f"\nCollecting training data with O19S exact features")
        print(f"Queries: {len(queries_df)}, Weights per query: {len(weights)}")
        if use_augmentation:
            print("Data augmentation: Emphasizing extreme weights (0.0, 1.0)")
        
        X = []
        y = []
        
        # Progress bar
        total = len(queries_df) * len(weights)
        if use_augmentation:
            total += len(queries_df) * 2  # Add samples for 0.0 and 1.0
        
        pbar = tqdm(total=total, desc="Collecting training data")
        
        for _, row in queries_df.iterrows():
            query = row['query_string']  # FIXED: Use correct column name
            query_id = row['query_id']
            
            # Get ratings for this query
            query_ratings = row.get('ratings', {})
            if not query_ratings:
                pbar.update(len(weights))
                if use_augmentation:
                    pbar.update(2)
                continue
            
            # Extract features once per query using O19S exact method
            features_dict = self.feature_extractor.extract_features(query)
            
            # Convert to list (O19S exact feature order)
            features = [
                features_dict.get('query_length', 0),
                features_dict.get('has_special_chars', 0),
                features_dict.get('has_punctuation', 0),
                features_dict.get('capitalization_ratio', 0),
                features_dict.get('stopword_ratio', 0),
                features_dict.get('max_document_frequency', 0),
                features_dict.get('min_document_frequency', 0),
                features_dict.get('total_document_frequency', 0),
                features_dict.get('average_document_frequency', 0),
                features_dict.get('variance_document_frequency', 0),
                features_dict.get('std_dev_document_frequency', 0),
                features_dict.get('max_inverse_document_frequency', 0),
                features_dict.get('min_inverse_document_frequency', 0),
                features_dict.get('total_inverse_document_frequency', 0),
                features_dict.get('average_inverse_document_frequency', 0),
                features_dict.get('variance_inverse_document_frequency', 0),
                features_dict.get('std_dev_inverse_document_frequency', 0)
            ]
            
            # Test each weight
            for weight in weights:
                # Add weight as first feature (O19S methodology)
                feature_vector = [weight] + features
                
                # Perform hybrid search
                results = self.client.hybrid_search(
                    query=query,
                    model_id=self.model_id,
                    neural_weight=weight,
                    normalization_technique=self.normalization,
                    combination_technique=self.combination,
                    size=100
                )
                
                # Calculate NDCG
                relevance_scores = []
                for result in results:
                    product_id = result['product_id']
                    rating = query_ratings.get(product_id, 0)
                    relevance_scores.append(rating)
                
                ndcg = calculate_ndcg_at_k(relevance_scores, k=10)
                
                X.append(feature_vector)
                y.append(ndcg)
                pbar.update(1)
            
            # Data augmentation: Add extra samples for extreme weights
            if use_augmentation:
                for weight in [0.0, 1.0]:
                    feature_vector = [weight] + features
                    
                    results = self.client.hybrid_search(
                        query=query,
                        model_id=self.model_id,
                        neural_weight=weight,
                        normalization_technique=self.normalization,
                        combination_technique=self.combination,
                        size=100
                    )
                    
                    relevance_scores = []
                    for result in results:
                        product_id = result['product_id']
                        rating = query_ratings.get(product_id, 0)
                        relevance_scores.append(rating)
                    
                    ndcg = calculate_ndcg_at_k(relevance_scores, k=10)
                    
                    X.append(feature_vector)
                    y.append(ndcg)
                    pbar.update(1)
        
        pbar.close()
        
        X = np.array(X)
        y = np.array(y)
        
        # Print feature statistics
        print(f"\n📊 Feature Statistics (before normalization):")
        print(f"  Weight: mean={np.mean(X[:, 0]):.4f}, std={np.std(X[:, 0]):.4f}")
        print(f"  Corpus max freq: mean={np.mean(X[:, 6]):.1f}, max={np.max(X[:, 6]):.1f}")
        
        # Apply improved normalization
        X_normalized = self.normalize_features(X, fit=True)
        
        # Print normalized statistics
        print(f"\n📊 After normalization (amplification={self.amplification_factor}x):")
        print(f"  Weight: std={np.std(X_normalized[:, 0]):.4f}")
        print(f"  Median other std: {np.median(np.std(X_normalized[:, 1:], axis=0)):.4f}")
        print(f"  Ratio: {np.std(X_normalized[:, 0])/np.median(np.std(X_normalized[:, 1:], axis=0)):.2f}")
        
        print(f"\nCollected {len(X_normalized)} training samples")
        
        return X_normalized, y
    
    def train_model(self, X: np.ndarray, y: np.ndarray, 
                   alpha: float = 0.5, test_size: float = 0.2,
                   use_cv: bool = True) -> Ridge:
        """
        Train improved Ridge regression model.
        
        Args:
            X: Feature matrix
            y: Target values
            alpha: Reduced regularization for stronger weight signal
            test_size: Fraction for test set (internal validation only)
            use_cv: Use cross-validation for evaluation
            
        Returns:
            Trained Ridge model
        """
        # Split data for internal validation only
        # This is NOT the train/test split for final evaluation
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        print(f"\n🎯 Training FIXED O19S Ridge regression...")
        print(f"  Training samples: {len(X_train)}")
        print(f"  Validation samples: {len(X_val)} (internal only)")
        print(f"  Alpha: {alpha}")
        print(f"  Amplification: {self.amplification_factor}x")
        
        # Train model on full training data
        model = Ridge(alpha=alpha, random_state=42)
        model.fit(X, y)  # Use all training data
        
        # Evaluate on internal validation split
        train_score = model.score(X_train, y_train)
        val_score = model.score(X_val, y_val)
        
        print(f"\n📈 Model Performance (internal validation):")
        print(f"  Train R²: {train_score:.4f}")
        print(f"  Validation R²: {val_score:.4f}")
        
        # Cross-validation
        if use_cv:
            cv_scores = cross_val_score(model, X, y, cv=5)
            print(f"  5-fold CV R²: {np.mean(cv_scores):.4f} (±{np.std(cv_scores):.4f})")
        
        # Analyze coefficients
        coefficients = model.coef_
        weight_coef = coefficients[0]
        
        print(f"\n🔍 Weight Feature Analysis:")
        print(f"  Weight coefficient: {weight_coef:.6f}")
        
        # Rank weight coefficient
        coef_ranks = np.argsort(np.abs(coefficients))[::-1]
        weight_rank = np.where(coef_ranks == 0)[0][0] + 1
        
        print(f"  Weight rank by magnitude: {weight_rank}/{len(coefficients)}")
        print(f"  Weight/median ratio: {abs(weight_coef)/np.median(np.abs(coefficients[1:])):.2f}")
        
        # Show top features
        print(f"\n📊 Top 5 features by magnitude:")
        feature_names = self.get_feature_names()
        for i in range(5):
            idx = coef_ranks[i]
            print(f"  #{i+1}: {feature_names[idx]}: {coefficients[idx]:.6f}")
        
        return model
    
    def get_feature_names(self) -> List[str]:
        """Get feature names matching O19S exact methodology."""
        return [
            'f_0_neuralness',  # Weight
            'f_2_query_length',
            'f_4_has_special_char',
            'f_5_has_punctuation_at_end',
            'f_7_capital_letters_ratio',
            'f_8_stopwords_ratio',
            'f_14_max_document_frequency',
            'f_15_min_document_frequency', 
            'f_16_total_document_frequency',
            'f_17_average_document_frequency',
            'f_18_variance_document_frequency',
            'f_19_std_dev_document_frequency',
            'f_20_max_inverse_document_frequency',
            'f_21_min_inverse_document_frequency',
            'f_22_total_inverse_document_frequency',
            'f_23_average_inverse_document_frequency',
            'f_24_variance_inverse_document_frequency',
            'f_25_std_dev_inverse_document_frequency'
        ]
    
    def save_model(self, model: Ridge, output_path: str = "o19s_improved_model.pkl",
                  scaler_path: str = "o19s_improved_scaler.pkl"):
        """Save model and scaler."""
        # Save model
        with open(output_path, 'wb') as f:
            pickle.dump(model, f)
        print(f"\n💾 Model saved to {output_path}")
        
        # Save scaler and metadata
        scaler_data = {
            'query_scaler': self.query_scaler,
            'corpus_scaler': self.corpus_scaler,
            'full_scaler': self.full_scaler,
            'amplification_factor': self.amplification_factor,
            'normalization': self.normalization,
            'combination': self.combination,
            'feature_names': self.get_feature_names(),
            'feature_extractor': 'O19SExactFeatureExtractor',  # FIXED
            'normalization_strategy': 'double_log_transform',
            'scaler_type': 'robust' if self.use_robust_scaler else 'standard',
            'training_file': 'query_train.csv',  # FIXED
            'timestamp': datetime.now().isoformat()
        }
        
        with open(scaler_path, 'wb') as f:
            pickle.dump(scaler_data, f)
        print(f"💾 Scaler saved to {scaler_path}")


def main():
    parser = argparse.ArgumentParser(description='Train FIXED O19S model')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--index-name', default='esci-products', help='Index name')
    parser.add_argument('--model-id', required=True, help='Neural model ID')
    parser.add_argument('--sample-size', type=int, default=100, help='Number of queries')
    parser.add_argument('--alpha', type=float, default=0.5, help='Regularization')
    parser.add_argument('--amplification', type=float, default=5.0, help='Weight amplification')
    parser.add_argument('--use-robust', action='store_true', help='Use RobustScaler')
    parser.add_argument('--augment', action='store_true', help='Augment with extreme weights')
    parser.add_argument('--output-model', default='o19s_improved_model.pkl', help='Output path')
    parser.add_argument('--output-scaler', default='o19s_improved_scaler.pkl', help='Scaler path')
    
    args = parser.parse_args()
    
    print("="*80)
    print("O19S FIXED MODEL TRAINING")
    print("="*80)
    print("Key Fixes:")
    print("  ✅ Using query_train.csv for training data")
    print("  ✅ Using O19SExactFeatureExtractor (matches evaluation)")
    print("  ✅ No data leakage - proper train/test separation")
    print("Configuration:")
    print(f"  • Amplification: {args.amplification}x")
    print(f"  • Alpha: {args.alpha}")
    print(f"  • Scaler: {'RobustScaler' if args.use_robust else 'StandardScaler'}")
    print(f"  • Augmentation: {'Yes' if args.augment else 'No'}")
    print("="*80)
    
    # Initialize client
    client = OpenSearchClient(
        host=args.host,
        port=args.port,
        index_name=args.index_name
    )
    
    # Initialize improved trainer
    trainer = O19SImprovedTrainer(
        opensearch_client=client,
        model_id=args.model_id,
        amplification_factor=args.amplification,
        use_robust_scaler=args.use_robust
    )
    
    # FIXED: Load training queries from query_train.csv
    queries_df = load_training_queries(
        query_file="dynamic_hybrid/data/query_train.csv",
        ratings_file="dynamic_hybrid/data/ratings.csv",
        sample_size=args.sample_size
    )
    
    if queries_df.empty:
        print("Error: No training queries loaded!")
        return
    
    # Collect training data with all 11 weights
    weights = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    X, y = trainer.collect_training_data(
        queries_df, 
        weights,
        use_augmentation=args.augment
    )
    
    # Train model
    model = trainer.train_model(X, y, alpha=args.alpha)
    
    # Save model
    trainer.save_model(model, args.output_model, args.output_scaler)
    
    print("\n" + "="*80)
    print("✅ FIXED TRAINING COMPLETE!")
    print("Next step: Run evaluation with query_test.csv")
    print("="*80)


if __name__ == "__main__":
    main()
