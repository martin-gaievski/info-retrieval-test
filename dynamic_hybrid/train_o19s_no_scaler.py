#!/usr/bin/env python3
"""
O19S Exact Training Approach - No Scalers

This script implements the exact training methodology from O19S:
- Plain LinearRegression (no Ridge regularization)
- No feature scaling or normalization
- No amplification or transformations
- Direct feature usage as-is
"""

import json
import numpy as np
import pandas as pd
import pickle
import argparse
from typing import Dict, List, Tuple
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import ShuffleSplit, cross_val_score, train_test_split
from sklearn.metrics import mean_squared_error, r2_score, make_scorer
from opensearchpy import OpenSearch
import warnings
warnings.filterwarnings('ignore')

# Import feature extractor and metrics
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.feature_extractor_o19s_exact import O19SExactFeatureExtractor
from dynamic_hybrid.utils import metrics


def root_mean_squared_error(y_true, y_pred):
    """Calculate RMSE"""
    return np.sqrt(mean_squared_error(y_true, y_pred))


class O19SNoScalerTrainer:
    """O19S training without any scalers - exact methodology"""
    
    def __init__(self, model_id: str, host: str = 'localhost', port: int = 9200):
        """
        Initialize trainer
        
        Args:
            model_id: Neural model ID for OpenSearch  
            host: OpenSearch host
            port: OpenSearch port
        """
        # Initialize OpenSearch client
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=None,
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        self.model_id = model_id
        self.index_name = "esci-products"
        
        # Initialize feature extractor
        self.feature_extractor = O19SExactFeatureExtractor(
            client=self.client,
            index_name=self.index_name
        )
        
    def extract_features(self, query: str) -> List[float]:
        """
        Extract features using O19S exact methodology
        
        Args:
            query: Search query
            
        Returns:
            List of feature values (no weight included)
        """
        features_dict = self.feature_extractor.extract_features(query)
        
        # O19S exact feature order (18 features - weight will be added separately)
        # Maps to: f_2_query_length, f_4_has_special_char, etc.
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
        
        return features
    
    def hybrid_search(self, query: str, weight: float, size: int = 100) -> List[str]:
        """
        Perform hybrid search with given weight
        
        Args:
            query: Search query
            weight: Neural search weight
            size: Number of results
            
        Returns:
            List of document IDs
        """
        lexical_weight = round(1.0 - weight, 2)
        neural_weight = round(weight, 2)
        
        body = {
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
                "description": "O19S no-scaler training pipeline",
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
            "size": size
        }
        
        try:
            response = self.client.search(index=self.index_name, body=body)
            return [hit['_id'] for hit in response['hits']['hits']]
        except Exception as e:
            print(f"Search failed for query '{query}': {e}")
            return []
    
    def collect_training_data(self, queries_df: pd.DataFrame, 
                            weights: List[float] = None,
                            augment: bool = False) -> Tuple[np.ndarray, np.ndarray]:
        """
        Collect training data following O19S exact methodology
        
        Args:
            queries_df: DataFrame with queries and ratings
            weights: List of weights to evaluate
            augment: Whether to use data augmentation
            
        Returns:
            Tuple of (features, targets)
        """
        if weights is None:
            if augment:
                # With augmentation: more weight samples
                weights = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
            else:
                # Without augmentation: fewer samples
                weights = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 0.9]
        
        X_list = []
        y_list = []
        
        print(f"\nCollecting training data for {len(queries_df)} queries...")
        print(f"Using weights: {weights}")
        
        for idx, row in queries_df.iterrows():
            query = row['query']
            relevance_dict = row['ratings']
            
            # Skip if no relevant items
            if not relevance_dict:
                continue
            
            # Extract query features once
            query_features = self.extract_features(query)
            
            # Create reference DataFrame for NDCG calculation
            reference_df = pd.DataFrame([
                {'docid': doc_id, 'rating': rating}
                for doc_id, rating in relevance_dict.items()
            ])
            
            # Evaluate each weight
            for weight in weights:
                # Create feature vector with weight as first feature (O19S style)
                # f_0_neuralness is the weight
                features = [weight] + query_features
                
                # Get search results
                search_results = self.hybrid_search(query, weight, size=100)
                
                # Calculate NDCG
                if search_results:
                    search_df = pd.DataFrame([
                        {'product_id': doc_id, 'position': pos, 'relevance': 1.0}
                        for pos, doc_id in enumerate(search_results[:10])
                    ])
                    
                    # Merge with ratings
                    merged_df = search_df.merge(
                        reference_df,
                        left_on='product_id',
                        right_on='docid',
                        how='left'
                    )
                    merged_df['rating'] = merged_df['rating'].fillna(0)
                    
                    # Calculate NDCG
                    if not merged_df.empty:
                        ndcg = metrics.ndcg_at_10(merged_df, reference=reference_df)
                    else:
                        ndcg = 0.0
                else:
                    ndcg = 0.0
                
                X_list.append(features)
                y_list.append(ndcg)
            
            if (idx + 1) % 10 == 0:
                print(f"  Processed {idx + 1} queries, {len(X_list)} samples collected...")
        
        X = np.array(X_list)
        y = np.array(y_list)
        
        print(f"\nTotal samples collected: {len(X)}")
        print(f"Feature shape: {X.shape}")
        print(f"Target shape: {y.shape}")
        
        return X, y


def load_training_queries(query_file: str = "dynamic_hybrid/data/query_train.csv",
                         ratings_file: str = "dynamic_hybrid/data/ratings.csv",
                         sample_size: int = None) -> pd.DataFrame:
    """
    Load training queries from query_train.csv with ratings
    
    Args:
        query_file: Path to query_train.csv
        ratings_file: Path to ratings.csv
        sample_size: Number of queries to use
        
    Returns:
        DataFrame with queries and ratings
    """
    print(f"\n📂 Loading TRAINING data from {query_file}")
    
    # Load training queries
    queries_df = pd.read_csv(query_file)
    print(f"  Loaded {len(queries_df)} training queries")
    
    # Load ratings - Handle tab-delimited file with no headers
    ratings_df = pd.read_csv(ratings_file, sep='\t', header=None, 
                           names=['query_string', 'product_id', 'esci_label', 'query_id'],
                           on_bad_lines='skip')
    print(f"  Loaded {len(ratings_df)} rating entries")
    
    # Group ratings by query_string
    ratings_grouped = ratings_df.groupby('query_string').apply(
        lambda x: dict(zip(x['product_id'], x['esci_label']))
    ).to_dict()
    
    # Map ratings using query_string
    queries_df['ratings'] = queries_df['query_string'].map(ratings_grouped)
    queries_df['query'] = queries_df['query_string']
    
    # Filter queries with ratings
    queries_with_ratings = queries_df[queries_df['ratings'].notna()]
    print(f"  Training queries with ratings: {len(queries_with_ratings)}")
    
    # Apply sample size if specified
    if sample_size:
        queries_with_ratings = queries_with_ratings.head(sample_size)
        print(f"  Using sample size: {sample_size}")
    
    return queries_with_ratings


def main():
    parser = argparse.ArgumentParser(description='Train O19S Model Without Scalers')
    parser.add_argument('--model-id', type=str, required=True,
                       help='Neural model ID in OpenSearch')
    parser.add_argument('--host', type=str, default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    parser.add_argument('--sample-size', type=int, default=100,
                       help='Number of queries to use for training')
    parser.add_argument('--augment', action='store_true',
                       help='Use data augmentation with more weight samples')
    parser.add_argument('--cv-splits', type=int, default=5,
                       help='Number of cross-validation splits')
    parser.add_argument('--test-size', type=float, default=0.2,
                       help='Test size for cross-validation')
    parser.add_argument('--random-state', type=int, default=42,
                       help='Random state for reproducibility')
    parser.add_argument('--output-prefix', type=str, default='o19s_no_scaler',
                       help='Prefix for output files')
    
    args = parser.parse_args()
    
    print("O19S No-Scaler Training (Exact Methodology)")
    print("=" * 50)
    
    # Load training data
    queries_df = load_training_queries(sample_size=args.sample_size)
    
    # Initialize trainer
    print(f"\nInitializing trainer with model ID: {args.model_id}")
    trainer = O19SNoScalerTrainer(
        model_id=args.model_id,
        host=args.host,
        port=args.port
    )
    
    # Collect training data
    X, y = trainer.collect_training_data(queries_df, augment=args.augment)
    
    # Split into train/test (80/20)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    print(f"\nTraining set size: {len(X_train)}")
    print(f"Test set size: {len(X_test)}")
    
    # Initialize model - plain LinearRegression as per O19S
    model = LinearRegression()
    
    # Cross-validation evaluation
    print(f"\nPerforming {args.cv_splits}-fold cross-validation...")
    cv = ShuffleSplit(n_splits=args.cv_splits, test_size=args.test_size, 
                      random_state=args.random_state)
    
    # RMSE scorer
    rmse_scorer = make_scorer(root_mean_squared_error)
    rmse_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring=rmse_scorer)
    
    print(f"Cross-Validation RMSE: {np.mean(rmse_scores):.4f} (+/- {np.std(rmse_scores):.4f})")
    
    # Train final model
    print("\nTraining final model...")
    model.fit(X_train, y_train)
    
    # Evaluate on test set
    y_pred = model.predict(X_test)
    
    rmse = root_mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"\nTest Set Performance:")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  R² Score: {r2:.4f}")
    
    # Analyze feature importance (coefficients)
    feature_names = ['f_0_neuralness (weight)'] + [
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
    
    coefficients = pd.DataFrame({
        'feature': feature_names,
        'coefficient': model.coef_,
        'abs_coefficient': np.abs(model.coef_)
    }).sort_values('abs_coefficient', ascending=False)
    
    print("\nTop 10 Feature Coefficients (by magnitude):")
    for idx, row in coefficients.head(10).iterrows():
        print(f"  {row['feature']:40s}: {row['coefficient']:+.6f}")
    
    # Check weight feature importance
    weight_rank = coefficients[coefficients['feature'].str.contains('neuralness')].index[0] + 1
    weight_coef = coefficients[coefficients['feature'].str.contains('neuralness')]['coefficient'].values[0]
    
    print(f"\nWeight Feature Analysis:")
    print(f"  Coefficient: {weight_coef:+.6f}")
    print(f"  Rank: {weight_rank}/{len(feature_names)}")
    
    # Save model
    model_file = f"{args.output_prefix}_model.pkl"
    with open(model_file, 'wb') as f:
        pickle.dump(model, f)
    print(f"\nModel saved to {model_file}")
    
    # Save metadata
    metadata = {
        'training_samples': len(X_train),
        'test_samples': len(X_test),
        'num_features': X.shape[1],
        'cv_rmse_mean': float(np.mean(rmse_scores)),
        'cv_rmse_std': float(np.std(rmse_scores)),
        'test_rmse': float(rmse),
        'test_r2': float(r2),
        'weight_coefficient': float(weight_coef),
        'weight_rank': int(weight_rank),
        'intercept': float(model.intercept_),
        'coefficients': model.coef_.tolist(),
        'feature_names': feature_names,
        'augmentation': args.augment,
        'sample_size': args.sample_size
    }
    
    metadata_file = f"{args.output_prefix}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"Metadata saved to {metadata_file}")
    
    print("\n" + "=" * 50)
    print("Training Complete!")
    print(f"\nNext step: Run evaluation with the trained model")
    print(f"python dynamic_hybrid/evaluate_o19s_no_scaler.py \\")
    print(f"    --model-file {model_file} \\")
    print(f"    --model-id {args.model_id} \\")
    print(f"    --host {args.host} \\")
    print(f"    --port {args.port}")


if __name__ == "__main__":
    main()
