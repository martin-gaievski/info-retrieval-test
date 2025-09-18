#!/usr/bin/env python3
"""
O19S Training with MinMaxScaler Feature Normalization

This script implements linear regression with proper feature scaling:
- MinMaxScaler for feature normalization (0-1 range)
- Plain LinearRegression (no Ridge regularization)
- Saves both model and scaler for evaluation
- Solves the scale imbalance problem identified in no-scaler version
"""

import json
import numpy as np
import pandas as pd
import pickle
import argparse
from typing import Dict, List, Tuple
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler
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


class O19SScaledTrainer:
    """O19S training with MinMaxScaler normalization"""
    
    def __init__(self, model_id: str, host: str = 'localhost', port: int = 9200):
        """
        Initialize trainer with MinMaxScaler
        
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
        
        # Initialize MinMaxScaler for feature normalization
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        
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
                "description": "O19S scaled training pipeline",
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
    parser = argparse.ArgumentParser(description='Train O19S Model with MinMaxScaler')
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
    parser.add_argument('--output-prefix', type=str, default='o19s_minmax_scaled',
                       help='Prefix for output files')
    
    args = parser.parse_args()
    
    print("O19S Training with MinMaxScaler Feature Normalization")
    print("=" * 50)
    print("\n✅ This implementation solves the feature scale imbalance problem!")
    print("   All features will be normalized to 0-1 range using MinMaxScaler")
    
    # Load training data
    queries_df = load_training_queries(sample_size=args.sample_size)
    
    # Initialize trainer
    print(f"\nInitializing trainer with model ID: {args.model_id}")
    trainer = O19SScaledTrainer(
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
    
    # Fit MinMaxScaler on training data
    print("\n🔧 Fitting MinMaxScaler on training data...")
    X_train_scaled = trainer.scaler.fit_transform(X_train)
    X_test_scaled = trainer.scaler.transform(X_test)
    
    # Show feature scale statistics before and after scaling
    print("\n📊 Feature Scale Analysis:")
    print("Before scaling (raw values):")
    print(f"  Min values: {X_train.min(axis=0)[:5]}... (showing first 5)")
    print(f"  Max values: {X_train.max(axis=0)[:5]}... (showing first 5)")
    print(f"  Range: {X_train.max(axis=0) - X_train.min(axis=0)}")
    
    print("\nAfter MinMaxScaler normalization:")
    print(f"  Min values: {X_train_scaled.min(axis=0)[:5]}... (all should be 0)")
    print(f"  Max values: {X_train_scaled.max(axis=0)[:5]}... (all should be 1)")
    print(f"  All features normalized to [0, 1] range ✓")
    
    # Initialize model - plain LinearRegression
    model = LinearRegression()
    
    # Cross-validation evaluation on scaled data
    print(f"\nPerforming {args.cv_splits}-fold cross-validation...")
    cv = ShuffleSplit(n_splits=args.cv_splits, test_size=args.test_size, 
                      random_state=args.random_state)
    
    # RMSE scorer
    rmse_scorer = make_scorer(root_mean_squared_error)
    rmse_scores = cross_val_score(model, X_train_scaled, y_train, cv=cv, scoring=rmse_scorer)
    
    print(f"Cross-Validation RMSE: {np.mean(rmse_scores):.4f} (+/- {np.std(rmse_scores):.4f})")
    
    # Train final model on scaled data
    print("\nTraining final model on scaled features...")
    model.fit(X_train_scaled, y_train)
    
    # Evaluate on test set
    y_pred = model.predict(X_test_scaled)
    
    rmse = root_mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"\n🎯 Test Set Performance:")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  R² Score: {r2:.4f}")
    
    # Analyze predictions
    print(f"\n📈 Prediction Analysis:")
    print(f"  Mean prediction: {np.mean(y_pred):.4f}")
    print(f"  Std prediction: {np.std(y_pred):.4f}")
    print(f"  Min prediction: {np.min(y_pred):.4f}")
    print(f"  Max prediction: {np.max(y_pred):.4f}")
    
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
    
    print("\n🏆 Top 10 Feature Coefficients (by magnitude):")
    for idx, row in coefficients.head(10).iterrows():
        print(f"  {row['feature']:40s}: {row['coefficient']:+.6f}")
    
    # Check weight feature importance
    weight_rank = coefficients[coefficients['feature'].str.contains('neuralness')].index[0] + 1
    weight_coef = coefficients[coefficients['feature'].str.contains('neuralness')]['coefficient'].values[0]
    
    print(f"\n⚖️ Weight Feature Analysis:")
    print(f"  Coefficient: {weight_coef:+.6f}")
    print(f"  Rank: {weight_rank}/{len(feature_names)}")
    print(f"  {'✓ Weight feature is properly utilized!' if weight_rank <= 5 else '⚠️ Weight feature still has low importance'}")
    
    # Save model
    model_file = f"{args.output_prefix}_model.pkl"
    with open(model_file, 'wb') as f:
        pickle.dump(model, f)
    print(f"\n💾 Model saved to {model_file}")
    
    # Save scaler (IMPORTANT for evaluation!)
    scaler_file = f"{args.output_prefix}_scaler.pkl"
    with open(scaler_file, 'wb') as f:
        pickle.dump(trainer.scaler, f)
    print(f"💾 Scaler saved to {scaler_file}")
    
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
        'sample_size': args.sample_size,
        'scaling': 'MinMaxScaler',
        'feature_range': [0, 1],
        'prediction_mean': float(np.mean(y_pred)),
        'prediction_std': float(np.std(y_pred))
    }
    
    metadata_file = f"{args.output_prefix}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"📋 Metadata saved to {metadata_file}")
    
    # Compare with no-scaler results if available
    try:
        with open('o19s_no_scaler.pkl_metadata.json', 'r') as f:
            no_scaler_meta = json.load(f)
            print("\n📊 Comparison with No-Scaler Model:")
            print(f"  No-Scaler R²: {no_scaler_meta.get('test_r2', 'N/A'):.4f}")
            print(f"  Scaled R²: {r2:.4f}")
            print(f"  Improvement: {r2 - no_scaler_meta.get('test_r2', 0):.4f}")
            print(f"  No-Scaler Weight Rank: {no_scaler_meta.get('weight_rank', 'N/A')}")
            print(f"  Scaled Weight Rank: {weight_rank}")
    except:
        pass
    
    print("\n" + "=" * 50)
    print("✅ Training Complete with MinMaxScaler!")
    print(f"\n🚀 Next Step: Create and run evaluation script with the scaler")
    print(f"   The evaluation script must load and use the saved scaler!")
    print(f"\n   Model file: {model_file}")
    print(f"   Scaler file: {scaler_file}")


if __name__ == "__main__":
    main()
