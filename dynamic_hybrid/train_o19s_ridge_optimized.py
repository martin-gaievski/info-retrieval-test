#!/usr/bin/env python3
"""
O19S Ridge Regression Training with Automated Alpha Optimization

This script implements Ridge Regression with automated hyperparameter tuning:
- MinMaxScaler for feature normalization (0-1 range)
- Ridge Regression with GridSearchCV for optimal alpha selection
- Cross-validation for robust evaluation
- Saves best model with optimal parameters
- Based on model_approach_recommendation_ecommerce.md recommendations
"""

import json
import numpy as np
import pandas as pd
import pickle
import argparse
from typing import Dict, List, Tuple, Optional
from sklearn.linear_model import Ridge
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.model_selection import (
    GridSearchCV, 
    ShuffleSplit, 
    cross_val_score, 
    train_test_split
)
from sklearn.metrics import mean_squared_error, r2_score, make_scorer
from opensearchpy import OpenSearch
import warnings
import time
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


class O19SRidgeOptimizedTrainer:
    """O19S training with Ridge Regression and automated alpha optimization"""
    
    def __init__(self, model_id: str, scaler_type: str = 'minmax', 
                 host: str = 'localhost', port: int = 9200):
        """
        Initialize trainer with configurable scaler and Ridge Regression
        
        Args:
            model_id: Neural model ID for OpenSearch  
            scaler_type: Type of scaler to use ('minmax' or 'standard')
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
        
        # Initialize scaler based on type
        self.scaler_type = scaler_type
        if scaler_type == 'minmax':
            self.scaler = MinMaxScaler(feature_range=(0, 1))
        elif scaler_type == 'standard':
            self.scaler = StandardScaler()
        else:
            raise ValueError(f"Invalid scaler_type: {scaler_type}. Must be 'minmax' or 'standard'")
        
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
                "description": "O19S Ridge optimized training pipeline",
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
            weights = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        
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

    def optimize_alpha(
        self, 
        X_train: np.ndarray, 
        y_train: np.ndarray,
        alpha_range: Optional[List[float]] = None,
        cv_folds: int = 5,
        scoring: str = 'neg_root_mean_squared_error',
        n_jobs: int = -1
    ) -> Tuple[float, Dict]:
        """
        Find optimal alpha using GridSearchCV
        
        Args:
            X_train: Training features (already scaled)
            y_train: Training targets
            alpha_range: Range of alpha values to test
            cv_folds: Number of cross-validation folds
            scoring: Scoring metric for optimization
            n_jobs: Number of parallel jobs
            
        Returns:
            Tuple of (best_alpha, cv_results)
        """
        if alpha_range is None:
            # Based on recommendation: test around 0.01
            # Also test wider range to find optimal value
            alpha_range = [
                0.0001, 0.0005, 0.001, 0.005, 
                0.01, 0.02, 0.05, 0.1, 
                0.2, 0.5, 1.0, 2.0, 5.0, 10.0
            ]
        
        print("\n🔍 Starting Alpha Optimization with GridSearchCV")
        print(f"Testing alpha values: {alpha_range}")
        print(f"Cross-validation folds: {cv_folds}")
        print(f"Scoring metric: {scoring}")
        
        # Create parameter grid
        param_grid = {'alpha': alpha_range}
        
        # Initialize Ridge model
        ridge_model = Ridge(solver='auto', random_state=42)
        
        # Setup GridSearchCV
        grid_search = GridSearchCV(
            estimator=ridge_model,
            param_grid=param_grid,
            cv=cv_folds,
            scoring=scoring,
            n_jobs=n_jobs,
            verbose=1,
            return_train_score=True
        )
        
        # Perform grid search
        start_time = time.time()
        grid_search.fit(X_train, y_train)
        search_time = time.time() - start_time
        
        # Extract results
        best_alpha = grid_search.best_params_['alpha']
        best_score = -grid_search.best_score_  # Negative because of neg_root_mean_squared_error
        
        print(f"\n✅ Grid Search Complete (took {search_time:.2f} seconds)")
        print(f"Best Alpha: {best_alpha}")
        print(f"Best CV RMSE: {best_score:.4f}")
        
        # Create detailed results
        cv_results = pd.DataFrame(grid_search.cv_results_)
        cv_results['rmse_test'] = -cv_results['mean_test_score']
        cv_results['rmse_train'] = -cv_results['mean_train_score']
        cv_results = cv_results.sort_values('rmse_test')
        
        print("\n📊 Top 5 Alpha Values by Performance:")
        print("-" * 60)
        print(f"{'Alpha':>10} | {'CV RMSE':>10} | {'Train RMSE':>10} | {'Overfit':>10}")
        print("-" * 60)
        
        for idx, row in cv_results.head(5).iterrows():
            alpha = row['param_alpha']
            test_rmse = row['rmse_test']
            train_rmse = row['rmse_train']
            overfit = test_rmse - train_rmse
            print(f"{alpha:>10.4f} | {test_rmse:>10.4f} | {train_rmse:>10.4f} | {overfit:>10.4f}")
        
        return best_alpha, cv_results.to_dict('records')


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


def load_test_queries(query_file: str = "dynamic_hybrid/data/query_test.csv",
                     ratings_file: str = "dynamic_hybrid/data/ratings.csv",
                     sample_size: int = None) -> pd.DataFrame:
    """
    Load test queries from query_test.csv with ratings (O19S methodology)
    
    Args:
        query_file: Path to query_test.csv
        ratings_file: Path to ratings.csv
        sample_size: Number of queries to use
        
    Returns:
        DataFrame with queries and ratings
    """
    print(f"\n📂 Loading TEST data from {query_file}")
    
    # Load test queries
    queries_df = pd.read_csv(query_file)
    print(f"  Loaded {len(queries_df)} test queries")
    
    # Load ratings - Handle tab-delimited file with no headers
    ratings_df = pd.read_csv(ratings_file, sep='\t', header=None, 
                           names=['query_string', 'product_id', 'esci_label', 'query_id'],
                           on_bad_lines='skip')
    
    # Group ratings by query_string
    ratings_grouped = ratings_df.groupby('query_string').apply(
        lambda x: dict(zip(x['product_id'], x['esci_label']))
    ).to_dict()
    
    # Map ratings using query_string
    queries_df['ratings'] = queries_df['query_string'].map(ratings_grouped)
    queries_df['query'] = queries_df['query_string']
    
    # Filter queries with ratings
    queries_with_ratings = queries_df[queries_df['ratings'].notna()]
    print(f"  Test queries with ratings: {len(queries_with_ratings)}")
    
    # Apply sample size if specified
    if sample_size:
        queries_with_ratings = queries_with_ratings.head(sample_size)
        print(f"  Using sample size: {sample_size}")
    
    return queries_with_ratings

def main():
    print("Test 1")
    parser = argparse.ArgumentParser(
        description='Train O19S Ridge Model with Automated Alpha Optimization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=''
    )
    
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
    parser.add_argument('--alpha-range', type=str, default=None,
                       help='Comma-separated list of alpha values to test')
    parser.add_argument('--cv-folds', type=int, default=5,
                       help='Number of cross-validation folds for GridSearch')
    parser.add_argument('--test-size', type=float, default=0.2,
                       help='Test set size for final evaluation')
    parser.add_argument('--random-state', type=int, default=42,
                       help='Random state for reproducibility')
    parser.add_argument('--output-prefix', type=str, default='o19s_ridge_optimized',
                       help='Prefix for output files')
    parser.add_argument('--quick-mode', action='store_true',
                       help='Quick mode with fewer alpha values for testing')
    parser.add_argument('--n-jobs', type=int, default=-1,
                       help='Number of parallel jobs for GridSearchCV (-1 for all cores)')
    parser.add_argument('--scaler-type', type=str, default='minmax',
                       choices=['minmax', 'standard'],
                       help='Type of scaler to use: minmax (0-1 range) or standard (z-score normalization)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("O19S RIDGE REGRESSION WITH AUTOMATED ALPHA OPTIMIZATION")
    print("=" * 70)
    print("\n📚 Based on model_approach_recommendation_ecommerce.md")
    print("✅ Ridge Regression for regularization")
    
    # Print scaler information
    if args.scaler_type == 'minmax':
        print("✅ MinMaxScaler for feature normalization (0-1 range)")
    else:
        print("✅ StandardScaler for feature normalization (z-score)")
    
    print("✅ GridSearchCV for optimal alpha selection")
    print("✅ Cross-validation for robust evaluation")
    
    # Parse alpha range if provided
    if args.alpha_range:
        alpha_range = [float(x.strip()) for x in args.alpha_range.split(',')]
        print(f"\n📌 Using custom alpha range: {alpha_range}")
    elif args.quick_mode:
        # Quick mode with fewer values
        alpha_range = [0.001, 0.01, 0.1, 1.0, 5.0, 10.0]
        print(f"\n⚡ Quick mode - testing limited alpha range: {alpha_range}")
    else:
        # Default comprehensive range
        alpha_range = None
        print("\n📊 Using default comprehensive alpha range")
    
    # Load training and test data separately (O19S methodology)
    print("\n🎯 Using O19S Methodology: Separate train/test files")
    train_queries_df = load_training_queries(sample_size=args.sample_size)
    
    # For test data, use a proportional sample size if specified
    test_sample_size = int(args.sample_size * 0.25) if args.sample_size else None
    test_queries_df = load_test_queries(sample_size=test_sample_size)
    
    # Initialize trainer
    print(f"\n🚀 Initializing trainer with model ID: {args.model_id}")
    print(f"📊 Using scaler type: {args.scaler_type}")
    trainer = O19SRidgeOptimizedTrainer(
        model_id=args.model_id,
        scaler_type=args.scaler_type,
        host=args.host,
        port=args.port
    )
    
    # Collect training data from query_train.csv
    print("\n📊 Collecting TRAINING data...")
    X_train, y_train = trainer.collect_training_data(train_queries_df)
    
    # Collect test data from query_test.csv
    print("\n📊 Collecting TEST data...")
    X_test, y_test = trainer.collect_training_data(test_queries_df, augment=False)
    
    print(f"\n✅ Data Collection Complete:")
    print(f"  Training set size: {len(X_train)} (from query_train.csv)")
    print(f"  Test set size: {len(X_test)} (from query_test.csv)")
    print(f"  No query overlap - true holdout test set")
    
    # Fit scaler on training data
    scaler_name = "MinMaxScaler" if args.scaler_type == 'minmax' else "StandardScaler"
    print(f"\n🔧 Fitting {scaler_name} on training data...")
    X_train_scaled = trainer.scaler.fit_transform(X_train)
    X_test_scaled = trainer.scaler.transform(X_test)
    
    # Show feature scale statistics
    print("\n📊 Feature Scaling Applied:")
    if args.scaler_type == 'minmax':
        print(f"  All features normalized to [0, 1] range")
        print(f"  Min-Max normalization: (x - min) / (max - min)")
    else:
        print(f"  All features standardized to z-scores")
        print(f"  Standardization: (x - mean) / std")
        print(f"  Mean after scaling: ~0, Std after scaling: ~1")
    print(f"  Weight feature preserved as first feature")
    
    # Optimize alpha using GridSearchCV
    best_alpha, cv_results = trainer.optimize_alpha(
        X_train_scaled, 
        y_train,
        alpha_range=alpha_range,
        cv_folds=args.cv_folds,
        n_jobs=args.n_jobs
    )
    
    # Train final model with best alpha
    print(f"\n🎯 Training Final Model with Optimal Alpha = {best_alpha}")
    final_model = Ridge(alpha=best_alpha, solver='auto', random_state=args.random_state)
    final_model.fit(X_train_scaled, y_train)
    
    # Evaluate on test set
    y_pred_train = final_model.predict(X_train_scaled)
    y_pred_test = final_model.predict(X_test_scaled)
    
    train_rmse = root_mean_squared_error(y_train, y_pred_train)
    test_rmse = root_mean_squared_error(y_test, y_pred_test)
    train_r2 = r2_score(y_train, y_pred_train)
    test_r2 = r2_score(y_test, y_pred_test)
    
    print("\n" + "=" * 60)
    print("FINAL MODEL PERFORMANCE")
    print("=" * 60)
    print(f"Training Set:")
    print(f"  RMSE: {train_rmse:.4f}")
    print(f"  R² Score: {train_r2:.4f}")
    print(f"\nTest Set:")
    print(f"  RMSE: {test_rmse:.4f}")
    print(f"  R² Score: {test_r2:.4f}")
    
    # Check for overfitting
    overfit_ratio = test_rmse / train_rmse
    print(f"\nOverfitting Check:")
    print(f"  Test/Train RMSE Ratio: {overfit_ratio:.3f}")
    if overfit_ratio > 1.2:
        print("  ⚠️ Possible overfitting detected")
    else:
        print("  ✅ No significant overfitting")
    
    # Analyze predictions
    print(f"\n📈 Prediction Analysis:")
    print(f"  Mean prediction: {np.mean(y_pred_test):.4f}")
    print(f"  Std prediction: {np.std(y_pred_test):.4f}")
    print(f"  Min prediction: {np.min(y_pred_test):.4f}")
    print(f"  Max prediction: {np.max(y_pred_test):.4f}")
    
    # Analyze feature importance
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
        'coefficient': final_model.coef_,
        'abs_coefficient': np.abs(final_model.coef_)
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
    print(f"  {'✅ Weight feature is properly utilized!' if weight_rank <= 5 else '⚠️ Weight feature has lower importance'}")
    
    # Save model
    model_file = f"{args.output_prefix}_model.pkl"
    with open(model_file, 'wb') as f:
        pickle.dump(final_model, f)
    print(f"\n💾 Model saved to {model_file}")
    
    # Save scaler
    scaler_file = f"{args.output_prefix}_scaler.pkl"
    with open(scaler_file, 'wb') as f:
        pickle.dump(trainer.scaler, f)
    print(f"💾 Scaler saved to {scaler_file}")
    
    # Save metadata
    metadata = {
        'optimal_alpha': float(best_alpha),
        'training_samples': len(X_train),
        'test_samples': len(X_test),
        'num_features': X_train.shape[1],
        'train_rmse': float(train_rmse),
        'test_rmse': float(test_rmse),
        'train_r2': float(train_r2),
        'test_r2': float(test_r2),
        'weight_coefficient': float(weight_coef),
        'weight_rank': int(weight_rank),
        'intercept': float(final_model.intercept_),
        'coefficients': final_model.coef_.tolist(),
        'feature_names': feature_names,
        'augmentation': args.augment,
        'sample_size': args.sample_size,
        'scaling': scaler_name,
        'scaler_type': args.scaler_type,
        'model_type': 'Ridge',
        'feature_range': [0, 1] if args.scaler_type == 'minmax' else None,
        'prediction_mean': float(np.mean(y_pred_test)),
        'prediction_std': float(np.std(y_pred_test)),
        'cv_folds': args.cv_folds,
        'alpha_range_tested': alpha_range if alpha_range else 'default',
        'cv_results_summary': {
            'total_alphas_tested': len(cv_results),
            'best_alpha': float(best_alpha),
            'best_cv_rmse': float(min([r['rmse_test'] for r in cv_results])),
            'top_5_alphas': [
                {
                    'alpha': float(r['param_alpha']),
                    'cv_rmse': float(r['rmse_test']),
                    'train_rmse': float(r['rmse_train'])
                }
                for r in sorted(cv_results, key=lambda x: x['rmse_test'])[:5]
            ]
        }
    }
    
    metadata_file = f"{args.output_prefix}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"📋 Metadata saved to {metadata_file}")
    
    # Save detailed CV results
    cv_results_file = f"{args.output_prefix}_cv_results.json"
    with open(cv_results_file, 'w') as f:
        json.dump(cv_results, f, indent=2)
    print(f"📊 Detailed CV results saved to {cv_results_file}")
    
    # Compare with existing models if available
    print("\n" + "=" * 60)
    print("MODEL COMPARISON")
    print("=" * 60)
    
    comparison_models = [
        ('o19s_minmax_scaled_metadata.json', 'MinMaxScaler + LinearRegression'),
        ('o19s_no_scaler.pkl_metadata.json', 'No Scaler + LinearRegression'),
        ('o19s_ridge_optimized_metadata.json', 'MinMaxScaler + Ridge (Previous)')
    ]
    
    for model_file, model_name in comparison_models:
        try:
            with open(model_file, 'r') as f:
                other_meta = json.load(f)
                print(f"\n{model_name}:")
                print(f"  Test R²: {other_meta.get('test_r2', 'N/A'):.4f}")
                print(f"  Test RMSE: {other_meta.get('test_rmse', 'N/A'):.4f}")
        except:
            pass
    
    print(f"\n{args.output_prefix} ({scaler_name} + Ridge + Optimized Alpha):")
    print(f"  Test R²: {test_r2:.4f}")
    print(f"  Test RMSE: {test_rmse:.4f}")
    print(f"  Optimal Alpha: {best_alpha}")
    print(f"  Scaler Type: {args.scaler_type}")
    
    print("\n" + "=" * 70)
    print("✅ TRAINING COMPLETE")
    print("=" * 70)
    print(f"\nModel files saved:")
    print(f"  - {model_file}")
    print(f"  - {scaler_file}")
    print(f"  - {metadata_file}")
    print(f"  - {cv_results_file}")
    print("\nNext steps:")
    print("  1. Run evaluation with the optimized model")
    print("  2. Compare performance with different alpha values")
    print("  3. Deploy to production if metrics are satisfactory")

if __name__ == "__main__":
    main()
