#!/usr/bin/env python3
"""
O19S True Methodology Training Script

Implements the actual O19S methodology where:
1. Weight is an INPUT feature (not target)
2. Model predicts NDCG (not weight)
3. Training data includes multiple weight-NDCG pairs per query
4. Evaluation tests all weights and selects the one with highest predicted NDCG

Author: Dynamic Hybrid Search Team
Version: 2.0.0
"""

import os
import sys
import json
import argparse
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import pickle
from tqdm import tqdm
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.model_selection import train_test_split, cross_val_score, ShuffleSplit
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error, make_scorer
from sklearn.preprocessing import StandardScaler
from pathlib import Path
import requests

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, '..'))

# O19S imports
from dynamic_hybrid.utils import metrics
from opensearchpy import OpenSearch

# BEIR imports
from beir import LoggingHandler

# Feature extractors
from feature_extractor_o19s_enhanced import O19SEnhancedFeatureExtractor

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class O19STrueMethodologyTrainer:
    """Train NDCG predictor using O19S true methodology"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 random_seed: int = 42):
        """Initialize O19S true methodology trainer"""
        
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
        self.random_seed = random_seed
        
        # Initialize O19S enhanced feature extractor (corpus + result features)
        self.feature_extractor = O19SEnhancedFeatureExtractor(
            client=self.client,
            host=host,
            port=port,
            index_name=index_name,
            model_id=model_id,
            cache_term_stats=True
        )
        
        logger.info(f"Initialized O19S true methodology trainer for {host}:{port}/{index_name}")
        
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
    
    def collect_o19s_true_training_data(self,
                                       o19s_data_path: str,
                                       ratings_file: str,
                                       weight_values: List[float] = None,
                                       sample_size: Optional[int] = None) -> pd.DataFrame:
        """
        Collect training data using O19S TRUE methodology.
        Creates multiple training examples per query (one for each weight).
        
        Args:
            o19s_data_path: Path to O19S data directory
            ratings_file: Path to O19S ratings.csv
            weight_values: Weight values to test (default: 0.1-0.9)
            sample_size: Number of training queries to sample (None = use all)
            
        Returns:
            Training DataFrame with weight as input feature and NDCG as target
        """
        logger.info("Collecting training data using O19S TRUE methodology...")
        logger.info("⚠️  Weight is INPUT feature, NDCG is target variable")
        
        # Default O19S weight values (0.1 to 0.9)
        if weight_values is None:
            weight_values = [round(0.1 + i * 0.1, 1) for i in range(9)]
        
        # Load O19S data
        df_train, df_test, df_ratings = self.load_o19s_data(o19s_data_path, ratings_file)
        
        # Create reference dictionary
        reference = {query: df for query, df in df_ratings.groupby("query")}
        logger.info(f"Created reference for {len(reference)} queries")
        
        # Use O19S training queries
        train_queries = df_train['query_string'].tolist()
        if sample_size and sample_size < len(train_queries):
            np.random.seed(self.random_seed)
            train_queries = np.random.choice(train_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(train_queries)} training queries for development")
        else:
            logger.info(f"Using all {len(train_queries)} O19S training queries")
        
        # Filter to queries that have ratings
        train_queries_with_ratings = [q for q in train_queries if q in reference]
        logger.info(f"Training queries with ratings: {len(train_queries_with_ratings)}")
        
        training_data = []
        
        logger.info(f"Creating {len(weight_values)} training examples per query")
        
        # Process each training query
        for query_string in tqdm(train_queries_with_ratings, desc="Collecting training data"):
            try:
                # Extract O19S features WITHOUT weight (25 features)
                base_features = self.feature_extractor.extract_features(query_string)
                
                # Test EACH weight value and create training example
                for neural_weight in weight_values:
                    lexical_weight = round(1.0 - neural_weight, 2)
                    
                    # Execute hybrid search with this weight
                    search_results = self._execute_hybrid_search(query_string, lexical_weight, neural_weight)
                    
                    if not search_results.empty:
                        # Merge with ratings and calculate NDCG
                        df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                        
                        if not df_with_ratings.empty:
                            # Calculate NDCG (this is our target)
                            ndcg_score = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                            
                            # Create training example with weight as INPUT feature
                            training_example = {
                                'query_string': query_string,
                                'neural_weight': neural_weight,  # INPUT feature
                                'ndcg': ndcg_score,              # TARGET variable
                                **base_features                  # 25 other features
                            }
                            training_data.append(training_example)
                    
            except Exception as e:
                logger.warning(f"Failed to process query '{query_string[:50]}...': {e}")
                continue
        
        logger.info(f"Collected {len(training_data)} training examples")
        logger.info(f"Average examples per query: {len(training_data) / len(train_queries_with_ratings):.1f}")
        
        return pd.DataFrame(training_data)
    
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
                                    "k": 10
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "O19S training hybrid search",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": "min_max"},
                            "combination": {
                                "technique": "arithmetic_mean",
                                "parameters": {"weights": [lexical_weight, neural_weight]}
                            }
                        }
                    }
                ]
            },
            "size": 10
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            
            # Convert to DataFrame format for O19S metrics
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
        """Merge search results with reference ratings for O19S metrics compatibility"""
        
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
    
    def train_ndcg_predictor(self, 
                            training_df: pd.DataFrame,
                            model_type: str = "ridge") -> Dict:
        """Train model to predict NDCG with weight as input feature"""
        
        logger.info(f"Training {model_type} model with O19S TRUE methodology...")
        logger.info("Target: NDCG, Input features include neural_weight")
        
        # Define feature columns (including neural_weight as input)
        exclude_cols = ['query_string', 'ndcg']  # ndcg is target, not feature
        feature_columns = [col for col in training_df.columns if col not in exclude_cols]
        
        # Verify neural_weight is in features
        if 'neural_weight' not in feature_columns:
            raise ValueError("neural_weight must be in feature columns!")
        
        logger.info(f"Using {len(feature_columns)} features: {', '.join(feature_columns[:5])}...")
        logger.info(f"Feature list includes: neural_weight (position {feature_columns.index('neural_weight')})")
        
        # Prepare features and target
        X = training_df[feature_columns].values
        y = training_df['ndcg'].values  # Target is NDCG
        
        logger.info(f"Training data shape: {X.shape}")
        logger.info(f"Target (NDCG) range: [{y.min():.4f}, {y.max():.4f}]")
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # O19S uses Ridge with hyperparameter tuning
        if model_type == "ridge":
            return self._train_o19s_ridge(X_scaled, y, feature_columns, scaler)
        
        # Alternative models for comparison
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=self.random_seed
        )
        
        # Select and train model
        if model_type == "random_forest":
            model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=self.random_seed,
                n_jobs=-1
            )
        elif model_type == "gradient_boosting":
            model = GradientBoostingRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=5,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=self.random_seed
            )
        elif model_type == "linear":
            model = LinearRegression()
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Train model
        model.fit(X_train, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        # Calculate metrics
        train_mse = mean_squared_error(y_train, y_pred_train)
        test_mse = mean_squared_error(y_test, y_pred_test)
        train_mae = mean_absolute_error(y_train, y_pred_train)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')
        
        logger.info(f"Training R²: {train_r2:.4f}, Test R²: {test_r2:.4f}")
        logger.info(f"Cross-validation R²: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
        
        # Feature importance
        feature_importance = None
        if hasattr(model, 'feature_importances_'):
            feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            logger.info("Top 10 most important features:")
            print(feature_importance.head(10))
            
            # Check weight feature importance
            weight_importance = feature_importance[feature_importance['feature'] == 'neural_weight']
            if not weight_importance.empty:
                logger.info(f"Neural weight importance rank: {weight_importance.index[0] + 1}/{len(feature_columns)}")
        
        return {
            'model': model,
            'scaler': scaler,
            'model_type': model_type,
            'feature_columns': feature_columns,
            'o19s_true_methodology': True,
            'predicts': 'ndcg',  # Model predicts NDCG
            'metrics': {
                'train_mse': train_mse,
                'test_mse': test_mse,
                'train_mae': train_mae,
                'test_mae': test_mae,
                'train_r2': train_r2,
                'test_r2': test_r2,
                'cv_r2_mean': cv_scores.mean(),
                'cv_r2_std': cv_scores.std()
            },
            'feature_importance': feature_importance,
            'training_config': {
                'random_seed': self.random_seed,
                'model_id': self.model_id,
                'methodology': 'o19s_true'
            }
        }
    
    def _train_o19s_ridge(self, X: np.ndarray, y: np.ndarray, 
                         feature_columns: List[str], scaler: StandardScaler) -> Dict:
        """Train Ridge regression with O19S methodology"""
        
        logger.info("Training Ridge with O19S methodology (predicting NDCG)...")
        
        # O19S cross-validation strategy
        cv = ShuffleSplit(n_splits=5, test_size=0.2, random_state=0)
        
        # O19S alpha values for hyperparameter tuning
        alpha_vals = [0.001, 0.1, 0.2, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0]
        
        # Find best alpha using RMSE
        def rmse_score(y_true, y_pred):
            return np.sqrt(mean_squared_error(y_true, y_pred))
        
        rmse_scorer = make_scorer(rmse_score, greater_is_better=False)
        
        best_alpha = None
        best_rmse = float('inf')
        
        logger.info("Tuning Ridge alpha parameter...")
        for alpha in alpha_vals:
            model = Ridge(alpha=alpha)
            rmse_scores = cross_val_score(model, X, y, cv=cv, scoring=rmse_scorer)
            mean_rmse = -np.mean(rmse_scores)  # Convert back to positive RMSE
            
            logger.info(f"Alpha {alpha}: RMSE {mean_rmse:.4f}")
            
            if mean_rmse < best_rmse:
                best_rmse = mean_rmse
                best_alpha = alpha
        
        logger.info(f"Best alpha: {best_alpha} (RMSE: {best_rmse:.4f})")
        
        # Train final model with best alpha
        final_model = Ridge(alpha=best_alpha)
        final_model.fit(X, y)
        
        # Calculate final metrics using last split
        train_idx, test_idx = list(cv.split(X))[-1]
        X_train_final, X_test_final = X[train_idx], X[test_idx]
        y_train_final, y_test_final = y[train_idx], y[test_idx]
        
        # Train on final split
        final_model.fit(X_train_final, y_train_final)
        
        # Predictions
        y_pred_train = final_model.predict(X_train_final)
        y_pred_test = final_model.predict(X_test_final)
        
        # Calculate metrics
        train_mse = mean_squared_error(y_train_final, y_pred_train)
        test_mse = mean_squared_error(y_test_final, y_pred_test)
        train_rmse = np.sqrt(train_mse)
        test_rmse = np.sqrt(test_mse)
        train_r2 = r2_score(y_train_final, y_pred_train)
        test_r2 = r2_score(y_test_final, y_pred_test)
        
        logger.info(f"Ridge Training RMSE: {train_rmse:.4f}, Test RMSE: {test_rmse:.4f}")
        logger.info(f"Ridge Training R²: {train_r2:.4f}, Test R²: {test_r2:.4f}")
        
        # Analyze weight coefficient
        weight_idx = feature_columns.index('neural_weight')
        weight_coef = final_model.coef_[weight_idx]
        logger.info(f"Neural weight coefficient: {weight_coef:.4f}")
        
        return {
            'model': final_model,
            'scaler': scaler,
            'model_type': 'ridge',
            'feature_columns': feature_columns,
            'o19s_true_methodology': True,
            'predicts': 'ndcg',
            'best_alpha': best_alpha,
            'weight_coefficient': weight_coef,
            'metrics': {
                'train_mse': train_mse,
                'test_mse': test_mse,
                'train_rmse': train_rmse,
                'test_rmse': test_rmse,
                'train_r2': train_r2,
                'test_r2': test_r2,
                'cv_rmse_mean': best_rmse,
                'best_alpha': best_alpha
            },
            'feature_importance': None,  # Ridge doesn't have feature_importances_
            'training_config': {
                'random_seed': self.random_seed,
                'model_id': self.model_id,
                'cv_strategy': 'ShuffleSplit',
                'alpha_values_tested': alpha_vals,
                'methodology': 'o19s_true'
            }
        }
    
    def save_model(self, model_dict: Dict, output_path: str):
        """Save trained model with O19S true methodology configuration"""
        with open(output_path, 'wb') as f:
            pickle.dump(model_dict, f)
        logger.info(f"O19S true methodology model saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Train O19S TRUE methodology NDCG predictor (weight as input feature)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This implements the ACTUAL O19S methodology where:
- Weight is an INPUT feature (not target)
- Model predicts NDCG (not weight)
- Training includes multiple examples per query (one per weight)
- Evaluation tests all weights and selects highest predicted NDCG

Examples:
  # Train with Ridge (O19S default)
  python3 %(prog)s --host your-cluster.com --port 80 --model-id MODEL_ID --output model.pkl
  
  # Fast development with sampling
  python3 %(prog)s --host your-cluster.com --port 80 --model-id MODEL_ID --sample-size 100 --output model.pkl
  
  # Try Random Forest
  python3 %(prog)s --host your-cluster.com --port 80 --model-id MODEL_ID --model-type random_forest --output model.pkl
        """
    )
    
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', default='esci-products', help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--o19s-data', default='dynamic_hybrid/data',
                       help='Path to O19S data directory')
    parser.add_argument('--ratings-file', default='dynamic_hybrid/data/ratings.csv',
                       help='Path to O19S ratings.csv file')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of training queries to sample (default: None = use all)')
    parser.add_argument('--model-type', choices=['ridge', 'random_forest', 'gradient_boosting', 'linear'],
                       default='ridge', help='Model type (default: ridge for O19S compatibility)')
    parser.add_argument('--weight-values', nargs='+', type=float, default=None,
                       help='Neural weight values to test (default: 0.1-0.9)')
    parser.add_argument('-o', '--output', default='o19s_true_methodology_model.pkl',
                       help='Output model file')
    parser.add_argument('--training-data-file', default=None,
                       help='Save training data CSV')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = O19STrueMethodologyTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        random_seed=args.seed
    )
    
    # Collect training data with O19S TRUE methodology
    training_df = trainer.collect_o19s_true_training_data(
        o19s_data_path=args.o19s_data,
        ratings_file=args.ratings_file,
        weight_values=args.weight_values,
        sample_size=args.sample_size
    )
    
    if len(training_df) == 0:
        logger.error("No training data collected - check O19S data and ratings")
        sys.exit(1)
    
    # Save training data if requested
    if args.training_data_file:
        training_df.to_csv(args.training_data_file, index=False)
        logger.info(f"Training data saved to {args.training_data_file}")
    
    # Analyze training data
    logger.info("\n=== O19S TRUE Methodology Training Data Analysis ===")
    print(f"Total training examples: {len(training_df)}")
    print(f"Unique queries: {training_df['query_string'].nunique()}")
    print(f"Examples per query: {len(training_df) / training_df['query_string'].nunique():.1f}")
    print(f"\nNDCG distribution (target variable):")
    print(f"  Mean: {training_df['ndcg'].mean():.4f}")
    print(f"  Std: {training_df['ndcg'].std():.4f}")
    print(f"  Range: [{training_df['ndcg'].min():.4f}, {training_df['ndcg'].max():.4f}]")
    
    # Weight distribution in training data
    print(f"\nNeural weight distribution (input feature):")
    weight_counts = training_df['neural_weight'].value_counts().sort_index()
    for weight, count in weight_counts.items():
        print(f"  {weight}: {count} examples")
    
    # Train NDCG predictor
    model_dict = trainer.train_ndcg_predictor(training_df, model_type=args.model_type)
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== O19S TRUE Methodology Training Summary ===")
    print(f"Model type: {model_dict['model_type']}")
    print(f"Predicts: {model_dict['predicts'].upper()}")
    print(f"Training examples: {len(training_df)}")
    print(f"Features used: {len(model_dict['feature_columns'])}")
    print(f"Weight is feature #: {model_dict['feature_columns'].index('neural_weight') + 1}")
    print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
    print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    
    if 'weight_coefficient' in model_dict:
        print(f"Weight coefficient: {model_dict['weight_coefficient']:.4f}")
    
    if 'cv_rmse_mean' in model_dict['metrics']:
        print(f"Cross-validation RMSE: {model_dict['metrics']['cv_rmse_mean']:.4f}")
    
    if 'best_alpha' in model_dict:
        print(f"Best Ridge alpha: {model_dict['best_alpha']}")
    
    print(f"\nModel saved to: {args.output}")
    print("\n⚠️  Remember: This model predicts NDCG, not weight!")
    print("Use evaluate_o19s_true_methodology.py for proper evaluation")


if __name__ == "__main__":
    main()
