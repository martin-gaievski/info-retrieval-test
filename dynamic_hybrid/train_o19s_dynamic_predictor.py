#!/usr/bin/env python3
"""
O19S-Compatible Dynamic Weight Predictor Training

Implements O19S methodology for training dynamic weight prediction models:
1. Uses O19S train split queries (all by default, configurable sampling for development)
2. Calculates ratings using O19S approach and metrics
3. Trains model with O19S-compatible features
4. Saves model in pkl format for evaluation

Author: Dynamic Hybrid Search Team
Version: 1.1.0
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
from feature_extractor_corpus_aware import ESCICorpusAwareFeatureExtractor
from feature_extractor_o19s_enhanced import O19SEnhancedFeatureExtractor

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class O19SDynamicWeightTrainer:
    """Train dynamic weight predictor using O19S methodology exactly"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 random_seed: int = 42,
                 feature_set: str = "o19s"):
        """Initialize O19S trainer"""
        
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
        self.feature_set = feature_set
        
        # Initialize feature extractor based on feature set
        if feature_set == "enhanced":
            # Use O19S enhanced feature extractor (corpus + result features)
            self.feature_extractor = O19SEnhancedFeatureExtractor(
                client=self.client,
                host=host,
                port=port,
                index_name=index_name,
                model_id=model_id,
                cache_term_stats=True
            )
            feature_count = 25  # 20 corpus + 5 O19S result features
        else:
            # Use corpus-aware feature extractor
            self.feature_extractor = ESCICorpusAwareFeatureExtractor(
                client=self.client,
                index_name=index_name,
                cache_term_stats=True,
                feature_set=feature_set
            )
            feature_count = 17 if feature_set == "o19s" else 22 if feature_set == "full" else "unknown"
        
        logger.info(f"Initialized dynamic weight trainer with {feature_set} feature set ({feature_count} features) for {host}:{port}/{index_name}")
        
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
    
    def collect_o19s_training_data(self,
                                  o19s_data_path: str,
                                  ratings_file: str,
                                  weight_values: List[float] = None,
                                  sample_size: Optional[int] = None) -> pd.DataFrame:
        """
        Collect training data using O19S methodology.
        Uses ALL O19S train queries by default, or sample for development.
        
        Args:
            o19s_data_path: Path to O19S data directory
            ratings_file: Path to O19S ratings.csv
            weight_values: Weight values to test (default: 0.1-0.9)
            sample_size: Number of training queries to sample (None = use all for O19S compliance)
            
        Returns:
            Training DataFrame with O19S features and optimal weights
        """
        logger.info("Collecting training data using O19S methodology...")
        
        # Default O19S weight values (0.1 to 0.9)
        if weight_values is None:
            weight_values = [round(0.1 + i * 0.1, 1) for i in range(9)]
        
        # Load O19S data
        df_train, df_test, df_ratings = self.load_o19s_data(o19s_data_path, ratings_file)
        
        # Create reference dictionary
        reference = {query: df for query, df in df_ratings.groupby("query")}
        logger.info(f"Created reference for {len(reference)} queries")
        
        # Use ALL O19S training queries by default, or sample for development
        train_queries = df_train['query_string'].tolist()
        if sample_size and sample_size < len(train_queries):
            np.random.seed(self.random_seed)
            train_queries = np.random.choice(train_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(train_queries)} training queries for development (seed: {self.random_seed})")
            logger.warning("⚠️  Using sampling - results may not match O19S exactly!")
        else:
            logger.info(f"Using all {len(train_queries)} O19S training queries (full O19S compliance)")
        
        # Filter to queries that have ratings
        train_queries_with_ratings = [q for q in train_queries if q in reference]
        logger.info(f"Training queries with ratings: {len(train_queries_with_ratings)}")
        
        training_data = []
        
        logger.info(f"Testing {len(weight_values)} weight values: {weight_values}")
        
        # Process each training query
        for query_string in tqdm(train_queries_with_ratings, desc="Collecting training data"):
            try:
                # Extract O19S corpus-aware features (17 features)
                query_features = self.feature_extractor.extract_features(query_string)
                
                # Test each weight value to find optimal
                best_weight = None
                best_ndcg = -1
                
                for neural_weight in weight_values:
                    lexical_weight = round(1.0 - neural_weight, 2)
                    
                    # Execute hybrid search
                    search_results = self._execute_hybrid_search(query_string, lexical_weight, neural_weight)
                    
                    if not search_results.empty:
                        # Merge with ratings and calculate O19S metrics
                        df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                        
                        if not df_with_ratings.empty:
                            # Calculate NDCG using O19S metrics
                            ndcg_score = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                            
                            if ndcg_score > best_ndcg:
                                best_ndcg = ndcg_score
                                best_weight = neural_weight
                
                # Add training example if we found a good weight
                if best_weight is not None and best_ndcg > 0:
                    training_example = {
                        'query_string': query_string,
                        'optimal_neural_weight': best_weight,
                        'best_ndcg': best_ndcg,
                        **query_features
                    }
                    training_data.append(training_example)
                    
            except Exception as e:
                logger.warning(f"Failed to process query '{query_string[:50]}...': {e}")
                continue
        
        logger.info(f"Collected {len(training_data)} training examples")
        return pd.DataFrame(training_data)
    
    def _extract_query_features(self, query_string: str) -> Dict[str, float]:
        """Extract basic query features for training"""
        words = query_string.split()
        
        return {
            'query_length': len(words),
            'avg_word_length': np.mean([len(w) for w in words]) if words else 0,
            'max_word_length': max([len(w) for w in words]) if words else 0,
            'num_stopwords': sum(1 for w in words if w.lower() in ['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']),
            'has_numbers': float(any(c.isdigit() for c in query_string)),
            'has_quotes': float('"' in query_string or "'" in query_string),
            'has_special_chars': float(any(c in query_string for c in ['!', '?', '$', '%', '&'])),
            'query_specificity': len(set(words)) / len(words) if words else 0
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
    
    def train_weight_model(self, 
                          training_df: pd.DataFrame,
                          model_type: str = "random_forest",
                          feature_columns: Optional[List[str]] = None,
                          o19s_exact_mode: bool = False) -> Dict:
        """Train model to predict optimal neural weights using O19S methodology"""
        
        logger.info(f"Training {model_type} model with O19S methodology...")
        
        # Auto-detect feature columns if not specified
        if feature_columns is None:
            exclude_cols = ['query_string', 'optimal_neural_weight', 'best_ndcg']
            feature_columns = [col for col in training_df.columns 
                             if col not in exclude_cols]
        
        logger.info(f"Using {len(feature_columns)} features: {', '.join(feature_columns)}")
        
        # Prepare features and target
        X = training_df[feature_columns].values
        
        # O19S Exact Mode: Predict NDCG instead of weight
        if o19s_exact_mode:
            y = training_df['best_ndcg'].values
            logger.info("O19S Exact Mode: Predicting NDCG scores (not weights)")
        else:
            y = training_df['optimal_neural_weight'].values
            logger.info("Standard Mode: Predicting optimal weights")
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # O19S Exact Mode: Use Ridge with hyperparameter tuning
        if o19s_exact_mode and model_type in ["linear", "ridge"]:
            return self._train_o19s_exact_ridge(X_scaled, y, feature_columns, scaler)
        
        # Standard training approach
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
        elif model_type == "ridge":
            model = Ridge(alpha=1.0)
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Train model
        model.fit(X_train, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        # Clip predictions to valid range
        if not o19s_exact_mode:
            y_pred_train = np.clip(y_pred_train, 0, 1)
            y_pred_test = np.clip(y_pred_test, 0, 1)
        
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
        
        return {
            'model': model,
            'scaler': scaler,
            'model_type': model_type,
            'feature_columns': feature_columns,
            'o19s_exact_mode': o19s_exact_mode,
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
                'model_id': self.model_id
            }
        }
    
    def _train_o19s_exact_ridge(self, X: np.ndarray, y: np.ndarray, 
                               feature_columns: List[str], scaler: StandardScaler) -> Dict:
        """Train Ridge regression with O19S exact methodology"""
        
        logger.info("Training with O19S exact Ridge methodology...")
        
        # O19S cross-validation strategy
        cv = ShuffleSplit(n_splits=5, test_size=0.2, random_state=0)
        
        # O19S alpha values for hyperparameter tuning
        alpha_vals = [0.001, 0.1, 0.2, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0]
        
        # Find best alpha using RMSE (O19S approach)
        # Note: squared=False parameter not available in older sklearn versions
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
        
        # Calculate final metrics using ShuffleSplit
        train_indices = []
        test_indices = []
        for train_idx, test_idx in cv.split(X):
            train_indices.extend(train_idx)
            test_indices.extend(test_idx)
        
        # Use last split for final evaluation
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
        
        logger.info(f"O19S Ridge Training RMSE: {train_rmse:.4f}, Test RMSE: {test_rmse:.4f}")
        logger.info(f"O19S Ridge Training R²: {train_r2:.4f}, Test R²: {test_r2:.4f}")
        
        return {
            'model': final_model,
            'scaler': scaler,
            'model_type': 'ridge_o19s_exact',
            'feature_columns': feature_columns,
            'o19s_exact_mode': True,
            'best_alpha': best_alpha,
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
                'alpha_values_tested': alpha_vals
            }
        }
    
    def save_model(self, model_dict: Dict, output_path: str):
        """Save trained model with O19S configuration"""
        with open(output_path, 'wb') as f:
            pickle.dump(model_dict, f)
        logger.info(f"O19S dynamic weight model saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Train O19S-compatible dynamic weight predictor (default: uses ALL O19S train queries)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full O19S compliance (recommended)
  python3 %(prog)s --host your-cluster.com --port 80 --model-id MODEL_ID --output model.pkl
  
  # Fast development (sampling)
  python3 %(prog)s --host your-cluster.com --port 80 --model-id MODEL_ID --sample-size 100 --output model.pkl
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
                       help='Number of training queries to sample (default: None = use all O19S train queries)')
    parser.add_argument('--model-type', choices=['random_forest', 'gradient_boosting', 'linear', 'ridge'],
                       default='linear', help='Model type (default: linear regression for O19S compatibility)')
    parser.add_argument('--weight-values', nargs='+', type=float, default=None,
                       help='Neural weight values to test (default: 0.1-0.9)')
    parser.add_argument('-o', '--output', default='o19s_dynamic_weight_predictor.pkl',
                       help='Output model file')
    parser.add_argument('--training-data-file', default=None,
                       help='Save training data CSV')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    parser.add_argument('--feature-set', choices=['o19s', 'full', 'enhanced'], default='o19s',
                       help='Feature set to use: o19s (17 features), full (22 features), enhanced (25 features: corpus + O19S result features), default: o19s')
    parser.add_argument('--o19s-exact-mode', action='store_true',
                       help='Use O19S exact methodology: predict NDCG (not weights), Ridge with alpha tuning, ShuffleSplit CV')
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = O19SDynamicWeightTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        random_seed=args.seed,
        feature_set=args.feature_set
    )
    
    # Collect training data
    training_df = trainer.collect_o19s_training_data(
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
    logger.info("\n=== O19S Training Data Analysis ===")
    print(f"Total training examples: {len(training_df)}")
    print(f"Optimal weight distribution:")
    print(f"  Mean: {training_df['optimal_neural_weight'].mean():.3f}")
    print(f"  Std: {training_df['optimal_neural_weight'].std():.3f}")
    print(f"  Range: [{training_df['optimal_neural_weight'].min():.1f}, {training_df['optimal_neural_weight'].max():.1f}]")
    print(f"Best NDCG distribution:")
    print(f"  Mean: {training_df['best_ndcg'].mean():.4f}")
    print(f"  Std: {training_df['best_ndcg'].std():.4f}")
    
    # Weight distribution
    weight_dist = training_df['optimal_neural_weight'].value_counts().sort_index()
    print("Weight distribution:")
    for weight, count in weight_dist.items():
        print(f"  {weight}: {count} queries")
    
    # Train model
    model_dict = trainer.train_weight_model(training_df, model_type=args.model_type, o19s_exact_mode=args.o19s_exact_mode)
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== O19S Model Training Summary ===")
    print(f"Model type: {model_dict['model_type']}")
    print(f"Training examples: {len(training_df)}")
    print(f"Features used: {len(model_dict['feature_columns'])}")
    print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
    print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    
    # Handle different metric keys for O19S exact mode
    if 'cv_rmse_mean' in model_dict['metrics']:
        print(f"Cross-validation RMSE: {model_dict['metrics']['cv_rmse_mean']:.4f}")
        print(f"Best alpha: {model_dict['metrics']['best_alpha']}")
    elif 'cv_r2_mean' in model_dict['metrics']:
        print(f"Cross-validation R²: {model_dict['metrics']['cv_r2_mean']:.4f}")
    
    print(f"Model saved to: {args.output}")


if __name__ == "__main__":
    main()
