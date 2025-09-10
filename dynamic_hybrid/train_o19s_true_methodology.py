#!/usr/bin/env python3
"""
O19S True Methodology Training Script

Implements the ACTUAL O19S methodology where:
1. Weight is used as an INPUT feature (26th feature), not as target
2. Target is NDCG score, not weight
3. Training data includes all weight-NDCG combinations (9x larger dataset)
4. During evaluation, test all weights and select the one with highest predicted NDCG

This matches the O19S documentation: "The neural search weight is one of the input features... 
The search weight that produces the highest NDCG prediction is the best search weight."

Author: Dynamic Hybrid Search Team
Version: 2.0.0 - True O19S Implementation
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
from sklearn.linear_model import Ridge
from sklearn.model_selection import ShuffleSplit, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, make_scorer
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
    """Train NDCG predictor using O19S TRUE methodology with weight as input feature"""
    
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
        
        # Initialize O19S enhanced feature extractor (25 features: 20 corpus + 5 result)
        self.feature_extractor = O19SEnhancedFeatureExtractor(
            client=self.client,
            host=host,
            port=port,
            index_name=index_name,
            model_id=model_id,
            cache_term_stats=True
        )
        
        logger.info(f"Initialized O19S TRUE methodology trainer (26 features: 25 base + weight as input)")
        logger.info(f"Target: NDCG score (not weight)")
        
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
    
    def collect_training_data_with_weight_as_input(self,
                                                  o19s_data_path: str,
                                                  ratings_file: str,
                                                  weight_values: List[float] = None,
                                                  sample_size: Optional[int] = None) -> pd.DataFrame:
        """
        Collect training data using O19S TRUE methodology.
        Weight is an INPUT feature, NDCG is the target.
        Creates multiple training examples per query (one for each weight).
        
        Args:
            o19s_data_path: Path to O19S data directory
            ratings_file: Path to O19S ratings.csv
            weight_values: Weight values to test (default: 0.1-0.9)
            sample_size: Number of training queries to sample (None = use all)
            
        Returns:
            Training DataFrame with weight as input feature and NDCG as target
        """
        logger.info("Collecting training data using O19S TRUE methodology (weight as input)...")
        
        # Default O19S weight values (0.1 to 0.9)
        if weight_values is None:
            weight_values = [round(0.1 + i * 0.1, 1) for i in range(9)]
        
        # Load O19S data
        df_train, df_test, df_ratings = self.load_o19s_data(o19s_data_path, ratings_file)
        
        # Create reference dictionary
        reference = {query: df for query, df in df_ratings.groupby("query")}
        logger.info(f"Created reference for {len(reference)} queries")
        
        # Use training queries
        train_queries = df_train['query_string'].tolist()
        if sample_size and sample_size < len(train_queries):
            np.random.seed(self.random_seed)
            train_queries = np.random.choice(train_queries, size=sample_size, replace=False).tolist()
            logger.info(f"Sampled {len(train_queries)} training queries")
        else:
            logger.info(f"Using all {len(train_queries)} O19S training queries")
        
        # Filter to queries that have ratings
        train_queries_with_ratings = [q for q in train_queries if q in reference]
        logger.info(f"Training queries with ratings: {len(train_queries_with_ratings)}")
        
        training_data = []
        
        logger.info(f"Creating {len(weight_values)} training examples per query (weight as input)")
        
        # Process each training query
        for query_string in tqdm(train_queries_with_ratings, desc="Collecting training data"):
            try:
                # Extract O19S features (25 features: 20 corpus + 5 result)
                base_features = self.feature_extractor.extract_features(query_string)
                
                # Create training example for EACH weight value
                for neural_weight in weight_values:
                    lexical_weight = round(1.0 - neural_weight, 2)
                    
                    # Execute hybrid search with this weight
                    search_results = self._execute_hybrid_search(query_string, lexical_weight, neural_weight)
                    
                    if not search_results.empty:
                        # Merge with ratings and calculate NDCG
                        df_with_ratings = self._merge_results_with_reference(search_results, reference[query_string])
                        
                        if not df_with_ratings.empty:
                            # Calculate NDCG (this is our TARGET)
                            ndcg_score = metrics.ndcg_at_10(df_with_ratings, reference=reference[query_string])
                            
                            # Create training example with weight as INPUT feature
                            training_example = {
                                'query_string': query_string,
                                'neural_weight_input': neural_weight,  # Weight as INPUT feature
                                'ndcg_target': ndcg_score,  # NDCG as TARGET
                                **base_features  # 25 base features
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
                "description": "O19S true methodology training",
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
            
            # Convert to DataFrame format
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
        
        return merged[['position', 'rating', 'product_id', 'relevance']]
    
    def train_ndcg_predictor(self, training_df: pd.DataFrame) -> Dict:
        """
        Train Ridge regression to predict NDCG with weight as input feature.
        Uses O19S exact methodology with hyperparameter tuning.
        """
        
        logger.info("Training NDCG predictor with O19S TRUE methodology...")
        logger.info(f"Training examples: {len(training_df)}")
        
        # Prepare features (including weight as input) and target (NDCG)
        exclude_cols = ['query_string', 'ndcg_target']
        feature_columns = [col for col in training_df.columns if col not in exclude_cols]
        
        # Ensure weight is included as a feature
        if 'neural_weight_input' not in feature_columns:
            raise ValueError("Weight must be included as input feature!")
        
        logger.info(f"Using {len(feature_columns)} features (including weight): {', '.join(feature_columns[:5])}...")
        
        X = training_df[feature_columns].values
        y = training_df['ndcg_target'].values  # Target is NDCG
        
        logger.info(f"Target (NDCG) statistics: mean={y.mean():.4f}, std={y.std():.4f}, range=[{y.min():.4f}, {y.max():.4f}]")
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # O19S cross-validation strategy
        cv = ShuffleSplit(n_splits=5, test_size=0.2, random_state=0)
        
        # O19S alpha values for hyperparameter tuning
        alpha_vals = [0.001, 0.01, 0.1, 0.2, 0.5, 1.0, 1.5, 2.0, 5.0, 10.0]
        
        # Find best alpha using RMSE
        def rmse_score(y_true, y_pred):
            return np.sqrt(mean_squared_error(y_true, y_pred))
        
        rmse_scorer = make_scorer(rmse_score, greater_is_better=False)
        
        best_alpha = None
        best_rmse = float('inf')
        
        logger.info("Tuning Ridge alpha parameter for NDCG prediction...")
        for alpha in alpha_vals:
            model = Ridge(alpha=alpha)
            rmse_scores = cross_val_score(model, X_scaled, y, cv=cv, scoring=rmse_scorer)
            mean_rmse = -np.mean(rmse_scores)  # Convert back to positive RMSE
            
            logger.info(f"Alpha {alpha}: RMSE {mean_rmse:.4f}")
            
            if mean_rmse < best_rmse:
                best_rmse = mean_rmse
                best_alpha = alpha
        
        logger.info(f"Best alpha: {best_alpha} (RMSE: {best_rmse:.4f})")
        
        # Train final model with best alpha
        final_model = Ridge(alpha=best_alpha)
        final_model.fit(X_scaled, y)
        
        # Calculate final metrics
        train_idx, test_idx = list(cv.split(X_scaled))[-1]
        X_train_final, X_test_final = X_scaled[train_idx], X_scaled[test_idx]
        y_train_final, y_test_final = y[train_idx], y[test_idx]
        
        # Train on final split
        final_model.fit(X_train_final, y_train_final)
        
        # Predictions
        y_pred_train = final_model.predict(X_train_final)
        y_pred_test = final_model.predict(X_test_final)
        
        # Calculate metrics
        train_rmse = np.sqrt(mean_squared_error(y_train_final, y_pred_train))
        test_rmse = np.sqrt(mean_squared_error(y_test_final, y_pred_test))
        train_r2 = r2_score(y_train_final, y_pred_train)
        test_r2 = r2_score(y_test_final, y_pred_test)
        
        logger.info(f"NDCG Prediction - Training RMSE: {train_rmse:.4f}, Test RMSE: {test_rmse:.4f}")
        logger.info(f"NDCG Prediction - Training R²: {train_r2:.4f}, Test R²: {test_r2:.4f}")
        
        # Analyze feature importance (coefficients)
        feature_importance = pd.DataFrame({
            'feature': feature_columns,
            'coefficient': final_model.coef_
        }).sort_values('coefficient', key=abs, ascending=False)
        
        logger.info("\nTop 10 most important features for NDCG prediction:")
        print(feature_importance.head(10))
        
        # Check weight feature importance
        weight_importance = feature_importance[feature_importance['feature'] == 'neural_weight_input']
        if not weight_importance.empty:
            weight_coef = weight_importance.iloc[0]['coefficient']
            weight_rank = (feature_importance['feature'] == 'neural_weight_input').idxmax() + 1
            logger.info(f"\nWeight feature importance: coefficient={weight_coef:.4f}, rank={weight_rank}/{len(feature_columns)}")
        
        return {
            'model': final_model,
            'scaler': scaler,
            'model_type': 'ridge_o19s_true_methodology',
            'feature_columns': feature_columns,
            'target': 'ndcg',  # Important: target is NDCG, not weight
            'methodology': 'weight_as_input',  # Weight is an input feature
            'best_alpha': best_alpha,
            'metrics': {
                'train_rmse': train_rmse,
                'test_rmse': test_rmse,
                'train_r2': train_r2,
                'test_r2': test_r2,
                'cv_rmse_mean': best_rmse,
                'best_alpha': best_alpha
            },
            'feature_importance': feature_importance,
            'training_config': {
                'random_seed': self.random_seed,
                'model_id': self.model_id,
                'cv_strategy': 'ShuffleSplit',
                'alpha_values_tested': alpha_vals,
                'num_features': len(feature_columns),
                'training_examples': len(training_df)
            }
        }
    
    def save_model(self, model_dict: Dict, output_path: str):
        """Save trained NDCG predictor model"""
        with open(output_path, 'wb') as f:
            pickle.dump(model_dict, f)
        logger.info(f"O19S TRUE methodology model saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Train O19S TRUE methodology NDCG predictor (weight as input feature)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This implements the TRUE O19S methodology where:
- Weight is an INPUT feature (not the target)
- Target is NDCG score
- Training data includes all weight-NDCG combinations
- During evaluation, test all weights and select the one with highest predicted NDCG

Examples:
  # Full training
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
                       help='Number of training queries to sample (default: None = use all)')
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
    
    # Collect training data with weight as input feature
    training_df = trainer.collect_training_data_with_weight_as_input(
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
    print(f"\nWeight distribution (as input):")
    print(training_df['neural_weight_input'].value_counts().sort_index())
    print(f"\nNDCG distribution (target):")
    print(f"  Mean: {training_df['ndcg_target'].mean():.4f}")
    print(f"  Std: {training_df['ndcg_target'].std():.4f}")
    print(f"  Range: [{training_df['ndcg_target'].min():.4f}, {training_df['ndcg_target'].max():.4f}]")
    
    # Train NDCG predictor model
    model_dict = trainer.train_ndcg_predictor(training_df)
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== O19S TRUE Methodology Training Summary ===")
    print(f"Model type: {model_dict['model_type']}")
    print(f"Target: {model_dict['target']} (not weight!)")
    print(f"Methodology: {model_dict['methodology']}")
    print(f"Training examples: {len(training_df)}")
    print(f"Features used: {len(model_dict['feature_columns'])} (including weight as input)")
    print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
    print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    print(f"Cross-validation RMSE: {model_dict['metrics']['cv_rmse_mean']:.4f}")
    print(f"Best alpha: {model_dict['metrics']['best_alpha']}")
    print(f"Model saved to: {args.output}")


if __name__ == "__main__":
    main()
