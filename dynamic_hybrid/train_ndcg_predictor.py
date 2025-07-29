"""
Train a model to predict NDCG values for different weight combinations.
Instead of predicting weights directly,
predict NDCG for each weight combination and select the best.
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
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler

# Add dynamic_hybrid to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# BEIR imports
from beir import util, LoggingHandler
from beir.datasets.data_loader import GenericDataLoader
from beir.retrieval.evaluation import EvaluateRetrieval

# Add the beir path to import 
beir_path = os.path.join(os.path.dirname(__file__), '..', 'beir')
sys.path.insert(0, beir_path)

try:
    from beir.datasets.data_loader_esci import DataLoader as ESCIDataLoader
except ImportError:
    from datasets.data_loader_esci import DataLoader as ESCIDataLoader

# OpenSearch imports
from opensearchpy import OpenSearch

# Local imports
from feature_extractor import DomainAwareFeatureExtractor, get_domain_for_dataset
try:
    from feature_extractor_esci_enhanced import ESCIEnhancedFeatureExtractor
except ImportError:
    ESCIEnhancedFeatureExtractor = None

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class NDCGPredictorTrainer:
    """Train a model to predict NDCG for different weight combinations"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "beir-index",
                 model_id: str = None):
        """Initialize trainer"""
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_compress=True,
            http_auth=None,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        
        self.index_name = index_name
        self.model_id = model_id
        self.training_data = []
        
    def collect_training_data(self,
                            dataset_name: str,
                            data_path: str,
                            weight_values: List[float] = None,
                            sample_size: Optional[int] = None,
                            include_result_features: bool = True) -> pd.DataFrame:
        """
        Collect training data by evaluating queries with different weights.
        For each query and weight combination, we'll predict the NDCG.
        
        Args:
            dataset_name: Name of the BEIR dataset
            data_path: Path to dataset files
            weight_values: List of neural weight values to test (default: 0.0 to 1.0 by 0.1)
            sample_size: Number of queries to sample (None for all)
            include_result_features: Whether to include search result features
            
        Returns:
            DataFrame with features and NDCG values
        """
        logger.info(f"Loading dataset: {dataset_name}")
        
        # Default weight values
        if weight_values is None:
            weight_values = [round(w/10, 1) for w in range(11)]  # 0.0, 0.1, ..., 1.0
        
        # Load dataset
        if dataset_name.lower() == "esci":
            loader = ESCIDataLoader(
                data_folder=data_path,
                language="us",
                small_version=True
            )
            corpus, queries, qrels = loader.load(split="test")
        else:
            corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split="test")
        
        # Sample queries if requested
        if sample_size and sample_size < len(queries):
            query_ids = list(queries.keys())
            np.random.shuffle(query_ids)
            query_ids = query_ids[:sample_size]
            queries = {qid: queries[qid] for qid in query_ids}
            logger.info(f"Sampled {sample_size} queries for training")
        
        # Initialize feature extractor
        if dataset_name.lower() == "esci" and ESCIEnhancedFeatureExtractor:
            feature_extractor = ESCIEnhancedFeatureExtractor()
            logger.info("Using ESCIEnhancedFeatureExtractor")
        else:
            domain = get_domain_for_dataset(dataset_name)
            feature_extractor = DomainAwareFeatureExtractor(domain)
            
        logger.info(f"Testing {len(weight_values)} weight values on {len(queries)} queries")
        logger.info(f"Total combinations: {len(weight_values) * len(queries)}")
        
        # For each query and weight combination
        for query_id, query_text in tqdm(queries.items(), desc="Processing queries"):
            # Extract query features
            query_features = feature_extractor.extract_features(query_text)
            
            # Test each weight value
            for neural_weight in weight_values:
                lexical_weight = round(1.0 - neural_weight, 1)
                
                # Run search
                results = self._run_hybrid_search(
                    query_text, 
                    lexical_weight, 
                    neural_weight,
                    top_k=10,
                    dataset_name=dataset_name
                )
                
                # Calculate NDCG
                ndcg_score = 0.0
                if query_id in qrels and results:
                    relevant_docs = qrels[query_id]
                    ndcg_score = self._calculate_ndcg_at_k(results, relevant_docs, k=10)
                
                # Prepare training example
                training_example = {
                    'query_id': query_id,
                    'query_text': query_text,
                    'neural_weight': neural_weight,  # This is now a feature!
                    'lexical_weight': lexical_weight,
                    'ndcg_score': ndcg_score,  # This is the target!
                    **query_features  # Add all query features
                }
                
                # Add result features if requested
                if include_result_features and results:
                    result_features = self._extract_result_features(results)
                    training_example.update(result_features)
                
                self.training_data.append(training_example)
        
        logger.info(f"Collected {len(self.training_data)} training examples")
        return pd.DataFrame(self.training_data)
    
    def _extract_result_features(self, results: Dict[str, float]) -> Dict[str, float]:
        """Extract features from search results"""
        if not results:
            return {
                'num_results': 0,
                'max_score': 0.0,
                'min_score': 0.0,
                'avg_score': 0.0,
                'score_std': 0.0,
                'score_range': 0.0
            }
        
        scores = list(results.values())
        return {
            'num_results': len(results),
            'max_score': max(scores),
            'min_score': min(scores),
            'avg_score': np.mean(scores),
            'score_std': np.std(scores),
            'score_range': max(scores) - min(scores)
        }
    
    def _run_hybrid_search(self, query: str, lexical_weight: float,
                          neural_weight: float, top_k: int = 10,
                          dataset_name: str = None) -> Dict[str, float]:
        """Run hybrid search (same as in train_weight_predictor.py)"""
        # Build hybrid query
        if dataset_name and dataset_name.lower() == "esci":
            embedding_field = "title_embedding"
            text_query = {
                "multi_match": {
                    "query": query,
                    "type": "best_fields",
                    "operator": "and",
                    "fields": ["product_id^100", "product_bullet_point^3", "product_color^2", 
                              "product_brand^5", "product_title^10", "product_description"]
                }
            }
        else:
            text_field = "passage_text"
            embedding_field = "passage_embedding"
            text_query = {
                "match": {
                    text_field: {
                        "query": query
                    }
                }
            }
        
        hybrid_query = {
            "_source": False,  
            "size": top_k,
            "query": {
                "hybrid": {
                    "queries": [
                        text_query,
                        {
                            "neural": {
                                embedding_field: {
                                    "query_text": query,
                                    "model_id": self.model_id,
                                    "k": top_k
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "Dynamic post processor for hybrid search",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {
                                "technique": "min_max"  
                            },
                            "combination": {
                                "technique": "arithmetic_mean",
                                "parameters": {
                                    "weights": [lexical_weight, neural_weight]
                                }
                            }
                        }
                    }
                ]
            }
        }
        
        try:
            response = self.client.search(
                index=self.index_name,
                body=hybrid_query
            )
            
            results = {}
            for hit in response['hits']['hits']:
                doc_id = hit['_id']
                score = hit['_score']
                results[doc_id] = score
            
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {}
    
    def _calculate_ndcg_at_k(self, results: Dict[str, float], 
                           relevant_docs: Dict[str, int], k: int = 10) -> float:
        """Calculate NDCG@k"""
        sorted_docs = sorted(results.items(), key=lambda x: x[1], reverse=True)[:k]
        
        dcg = 0.0
        for i, (doc_id, _) in enumerate(sorted_docs):
            if doc_id in relevant_docs:
                relevance = relevant_docs[doc_id]
                dcg += relevance / np.log2(i + 2)
        
        ideal_relevances = sorted(relevant_docs.values(), reverse=True)[:k]
        idcg = sum(rel / np.log2(i + 2) for i, rel in enumerate(ideal_relevances))
        
        return dcg / idcg if idcg > 0 else 0.0
    
    def train_ndcg_model(self, 
                        training_df: pd.DataFrame,
                        model_type: str = "random_forest",
                        feature_columns: Optional[List[str]] = None) -> Dict:
        """
        Train model to predict NDCG values.
        
        Args:
            training_df: DataFrame with training data
            model_type: One of 'random_forest', 'gradient_boosting', 'linear', 'ridge'
            feature_columns: List of feature columns to use
            
        Returns:
            Dictionary with model, scaler, and evaluation metrics
        """
        logger.info(f"Training {model_type} model to predict NDCG...")
        
        # Auto-detect feature columns if not specified
        if feature_columns is None:
            exclude_cols = ['query_id', 'query_text', 'ndcg_score']
            feature_columns = [col for col in training_df.columns 
                             if col not in exclude_cols]
        
        logger.info(f"Using {len(feature_columns)} features")
        logger.info(f"Features: {feature_columns[:10]}...")  # Show first 10
        
        # Prepare features and target
        X = training_df[feature_columns].values
        y = training_df['ndcg_score'].values  # Now predicting NDCG!
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Select and train model
        if model_type == "random_forest":
            model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1
            )
        elif model_type == "gradient_boosting":
            model = GradientBoostingRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=5,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42
            )
        elif model_type == "linear":
            model = LinearRegression()
        elif model_type == "ridge":
            model = Ridge(alpha=1.0)
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Train model
        model.fit(X_train_scaled, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train_scaled)
        y_pred_test = model.predict(X_test_scaled)
        
        # Calculate metrics
        train_mse = mean_squared_error(y_train, y_pred_train)
        test_mse = mean_squared_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        # Cross-validation
        if model_type in ["random_forest", "gradient_boosting"]:
            cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='r2')
            logger.info(f"Cross-validation R² scores: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
        
        logger.info(f"Training MSE: {train_mse:.4f}, R²: {train_r2:.4f}")
        logger.info(f"Test MSE: {test_mse:.4f}, R²: {test_r2:.4f}")
        
        # Feature importance
        feature_importance = None
        if hasattr(model, 'feature_importances_'):
            feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            logger.info("\nTop 10 most important features:")
            print(feature_importance.head(10))
        elif model_type in ["linear", "ridge"]:
            feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'coefficient': model.coef_,
                'abs_coefficient': np.abs(model.coef_)
            }).sort_values('abs_coefficient', ascending=False)
            
            logger.info("\nTop 10 features by absolute coefficient:")
            print(feature_importance.head(10))
        
        return {
            'model': model,
            'scaler': scaler,
            'model_type': model_type,
            'feature_columns': feature_columns,
            'metrics': {
                'train_mse': train_mse,
                'test_mse': test_mse,
                'train_r2': train_r2,
                'test_r2': test_r2,
                'cv_r2_mean': cv_scores.mean() if 'cv_scores' in locals() else None,
                'cv_r2_std': cv_scores.std() if 'cv_scores' in locals() else None
            },
            'feature_importance': feature_importance
        }
    
    def save_model(self, model_dict: Dict, output_path: str):
        """Save trained model and associated data"""
        with open(output_path, 'wb') as f:
            pickle.dump(model_dict, f)
        logger.info(f"Model saved to {output_path}")
    
    def analyze_training_data(self, training_df: pd.DataFrame) -> Dict:
        """Analyze the training data distribution"""
        if len(training_df) == 0:
            return {}
            
        analysis = {
            'total_examples': len(training_df),
            'unique_queries': training_df['query_id'].nunique(),
            'examples_per_query': len(training_df) / training_df['query_id'].nunique(),
            'ndcg_distribution': {
                'mean': training_df['ndcg_score'].mean(),
                'std': training_df['ndcg_score'].std(),
                'min': training_df['ndcg_score'].min(),
                'max': training_df['ndcg_score'].max(),
                'zero_ndcg_count': (training_df['ndcg_score'] == 0).sum()
            }
        }
        
        # Analyze NDCG by weight
        weight_analysis = training_df.groupby('neural_weight')['ndcg_score'].agg(['mean', 'std', 'count'])
        analysis['ndcg_by_weight'] = weight_analysis.to_dict('index')
        
        # Find best weight per query
        best_weights = training_df.loc[training_df.groupby('query_id')['ndcg_score'].idxmax()]
        weight_dist = best_weights['neural_weight'].value_counts().sort_index()
        analysis['optimal_weight_distribution'] = weight_dist.to_dict()
        
        return analysis


def main():
    parser = argparse.ArgumentParser(description='Train NDCG predictor for dynamic hybrid search')
    parser.add_argument('-d', '--dataset', required=True, help='Dataset name')
    parser.add_argument('-u', '--url', required=True, help='Dataset URL (use "local" for ESCI)')
    parser.add_argument('--data-path', default=None, help='Path to dataset files')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', required=True, help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('-o', '--output', default='ndcg_predictor_model.pkl', 
                       help='Output file for trained model')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of queries to sample for training')
    parser.add_argument('--model-type', choices=['random_forest', 'gradient_boosting', 'linear', 'ridge'],
                       default='random_forest', help='Model type to use')
    parser.add_argument('--weight-values', nargs='+', type=float, default=None,
                       help='Neural weight values to test (default: 0.0 to 1.0 by 0.1)')
    parser.add_argument('--training-data-file', default=None,
                       help='Save/load training data to/from this file')
    parser.add_argument('--include-result-features', action='store_true',
                       help='Include search result features in training')
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = NDCGPredictorTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id
    )
    
    # Check if we have saved training data
    if args.training_data_file and os.path.exists(args.training_data_file):
        logger.info(f"Loading training data from {args.training_data_file}")
        training_df = pd.read_csv(args.training_data_file)
    else:
        # Handle dataset loading
        if args.dataset.lower() == "esci":
            if args.data_path:
                data_path = args.data_path
            else:
                data_path = "esci_data"
            
            logger.info(f"Using ESCI data from {data_path}")
            
            if not os.path.exists(data_path):
                logger.error(f"ESCI data not found at {data_path}")
                sys.exit(1)
        else:
            logger.info(f"Downloading dataset from {args.url}")
            out_dir = os.path.join(os.getcwd(), "datasets")
            data_path = util.download_and_unzip(args.url, out_dir)
        
        # Collect training data
        training_df = trainer.collect_training_data(
            args.dataset,
            data_path,
            weight_values=args.weight_values,
            sample_size=args.sample_size,
            include_result_features=args.include_result_features
        )
        
        # Save training data if requested
        if args.training_data_file:
            training_df.to_csv(args.training_data_file, index=False)
            logger.info(f"Training data saved to {args.training_data_file}")
    
    # Analyze training data
    logger.info("\n=== Training Data Analysis ===")
    analysis = trainer.analyze_training_data(training_df)
    
    print(f"Total examples: {analysis['total_examples']}")
    print(f"Unique queries: {analysis['unique_queries']}")
    print(f"Examples per query: {analysis['examples_per_query']:.1f}")
    
    print(f"\nNDCG distribution:")
    print(f"  Mean: {analysis['ndcg_distribution']['mean']:.4f}")
    print(f"  Std: {analysis['ndcg_distribution']['std']:.4f}")
    print(f"  Range: [{analysis['ndcg_distribution']['min']:.4f}, {analysis['ndcg_distribution']['max']:.4f}]")
    print(f"  Zero NDCG: {analysis['ndcg_distribution']['zero_ndcg_count']} examples")
    
    print("\nOptimal weight distribution:")
    for weight, count in sorted(analysis['optimal_weight_distribution'].items()):
        print(f"  {weight}: {count} queries")
    
    # Train model
    model_dict = trainer.train_ndcg_model(training_df, model_type=args.model_type)
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== Model Training Summary ===")
    print(f"Model type: {args.model_type}")
    print(f"Features used: {len(model_dict['feature_columns'])}")
    print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
    print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    if model_dict['metrics']['cv_r2_mean'] is not None:
        print(f"Cross-validation R²: {model_dict['metrics']['cv_r2_mean']:.4f} (+/- {model_dict['metrics']['cv_r2_std']*2:.4f})")
    print(f"\nModel saved to: {args.output}")


if __name__ == "__main__":
    main()
