"""
Train a model with corpus-aware features for dynamic hybrid search weight prediction.
This implements the approach with query string features and corpus-based features.
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
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
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
from feature_extractor_corpus_aware import CorpusAwareFeatureExtractor, ESCICorpusAwareFeatureExtractor

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class CorpusAwareWeightPredictorTrainer:
    """Train a model to predict optimal weights using corpus-aware features"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "beir-index",
                 model_id: str = None,
                 random_seed: int = 42,
                 cache_term_stats: bool = True):
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
        self.random_seed = random_seed
        self.cache_term_stats = cache_term_stats
        
    def collect_training_data(self,
                            dataset_name: str,
                            data_path: str,
                            weight_values: List[float] = None,
                            sample_size: Optional[int] = None,
                            include_result_features: bool = False,
                            extraction_method: str = 'corpus',
                            feature_set: str = 'full') -> pd.DataFrame:
        """
        Collect training data by evaluating queries with different weights.
        
        Args:
            dataset_name: Name of the BEIR dataset
            data_path: Path to dataset files
            weight_values: List of neural weight values to test (default: 0.0 to 1.0 by 0.1)
            sample_size: Number of queries to sample (None for all)
            include_result_features: Whether to include search result features
            extraction_method: Feature extraction method to use
            feature_set: Feature set to use ('full' or 'o19s')
            
        Returns:
            DataFrame with features and optimal weights
        """
        logger.info(f"Loading dataset: {dataset_name}")
        
        # Default weight values
        if weight_values is None:
            weight_values = [round(w/10, 1) for w in range(0, 11)]  # 0.0, 0.1, ..., 1.0
        
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
            # Set seed for reproducible sampling
            np.random.seed(self.random_seed)
            np.random.shuffle(query_ids)
            query_ids = query_ids[:sample_size]
            queries = {qid: queries[qid] for qid in query_ids}
            logger.info(f"Sampled {sample_size} queries for training (seed: {self.random_seed})")
        
        # Initialize feature extractor based on chosen method
        logger.info(f"Using {extraction_method} feature extraction method with {feature_set} feature set")
        
        if dataset_name.lower() == "esci":
            if extraction_method == 'corpus':
                # Corpus-aware approach using termvectors API
                feature_extractor = ESCICorpusAwareFeatureExtractor(
                    client=self.client,
                    index_name=self.index_name,
                    cache_term_stats=self.cache_term_stats,
                    feature_set=feature_set
                )
                logger.info(f"Using ESCICorpusAwareFeatureExtractor (termvectors API) with {feature_set} features")
                
            elif extraction_method == 'o19s':
                # O19S search result-based approach
                from o19s_validation_implementation import O19SResultFeatureExtractor
                from beir.hybrid.search import RetrievalOpenSearch
                
                searcher = RetrievalOpenSearch(
                    endpoint='localhost',  # Will be overridden by actual args
                    port='9200',
                    index_name=self.index_name,
                    model_id=self.model_id,
                    search_method="hybrid"
                )
                
                feature_extractor = O19SResultFeatureExtractor(searcher)
                logger.info("Using O19SResultFeatureExtractor (query + search results only)")
                
            elif extraction_method == 'corpus_search':
                # O19S-compatible search-based approach
                from feature_extractor_o19s_compatible import O19SCompatibleFeatureExtractor
                
                feature_extractor = O19SCompatibleFeatureExtractor(
                    client=self.client,
                    index_name=self.index_name,
                    model_id=self.model_id
                )
                logger.info("Using O19SCompatibleFeatureExtractor (search-based, no termvectors)")
                
            else:
                raise ValueError(f"Unknown extraction method: {extraction_method}")
                
        else:
            # Use generic corpus-aware extractor for non-ESCI datasets
            feature_extractor = CorpusAwareFeatureExtractor(
                client=self.client,
                index_name=self.index_name,
                field_name="passage_text",  # Adjust based on dataset
                cache_term_stats=self.cache_term_stats
            )
            logger.info("Using CorpusAwareFeatureExtractor")
            
        logger.info(f"Testing {len(weight_values)} weight values on {len(queries)} queries")
        
        # For each query, find the best weight
        for query_id, query_text in tqdm(queries.items(), desc="Processing queries"):
            # Extract corpus-aware features
            query_features = feature_extractor.extract_features(query_text)
            
            # Test each weight value and find the best
            best_weight = None
            best_ndcg = -1
            weight_ndcg_scores = []
            
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
                
                weight_ndcg_scores.append((neural_weight, ndcg_score))
                
                # Track best weight
                if ndcg_score > best_ndcg:
                    best_ndcg = ndcg_score
                    best_weight = neural_weight
            
            # Only add training example if we found relevant results
            if best_weight is not None and best_ndcg > 0:
                training_example = {
                    'query_id': query_id,
                    'query_text': query_text,
                    'optimal_neural_weight': best_weight,
                    'best_ndcg': best_ndcg,
                    **query_features  # Add all corpus-aware features
                }
                
                # Add result features if requested (from the best performing weight)
                if include_result_features:
                    results = self._run_hybrid_search(
                        query_text, 
                        1.0 - best_weight, 
                        best_weight,
                        top_k=10,
                        dataset_name=dataset_name
                    )
                    if results:
                        result_features = self._extract_result_features(results)
                        training_example.update(result_features)
                
                self.training_data.append(training_example)
        
        logger.info(f"Collected {len(self.training_data)} training examples with positive NDCG")
        
        # Clear feature extractor cache if needed
        if hasattr(feature_extractor, 'clear_cache'):
            feature_extractor.clear_cache()
        
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
        """Run hybrid search"""
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
    
    def train_weight_model(self, 
                          training_df: pd.DataFrame,
                          model_type: str = "random_forest",
                          feature_columns: Optional[List[str]] = None) -> Dict:
        """
        Train model to predict optimal neural weights.
        
        Args:
            training_df: DataFrame with training data
            model_type: One of 'random_forest', 'gradient_boosting', 'linear', 'ridge'
            feature_columns: List of feature columns to use
            
        Returns:
            Dictionary with model, scaler, and evaluation metrics
        """
        logger.info(f"Training {model_type} model to predict optimal weights...")
        
        # Auto-detect feature columns if not specified
        if feature_columns is None:
            exclude_cols = ['query_id', 'query_text', 'optimal_neural_weight', 'best_ndcg']
            feature_columns = [col for col in training_df.columns 
                             if col not in exclude_cols]
        
        logger.info(f"Using {len(feature_columns)} features")
        logger.info(f"Features: {', '.join(feature_columns[:10])}...")  # Show first 10
        
        # Log corpus feature statistics
        corpus_features = [col for col in feature_columns if 'document_frequency' in col]
        if corpus_features:
            logger.info(f"Including {len(corpus_features)} corpus-based features")
        
        # Prepare features and target
        X = training_df[feature_columns].values
        y = training_df['optimal_neural_weight'].values
        
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
        
        # Clip predictions to valid range [0, 1]
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
        cv_scores = None
        if model_type in ["random_forest", "gradient_boosting"]:
            cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='r2')
            logger.info(f"Cross-validation R² scores: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
        
        logger.info(f"Training MSE: {train_mse:.4f}, MAE: {train_mae:.4f}, R²: {train_r2:.4f}")
        logger.info(f"Test MSE: {test_mse:.4f}, MAE: {test_mae:.4f}, R²: {test_r2:.4f}")
        
        # Feature importance analysis
        feature_importance = None
        if hasattr(model, 'feature_importances_'):
            feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            logger.info("\nTop 15 most important features:")
            print(feature_importance.head(15))
            
            # Analyze corpus feature importance
            corpus_importance = feature_importance[feature_importance['feature'].str.contains('document_frequency')]
            if not corpus_importance.empty:
                logger.info(f"\nCorpus feature importance: {corpus_importance['importance'].sum():.4f} "
                          f"({100 * corpus_importance['importance'].sum() / feature_importance['importance'].sum():.1f}% of total)")
                
        elif model_type in ["linear", "ridge"]:
            feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'coefficient': model.coef_,
                'abs_coefficient': np.abs(model.coef_)
            }).sort_values('abs_coefficient', ascending=False)
            
            logger.info("\nTop 15 features by absolute coefficient:")
            print(feature_importance.head(15))
        
        return {
            'model': model,
            'scaler': scaler,
            'model_type': model_type,
            'feature_columns': feature_columns,
            'metrics': {
                'train_mse': train_mse,
                'test_mse': test_mse,
                'train_mae': train_mae,
                'test_mae': test_mae,
                'train_r2': train_r2,
                'test_r2': test_r2,
                'cv_r2_mean': cv_scores.mean() if cv_scores is not None else None,
                'cv_r2_std': cv_scores.std() if cv_scores is not None else None
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
            'optimal_weight_distribution': {
                'mean': training_df['optimal_neural_weight'].mean(),
                'std': training_df['optimal_neural_weight'].std(),
                'min': training_df['optimal_neural_weight'].min(),
                'max': training_df['optimal_neural_weight'].max()
            },
            'best_ndcg_distribution': {
                'mean': training_df['best_ndcg'].mean(),
                'std': training_df['best_ndcg'].std(),
                'min': training_df['best_ndcg'].min(),
                'max': training_df['best_ndcg'].max()
            }
        }
        
        # Analyze corpus features if present
        corpus_cols = [col for col in training_df.columns if 'document_frequency' in col]
        if corpus_cols:
            analysis['corpus_features'] = {}
            for col in corpus_cols[:6]:  # Analyze first 6 corpus features
                analysis['corpus_features'][col] = {
                    'mean': training_df[col].mean(),
                    'std': training_df[col].std(),
                    'min': training_df[col].min(),
                    'max': training_df[col].max()
                }
        
        # Weight distribution
        weight_dist = training_df['optimal_neural_weight'].value_counts().sort_index()
        analysis['weight_distribution'] = weight_dist.to_dict()
        
        return analysis


def main():
    parser = argparse.ArgumentParser(description='Train corpus-aware weight predictor for dynamic hybrid search')
    parser.add_argument('-d', '--dataset', required=True, help='Dataset name')
    parser.add_argument('-u', '--url', required=True, help='Dataset URL (use "local" for ESCI)')
    parser.add_argument('--data-path', default=None, help='Path to dataset files')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', required=True, help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('-o', '--output', default='corpus_aware_weight_predictor.pkl', 
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
    parser.add_argument('--no-cache', action='store_true',
                       help='Disable caching of term statistics')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for training query selection')
    parser.add_argument('--extraction-method', 
                       choices=['corpus', 'o19s', 'corpus_search'],
                       default='corpus',
                       help='Feature extraction method: corpus (termvectors), o19s (search results), corpus_search (search-based)')
    parser.add_argument('--feature-set',
                       choices=['full', 'o19s'],
                       default='full',
                       help='Feature set to use: full (22 features) or o19s (15 features, matches O19S exactly)')
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = CorpusAwareWeightPredictorTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        random_seed=args.seed,
        cache_term_stats=not args.no_cache
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
            include_result_features=args.include_result_features,
            extraction_method=args.extraction_method,
            feature_set=args.feature_set
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
    
    print(f"\nOptimal weight distribution:")
    print(f"  Mean: {analysis['optimal_weight_distribution']['mean']:.3f}")
    print(f"  Std: {analysis['optimal_weight_distribution']['std']:.3f}")
    print(f"  Range: [{analysis['optimal_weight_distribution']['min']:.1f}, {analysis['optimal_weight_distribution']['max']:.1f}]")
    
    print(f"\nBest NDCG distribution:")
    print(f"  Mean: {analysis['best_ndcg_distribution']['mean']:.4f}")
    print(f"  Std: {analysis['best_ndcg_distribution']['std']:.4f}")
    
    if 'corpus_features' in analysis:
        print(f"\nCorpus feature statistics (sample):")
        for feat_name, stats in list(analysis['corpus_features'].items())[:3]:
            print(f"  {feat_name}:")
            print(f"    Mean: {stats['mean']:.2f}, Range: [{stats['min']:.2f}, {stats['max']:.2f}]")
    
    print("\nWeight distribution:")
    for weight, count in sorted(analysis['weight_distribution'].items()):
        print(f"  {weight}: {count} queries")
    
    # Train model
    model_dict = trainer.train_weight_model(training_df, model_type=args.model_type)
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== Model Training Summary ===")
    print(f"Model type: {args.model_type}")
    print(f"Feature set: {args.feature_set}")
    print(f"Features used: {len(model_dict['feature_columns'])}")
    print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
    print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    print(f"Test MAE: {model_dict['metrics']['test_mae']:.4f}")
    if model_dict['metrics']['cv_r2_mean'] is not None:
        print(f"Cross-validation R²: {model_dict['metrics']['cv_r2_mean']:.4f} (+/- {model_dict['metrics']['cv_r2_std']*2:.4f})")
    print(f"\nModel saved to: {args.output}")


if __name__ == "__main__":
    main()
