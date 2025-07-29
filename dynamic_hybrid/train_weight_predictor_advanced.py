"""
Advanced training script for weight prediction using RandomForest/GradientBoosting.
Improvements over enhanced version:
1. Better model architectures (RandomForest, GradientBoosting)
2. Classification approach for weight prediction
3. Feature importance analysis
4. Cross-validation for better evaluation
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
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, classification_report
from sklearn.preprocessing import StandardScaler

# Add dynamic_hybrid to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# BEIR imports
from beir import util, LoggingHandler
from beir.datasets.data_loader import GenericDataLoader
from beir.retrieval.evaluation import EvaluateRetrieval

# Add the beir path to import the ESCI data loader
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
try:
    from feature_extractor_esci_optimized import ESCIOptimizedFeatureExtractor
except ImportError:
    # If optimized doesn't exist, try to import from enhanced
    try:
        from feature_extractor_esci_enhanced import ESCIOptimizedFeatureExtractor
    except ImportError:
        ESCIOptimizedFeatureExtractor = None

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class AdvancedWeightPredictorTrainer:
    """Advanced trainer with better models and techniques"""
    
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
                            weight_grid: List[Tuple[float, float]],
                            sample_size: Optional[int] = None) -> pd.DataFrame:
        """Collect training data (same as enhanced version)"""
        logger.info(f"Loading dataset: {dataset_name}")
        
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
        if dataset_name.lower() == "esci":
            feature_extractor = ESCIOptimizedFeatureExtractor()
            logger.info("Using ESCIOptimizedFeatureExtractor with stronger intent signals")
        else:
            domain = get_domain_for_dataset(dataset_name)
            feature_extractor = DomainAwareFeatureExtractor(domain)
            
        logger.info(f"Testing {len(weight_grid)} weight combinations on {len(queries)} queries")
        
        # Collect data (same logic as before)
        for query_id, query_text in tqdm(queries.items(), desc="Processing queries"):
            features = feature_extractor.extract_features(query_text)
            
            best_score = -1
            best_weights = None
            weight_scores = {}
            
            for lex_weight, neural_weight in weight_grid:
                results = self._run_hybrid_search(
                    query_text, 
                    lex_weight, 
                    neural_weight,
                    top_k=10,
                    dataset_name=dataset_name
                )
                
                if query_id in qrels and results:
                    relevant_docs = qrels[query_id]
                    found_relevant = any(doc_id in relevant_docs for doc_id in results.keys())
                    if found_relevant:
                        score = self._calculate_ndcg_at_k(results, relevant_docs, k=10)
                        weight_scores[(lex_weight, neural_weight)] = score
                        
                        if score > best_score:
                            best_score = score
                            best_weights = (lex_weight, neural_weight)
            
            if best_weights and best_score > 0:
                training_example = {
                    'query_id': query_id,
                    'query_text': query_text,
                    'best_lexical_weight': best_weights[0],
                    'best_neural_weight': best_weights[1],
                    'best_score': best_score,
                    **features,
                    'weight_scores': json.dumps(
                        {f"{k[0]},{k[1]}": v for k, v in weight_scores.items()}
                    )
                }
                self.training_data.append(training_example)
        
        logger.info(f"Collected {len(self.training_data)} training examples")
        return pd.DataFrame(self.training_data)
    
    def _run_hybrid_search(self, query: str, lexical_weight: float,
                          neural_weight: float, top_k: int = 10,
                          dataset_name: str = None) -> Dict[str, float]:
        """Run hybrid search (same as enhanced version)"""
        # Build hybrid query
        if dataset_name and dataset_name.lower() == "esci":
            text_field = "text_key"
            embedding_field = "title_embedding"
            text_query = {
                "multi_match": {
                    "query": query,
                    "type": "best_fields",
                    "fields": [text_field, "title_key"],
                    "tie_breaker": 0.5
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
        """Calculate NDCG@k (same as enhanced version)"""
        sorted_docs = sorted(results.items(), key=lambda x: x[1], reverse=True)[:k]
        
        dcg = 0.0
        for i, (doc_id, _) in enumerate(sorted_docs):
            if doc_id in relevant_docs:
                relevance = relevant_docs[doc_id]
                dcg += relevance / np.log2(i + 2)
        
        ideal_relevances = sorted(relevant_docs.values(), reverse=True)[:k]
        idcg = sum(rel / np.log2(i + 2) for i, rel in enumerate(ideal_relevances))
        
        return dcg / idcg if idcg > 0 else 0.0
    
    def train_advanced_model(self, 
                           training_df: pd.DataFrame,
                           model_type: str = "random_forest",
                           feature_columns: Optional[List[str]] = None) -> Dict:
        """
        Train advanced model with better architectures.
        
        Args:
            training_df: DataFrame with training data
            model_type: One of 'random_forest', 'gradient_boosting', 'classification'
            feature_columns: List of feature columns to use
            
        Returns:
            Dictionary with model, scaler, and evaluation metrics
        """
        logger.info(f"Training {model_type} model...")
        
        # Auto-detect feature columns if not specified
        if feature_columns is None:
            exclude_cols = ['query_id', 'query_text', 'best_lexical_weight', 
                          'best_neural_weight', 'best_score', 'weight_scores']
            feature_columns = [col for col in training_df.columns 
                             if col not in exclude_cols]
        
        logger.info(f"Using {len(feature_columns)} features")
        
        # Prepare features and target
        X = training_df[feature_columns].values
        y = training_df['best_neural_weight'].values
        
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
        elif model_type == "classification":
            # Convert to classification problem
            def get_weight_class(weight):
                if weight >= 0.8: return 4  # Neural heavy
                elif weight >= 0.6: return 3  # Neural moderate
                elif weight >= 0.4: return 2  # Balanced
                elif weight >= 0.2: return 1  # Lexical moderate
                else: return 0  # Lexical heavy
            
            y_train_class = [get_weight_class(y) for y in y_train]
            y_test_class = [get_weight_class(y) for y in y_test]
            
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                class_weight='balanced',
                random_state=42,
                n_jobs=-1
            )
            model.fit(X_train_scaled, y_train_class)
            
            # For classification, we'll convert back to regression values
            class_to_weight = {0: 0.1, 1: 0.3, 2: 0.5, 3: 0.7, 4: 0.9}
            y_pred_train = [class_to_weight[c] for c in model.predict(X_train_scaled)]
            y_pred_test = [class_to_weight[c] for c in model.predict(X_test_scaled)]
        else:
            raise ValueError(f"Unknown model type: {model_type}")
        
        # Train regression models
        if model_type != "classification":
            model.fit(X_train_scaled, y_train)
            y_pred_train = model.predict(X_train_scaled)
            y_pred_test = model.predict(X_test_scaled)
        
        # Clip predictions to valid range
        y_pred_train = np.clip(y_pred_train, 0.1, 0.9)
        y_pred_test = np.clip(y_pred_test, 0.1, 0.9)
        
        # Calculate metrics
        train_mse = mean_squared_error(y_train, y_pred_train)
        test_mse = mean_squared_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring='r2')
        
        logger.info(f"Training MSE: {train_mse:.4f}, R²: {train_r2:.4f}")
        logger.info(f"Test MSE: {test_mse:.4f}, R²: {test_r2:.4f}")
        logger.info(f"Cross-validation R² scores: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
        
        # Feature importance
        if hasattr(model, 'feature_importances_'):
            feature_importance = pd.DataFrame({
                'feature': feature_columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            logger.info("\nTop 10 most important features:")
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
                'cv_r2_mean': cv_scores.mean(),
                'cv_r2_std': cv_scores.std()
            },
            'feature_importance': feature_importance if 'feature_importance' in locals() else None
        }
    
    def save_model(self, model_dict: Dict, output_path: str):
        """Save trained model and associated data"""
        with open(output_path, 'wb') as f:
            pickle.dump(model_dict, f)
        logger.info(f"Model saved to {output_path}")
    
    def analyze_training_data(self, training_df: pd.DataFrame) -> Dict:
        """Analyze the training data distribution"""
        if len(training_df) == 0:
            return {
                'total_examples': 0,
                'weight_distribution': {},
                'avg_best_score': 0.0,
                'score_by_weights': {}
            }
            
        analysis = {
            'total_examples': len(training_df),
            'weight_distribution': training_df.groupby(
                ['best_lexical_weight', 'best_neural_weight']
            ).size().to_dict(),
            'avg_best_score': training_df['best_score'].mean(),
            'score_by_weights': {}
        }
        
        # Analyze average score for each weight combination
        for _, row in training_df.iterrows():
            weight_scores = json.loads(row['weight_scores'])
            for weight_str, score in weight_scores.items():
                if weight_str not in analysis['score_by_weights']:
                    analysis['score_by_weights'][weight_str] = []
                analysis['score_by_weights'][weight_str].append(score)
        
        # Calculate average scores
        for weight_str in analysis['score_by_weights']:
            scores = analysis['score_by_weights'][weight_str]
            analysis['score_by_weights'][weight_str] = {
                'avg_score': np.mean(scores),
                'std_score': np.std(scores)
            }
        
        return analysis


def main():
    parser = argparse.ArgumentParser(description='Train advanced weight predictor')
    parser.add_argument('-d', '--dataset', required=True, help='Dataset name')
    parser.add_argument('-u', '--url', required=True, help='Dataset URL')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', required=True, help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('-o', '--output', default='weight_predictor_advanced.pkl', 
                       help='Output file for trained model')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of queries to sample for training')
    parser.add_argument('--weight-step', type=float, default=0.1,
                       help='Step size for weight grid')
    parser.add_argument('--model-type', choices=['random_forest', 'gradient_boosting', 'classification'],
                       default='random_forest', help='Model type to use')
    parser.add_argument('--training-data-file', default=None,
                       help='Save/load training data to/from this file')
    parser.add_argument('--data-path', default=None,
                       help='Path to dataset (for ESCI, path to folder with parquet files)')
    parser.add_argument('--full-dataset', action='store_true',
                       help='Use full dataset instead of small version (ESCI specific)')
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = AdvancedWeightPredictorTrainer(
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
            # Check if --data-path was provided
            if hasattr(args, 'data_path') and args.data_path:
                data_path = args.data_path
            else:
                data_path = "esci_data"
            
            logger.info(f"Using ESCI data from {data_path}")
            
            if not os.path.exists(data_path):
                logger.error(f"ESCI data not found at {data_path}")
                logger.error("Please specify the correct path with --data-path argument")
                sys.exit(1)
        else:
            logger.info(f"Downloading dataset from {args.url}")
            out_dir = os.path.join(os.getcwd(), "datasets")
            data_path = util.download_and_unzip(args.url, out_dir)
        
        # Generate weight grid
        weight_values = np.arange(0.1, 1.0, args.weight_step)
        weight_grid = [(round(w1, 1), round(1-w1, 1)) for w1 in weight_values]
        
        logger.info(f"Testing weight combinations: {weight_grid}")
        
        # Collect training data
        training_df = trainer.collect_training_data(
            args.dataset,
            data_path,
            weight_grid,
            sample_size=args.sample_size
        )
        
        # Save training data if requested
        if args.training_data_file:
            training_df.to_csv(args.training_data_file, index=False)
            logger.info(f"Training data saved to {args.training_data_file}")
    
    # Analyze training data
    logger.info("\n=== Training Data Analysis ===")
    analysis = trainer.analyze_training_data(training_df)
    
    print(f"Total training examples: {analysis['total_examples']}")
    
    if analysis['total_examples'] == 0:
        logger.error("Cannot train model without training examples!")
        sys.exit(1)
    
    print("\nOptimal weight distribution:")
    for (lex, neural), count in sorted(analysis['weight_distribution'].items()):
        print(f"  {lex}/{neural}: {count} queries ({count/analysis['total_examples']*100:.1f}%)")
    
    print(f"\nAverage best score: {analysis['avg_best_score']:.4f}")
    
    # Train advanced model
    model_dict = trainer.train_advanced_model(training_df, model_type=args.model_type)
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== Model Training Summary ===")
    print(f"Model type: {args.model_type}")
    print(f"Features used: {len(model_dict['feature_columns'])}")
    print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
    print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    print(f"Cross-validation R²: {model_dict['metrics']['cv_r2_mean']:.4f} (+/- {model_dict['metrics']['cv_r2_std']*2:.4f})")
    print(f"\nModel saved to: {args.output}")


if __name__ == "__main__":
    main()
