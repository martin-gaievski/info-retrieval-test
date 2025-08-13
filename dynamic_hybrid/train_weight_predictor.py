"""
Train a model for dynamic hybrid search weight prediction.
This script collects training data by evaluating queries with different weights
and trains either regression or classification models based on query features.
Supports both continuous weight prediction (regression) and discrete weight classes (classification).
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
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.utils.class_weight import compute_class_weight

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
    # Fallback to the local version
    from datasets.data_loader_esci import DataLoader as ESCIDataLoader

# OpenSearch imports
from opensearchpy import OpenSearch

# Local imports
from feature_extractor import DomainAwareFeatureExtractor, get_domain_for_dataset
try:
    from feature_extractor_esci_basic import ESCIBasicFeatureExtractor
except ImportError:
    ESCIBasicFeatureExtractor = None

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


# Define weight classes for classification
WEIGHT_CLASSES = {
    (0.9, 0.1): 0,  # Lexical-heavy
    (0.7, 0.3): 1,  # Balanced-lexical
    (0.5, 0.5): 2,  # Equal
    (0.3, 0.7): 3,  # Balanced-neural
    (0.1, 0.9): 4   # Neural-heavy
}

# Reverse mapping
CLASS_TO_WEIGHTS = {v: k for k, v in WEIGHT_CLASSES.items()}


class WeightPredictorTrainer:
    """Trainer for weight prediction models (regression or classification)"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "beir-index",
                 model_id: str = None,
                 use_basic_features: bool = False):
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
        self.use_basic_features = use_basic_features
        
        # Training data storage
        self.training_data = []
        
    def collect_training_data(self,
                            dataset_name: str,
                            data_path: str,
                            weight_grid: List[Tuple[float, float]],
                            sample_size: Optional[int] = None,
                            use_full_dataset: bool = False) -> pd.DataFrame:
        """
        Collect training data by evaluating queries with different weights.
        
        Args:
            dataset_name: Name of the BEIR dataset
            data_path: Path to dataset files
            weight_grid: List of (lexical, neural) weight combinations to test
            sample_size: Number of queries to sample (None for all)
            
        Returns:
            DataFrame with features and optimal weights
        """
        logger.info(f"Loading dataset: {dataset_name}")
        
        # Load dataset - handle ESCI as special case
        if dataset_name.lower() == "esci":
            loader = ESCIDataLoader(
                data_folder=data_path,
                language="us",
                small_version=not use_full_dataset
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
        if dataset_name.lower() == "esci" and self.use_basic_features:
            if ESCIBasicFeatureExtractor:
                feature_extractor = ESCIBasicFeatureExtractor()
                logger.info("Using ESCIBasicFeatureExtractor (basic features only)")
            else:
                logger.warning("ESCIBasicFeatureExtractor not available, falling back to domain-aware extractor")
                domain = get_domain_for_dataset(dataset_name)
                feature_extractor = DomainAwareFeatureExtractor(domain)
        else:
            domain = get_domain_for_dataset(dataset_name)
            feature_extractor = DomainAwareFeatureExtractor(domain)
        
        logger.info(f"Testing {len(weight_grid)} weight combinations on {len(queries)} queries")
        
        # For each query, find the best weight combination
        for query_id, query_text in tqdm(queries.items(), desc="Processing queries"):
            # Extract features
            features = feature_extractor.extract_features(query_text)
            
            # Evaluate each weight combination
            best_score = -1
            best_weights = None
            weight_scores = {}
            
            for lex_weight, neural_weight in weight_grid:
                # Run search with these weights
                results = self._run_hybrid_search(
                    query_text, 
                    lex_weight, 
                    neural_weight,
                    top_k=10,
                    dataset_name=dataset_name
                )
                
                # Debug: log first result for first weight combination
                if lex_weight == 0.1 and len(results) > 0:
                    logger.debug(f"Query {query_id}: Found {len(results)} results")
                    first_doc = list(results.items())[0]
                    logger.debug(f"  First result: {first_doc}")
                
                # Calculate NDCG@10 for this query
                if query_id in qrels and results:
                    relevant_docs = qrels[query_id]
                    # Check if any retrieved docs are relevant
                    found_relevant = any(doc_id in relevant_docs for doc_id in results.keys())
                    if found_relevant:
                        score = self._calculate_ndcg_at_k(results, relevant_docs, k=10)
                        weight_scores[(lex_weight, neural_weight)] = score
                        
                        if score > best_score:
                            best_score = score
                            best_weights = (lex_weight, neural_weight)
                    else:
                        # No relevant documents found in results
                        if lex_weight == 0.1:  # Log once per query
                            logger.warning(f"Query {query_id}: No relevant docs found in top {len(results)} results")
                            logger.warning(f"  Relevant doc IDs: {list(relevant_docs.keys())[:5]}...")
                            logger.warning(f"  Retrieved doc IDs: {list(results.keys())[:5]}...")
            
            # Store training example if we found a good weight
            if best_weights and best_score > 0:
                training_example = {
                    'query_id': query_id,
                    'query_text': query_text,
                    'best_lexical_weight': best_weights[0],
                    'best_neural_weight': best_weights[1],
                    'best_score': best_score,
                    **features,  # Add all extracted features
                    'weight_scores': json.dumps(
                        {f"{k[0]},{k[1]}": v for k, v in weight_scores.items()}
                    )
                }
                self.training_data.append(training_example)
        
        logger.info(f"Collected {len(self.training_data)} training examples")
        
        # Convert to DataFrame
        df = pd.DataFrame(self.training_data)
        return df
    
    def _run_hybrid_search(self, 
                          query: str, 
                          lexical_weight: float,
                          neural_weight: float,
                          top_k: int = 10,
                          dataset_name: str = None) -> Dict[str, float]:
        """Run hybrid search with specified weights"""
        # Build hybrid query - handle ESCI field names
        if dataset_name and dataset_name.lower() == "esci":
            # ESCI uses different field names (match the ingestion script)
            text_field = "product_title"
            embedding_field = "title_embedding"  # ML pipeline maps product_title to title_embedding
            text_query = {
                "multi_match": {
                    "query": query,
                    "type": "best_fields",
                    "operator": "and",
                    "fields": ["product_id^100", "product_bullet_point^3", "product_color^2", "product_brand^5", "product_title^10", "product_description"]
                }
            }
        else:
            # Standard BEIR field names
            text_field = "product_title"
            embedding_field = "title_embedding"
            text_query = {
                "multi_match": {
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
    
    def _calculate_ndcg_at_k(self, 
                           results: Dict[str, float], 
                           relevant_docs: Dict[str, int],
                           k: int = 10) -> float:
        """Calculate NDCG@k for a single query"""
        # Sort results by score
        sorted_docs = sorted(results.items(), key=lambda x: x[1], reverse=True)[:k]
        
        # Calculate DCG
        dcg = 0.0
        for i, (doc_id, _) in enumerate(sorted_docs):
            if doc_id in relevant_docs:
                relevance = relevant_docs[doc_id]
                dcg += relevance / np.log2(i + 2)  # i+2 because rank starts at 1
        
        # Calculate IDCG
        ideal_relevances = sorted(relevant_docs.values(), reverse=True)[:k]
        idcg = sum(rel / np.log2(i + 2) for i, rel in enumerate(ideal_relevances))
        
        # Calculate NDCG
        return dcg / idcg if idcg > 0 else 0.0
    
    def train_model(self, 
                   training_df: pd.DataFrame,
                   model_type: str = "regression",
                   feature_columns: Optional[List[str]] = None,
                   use_polynomial_features: bool = False) -> Dict:
        """
        Train weight prediction model on collected data.
        
        Args:
            training_df: DataFrame with training data
            model_type: "regression" or "classification"
            feature_columns: List of feature columns to use (None for auto-detect)
            use_polynomial_features: Whether to create polynomial features (for classification)
            
        Returns:
            Dictionary with model, scaler, and evaluation metrics
        """
        if model_type == "classification":
            return self._train_classification_model(training_df, feature_columns, use_polynomial_features)
        else:
            return self._train_regression_model(training_df, feature_columns)
    
    def _train_regression_model(self, 
                               training_df: pd.DataFrame,
                               feature_columns: Optional[List[str]] = None) -> Dict:
        """Train linear regression model (original functionality)"""
        logger.info("Training linear regression model...")
        
        # Auto-detect feature columns if not specified
        if feature_columns is None:
            # Exclude non-feature columns
            exclude_cols = ['query_id', 'query_text', 'best_lexical_weight', 
                          'best_neural_weight', 'best_score', 'weight_scores']
            feature_columns = [col for col in training_df.columns 
                             if col not in exclude_cols]
        
        logger.info(f"Using features: {feature_columns}")
        
        # Prepare features and target
        X = training_df[feature_columns].values
        # Target is neural weight (lexical = 1 - neural)
        y = training_df['best_neural_weight'].values
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        model = LinearRegression()
        model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred_train = model.predict(X_train_scaled)
        y_pred_test = model.predict(X_test_scaled)
        
        # Clip predictions to [0.1, 0.9] range
        y_pred_train = np.clip(y_pred_train, 0.1, 0.9)
        y_pred_test = np.clip(y_pred_test, 0.1, 0.9)
        
        train_mse = mean_squared_error(y_train, y_pred_train)
        test_mse = mean_squared_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        logger.info(f"Training MSE: {train_mse:.4f}, R²: {train_r2:.4f}")
        logger.info(f"Test MSE: {test_mse:.4f}, R²: {test_r2:.4f}")
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_columns,
            'coefficient': model.coef_,
            'abs_coefficient': np.abs(model.coef_)
        }).sort_values('abs_coefficient', ascending=False)
        
        logger.info("\nTop 10 most important features:")
        print(feature_importance.head(10))
        
        return {
            'model': model,
            'scaler': scaler,
            'feature_columns': feature_columns,
            'model_type': 'regression',
            'metrics': {
                'train_mse': train_mse,
                'test_mse': test_mse,
                'train_r2': train_r2,
                'test_r2': test_r2
            },
            'feature_importance': feature_importance
        }
    
    def _train_classification_model(self, 
                                   training_df: pd.DataFrame,
                                   feature_columns: Optional[List[str]] = None,
                                   use_polynomial_features: bool = True) -> Dict:
        """Train logistic regression classifier for weight prediction"""
        logger.info("Training logistic regression classifier...")
        
        # Convert continuous weights to classes
        training_df = self._add_weight_classes(training_df)
        
        # Auto-detect feature columns if not specified
        if feature_columns is None:
            exclude_cols = ['query_id', 'query_text', 'best_lexical_weight', 
                          'best_neural_weight', 'best_score', 'weight_scores', 'weight_class']
            feature_columns = [col for col in training_df.columns 
                             if col not in exclude_cols]
        
        logger.info(f"Using features: {feature_columns}")
        
        # Prepare features and target
        X = training_df[feature_columns].values
        y = training_df['weight_class'].values
        
        # Create polynomial features if requested
        poly_transformer = None
        if use_polynomial_features:
            poly_transformer = PolynomialFeatures(degree=2, interaction_only=True, 
                                                 include_bias=False)
            X = poly_transformer.fit_transform(X)
            logger.info(f"Created polynomial features. Shape: {X.shape}")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Calculate class weights to handle imbalance
        class_weights = compute_class_weight(
            'balanced', 
            classes=np.unique(y_train), 
            y=y_train
        )
        class_weight_dict = {i: w for i, w in enumerate(class_weights)}
        
        # Train model
        model = LogisticRegression(
            # multi_class='ovr' removed - default handles it properly
            solver='lbfgs',  # Better solver for multi-class
            class_weight=class_weight_dict,
            max_iter=1000,
            random_state=42
        )
        model.fit(X_train_scaled, y_train)
        
        # Predictions
        y_pred_train = model.predict(X_train_scaled)
        y_pred_test = model.predict(X_test_scaled)
        
        # Calculate metrics
        train_accuracy = accuracy_score(y_train, y_pred_train)
        test_accuracy = accuracy_score(y_test, y_pred_test)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5)
        
        logger.info(f"Training accuracy: {train_accuracy:.4f}")
        logger.info(f"Test accuracy: {test_accuracy:.4f}")
        logger.info(f"Cross-validation accuracy: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
        
        # Classification report
        class_names = [f"{CLASS_TO_WEIGHTS[i][0]}/{CLASS_TO_WEIGHTS[i][1]}" 
                      for i in range(len(WEIGHT_CLASSES))]
        logger.info("\nClassification Report:")
        print(classification_report(y_test, y_pred_test, target_names=class_names))
        
        # Confusion matrix
        logger.info("\nConfusion Matrix:")
        cm = confusion_matrix(y_test, y_pred_test)
        print(cm)
        
        # Feature importance (from coefficients)
        feature_importance = self._calculate_classification_feature_importance(
            model, feature_columns, poly_transformer
        )
        
        return {
            'model': model,
            'scaler': scaler,
            'poly_transformer': poly_transformer,
            'feature_columns': feature_columns,
            'class_weights': class_weight_dict,
            'model_type': 'classification',
            'weight_classes': WEIGHT_CLASSES,
            'class_to_weights': CLASS_TO_WEIGHTS,
            'metrics': {
                'train_accuracy': train_accuracy,
                'test_accuracy': test_accuracy,
                'cv_accuracy_mean': cv_scores.mean(),
                'cv_accuracy_std': cv_scores.std(),
                'classification_report': classification_report(y_test, y_pred_test, output_dict=True),
                'confusion_matrix': cm.tolist()
            },
            'feature_importance': feature_importance
        }
    
    def _add_weight_classes(self, training_df: pd.DataFrame) -> pd.DataFrame:
        """Add weight class labels to training data"""
        def get_weight_class(row):
            weights = (row['best_lexical_weight'], row['best_neural_weight'])
            # Find closest weight class
            min_dist = float('inf')
            best_class = 0
            for class_weights, class_id in WEIGHT_CLASSES.items():
                dist = abs(weights[0] - class_weights[0]) + abs(weights[1] - class_weights[1])
                if dist < min_dist:
                    min_dist = dist
                    best_class = class_id
            return best_class
        
        training_df['weight_class'] = training_df.apply(get_weight_class, axis=1)
        return training_df
    
    def _calculate_classification_feature_importance(self, model, feature_columns, poly_transformer):
        """Calculate feature importance from logistic regression coefficients"""
        # For multi-class, average absolute coefficients across classes
        avg_coef = np.mean(np.abs(model.coef_), axis=0)
        
        # Get feature names
        if poly_transformer:
            feature_names = poly_transformer.get_feature_names_out(feature_columns)
        else:
            feature_names = feature_columns
        
        # Create importance DataFrame
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': avg_coef
        }).sort_values('importance', ascending=False)
        
        logger.info("\nTop 10 most important features:")
        print(importance_df.head(10))
        
        return importance_df
    
    def save_model(self, model_dict: Dict, output_path: str):
        """Save trained model and associated data"""
        with open(output_path, 'wb') as f:
            pickle.dump(model_dict, f)
        logger.info(f"Model saved to {output_path}")
        
        # Save Java-compatible parameters for classification models
        if model_dict.get('model_type') == 'classification':
            self._save_java_parameters(model_dict, output_path)
    
    def _save_java_parameters(self, model_dict: Dict, base_path: str):
        """Save model parameters in a format easy to copy to Java"""
        java_path = base_path.replace('.pkl', '_java_params.json')
        
        model = model_dict['model']
        scaler = model_dict['scaler']
        
        java_params = {
            'coefficients': model.coef_.tolist(),
            'intercepts': model.intercept_.tolist(),
            'scaler_mean': scaler.mean_.tolist(),
            'scaler_scale': scaler.scale_.tolist(),  # Fixed: use scale_ not std_
            'feature_columns': model_dict['feature_columns'],
            'weight_classes': {str(k): v for k, v in WEIGHT_CLASSES.items()},
            'class_to_weights': {str(k): v for k, v in CLASS_TO_WEIGHTS.items()}
        }
        
        if model_dict.get('poly_transformer'):
            java_params['polynomial_features'] = True
            java_params['polynomial_degree'] = 2
            java_params['interaction_only'] = True
        
        with open(java_path, 'w') as f:
            json.dump(java_params, f, indent=2)
        
        logger.info(f"Java parameters saved to {java_path}")
    
    def analyze_training_data(self, training_df: pd.DataFrame, 
                            model_type: str = "regression") -> Dict:
        """Analyze the training data distribution"""
        # Handle empty DataFrame
        if len(training_df) == 0:
            logger.warning("No training examples collected. Check if:")
            logger.warning("1. The index contains the documents")
            logger.warning("2. The document IDs match between index and qrels")
            logger.warning("3. The queries are finding relevant documents")
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
        
        # Add classification-specific analysis
        if model_type == "classification":
            training_df = self._add_weight_classes(training_df)
            class_dist = training_df['weight_class'].value_counts().sort_index()
            
            analysis['class_distribution'] = {
                f"{CLASS_TO_WEIGHTS[i][0]}/{CLASS_TO_WEIGHTS[i][1]}": count 
                for i, count in class_dist.items()
            }
            analysis['class_balance'] = class_dist.min() / class_dist.max() if len(class_dist) > 0 else 0
            
            # Analyze scores by weight class
            score_by_class = {}
            for class_id in range(len(WEIGHT_CLASSES)):
                class_data = training_df[training_df['weight_class'] == class_id]
                if len(class_data) > 0:
                    score_by_class[f"{CLASS_TO_WEIGHTS[class_id][0]}/{CLASS_TO_WEIGHTS[class_id][1]}"] = {
                        'mean_score': class_data['best_score'].mean(),
                        'std_score': class_data['best_score'].std(),
                        'count': len(class_data)
                    }
            analysis['score_by_class'] = score_by_class
        
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
    parser = argparse.ArgumentParser(description='Train weight predictor for dynamic hybrid search')
    parser.add_argument('-d', '--dataset', required=True, help='Dataset name')
    parser.add_argument('-u', '--url', required=True, help='Dataset URL (use "local" for ESCI)')
    parser.add_argument('--data-path', default=None, help='Path to ESCI parquet files (for ESCI dataset)')
    parser.add_argument('--full-dataset', action='store_true', help='Use full ESCI dataset instead of small version')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', required=True, help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('-o', '--output', default='weight_predictor_model.pkl', 
                       help='Output file for trained model')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of queries to sample for training')
    parser.add_argument('--weight-step', type=float, default=0.1,
                       help='Step size for weight grid (default: 0.1)')
    parser.add_argument('--training-data-file', default=None,
                       help='Save/load training data to/from this file')
    parser.add_argument('--model-type', choices=['regression', 'classification'], 
                       default='regression',
                       help='Type of model to train (default: regression)')
    parser.add_argument('--no-polynomial', action='store_true',
                       help='Disable polynomial feature creation (for classification)')
    parser.add_argument('--use-basic-features', action='store_true',
                       help='Use only basic features for ESCI dataset')
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = WeightPredictorTrainer(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        use_basic_features=args.use_basic_features
    )
    
    # Check if we have saved training data
    if args.training_data_file and os.path.exists(args.training_data_file):
        logger.info(f"Loading training data from {args.training_data_file}")
        training_df = pd.read_csv(args.training_data_file)
    else:
        # Handle dataset loading - ESCI is special case
        if args.dataset.lower() == "esci":
            # ESCI uses local data - use custom path if provided
            if args.data_path:
                data_path = args.data_path
                logger.info(f"Using ESCI data from custom path: {data_path}")
            else:
                data_path = os.path.join(os.getcwd(), "dynamic_hybrid", "datasets", "esci")
                logger.info(f"Using default ESCI data path: {data_path}")
            
            # Check if data exists
            if not os.path.exists(data_path):
                logger.error(f"ESCI data not found at {data_path}")
                if args.data_path:
                    logger.error("Please check the --data-path parameter points to a folder containing:")
                    logger.error("  - shopping_queries_dataset_products_us_small.parquet")
                    logger.error("  - shopping_queries_dataset_examples_us_small.parquet")
                else:
                    logger.error("Please either:")
                    logger.error("1. Use --data-path to specify your ESCI data folder, or")
                    logger.error("2. Run the ESCI setup script first:")
                    logger.error("   python dynamic_hybrid/esci_ingestion.py -m MODEL_ID -d esci_data")
                sys.exit(1)
        else:
            # Download regular BEIR dataset
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
            sample_size=args.sample_size,
            use_full_dataset=args.full_dataset
        )
        
        # Save training data if requested
        if args.training_data_file:
            training_df.to_csv(args.training_data_file, index=False)
            logger.info(f"Training data saved to {args.training_data_file}")
    
    # Analyze training data
    logger.info("\n=== Training Data Analysis ===")
    analysis = trainer.analyze_training_data(training_df, model_type=args.model_type)
    
    print(f"Total training examples: {analysis['total_examples']}")
    
    # If no training examples, exit early
    if analysis['total_examples'] == 0:
        logger.error("Cannot train model without training examples!")
        logger.error("Common issues:")
        logger.error("1. Document IDs in index don't match qrels")
        logger.error("2. Wrong field names in search query (check passage_text vs text)")
        logger.error("3. No documents in index")
        logger.error("4. Model not deployed correctly")
        
        # Run a test query to debug
        logger.info("\nRunning debug query...")
        test_query = "What is COVID-19?"  # Generic test query
        test_results = trainer._run_hybrid_search(test_query, 0.5, 0.5, top_k=10)
        logger.info(f"Debug query '{test_query}' returned {len(test_results)} results")
        
        # Check index stats
        try:
            stats = trainer.client.indices.stats(index=args.index)
            doc_count = stats['indices'][args.index]['total']['docs']['count']
            logger.info(f"Index {args.index} contains {doc_count} documents")
        except Exception as e:
            logger.error(f"Failed to get index stats: {e}")
        
        sys.exit(1)
    
    print(f"Average best score: {analysis['avg_best_score']:.4f}")
    
    if args.model_type == "classification":
        print(f"Class balance ratio: {analysis.get('class_balance', 0):.2f}")
        print("\nClass distribution:")
        for weights, count in analysis.get('class_distribution', {}).items():
            print(f"  {weights}: {count} queries")
        
        print("\nAverage scores by class:")
        for weights, stats in analysis.get('score_by_class', {}).items():
            print(f"  {weights}: {stats['mean_score']:.4f} (±{stats['std_score']:.4f})")
    else:
        print("\nOptimal weight distribution:")
        for (lex, neural), count in sorted(analysis['weight_distribution'].items()):
            print(f"  {lex}/{neural}: {count} queries")
    
    print("\nAverage scores by weight combination:")
    for weight_str, stats in sorted(analysis['score_by_weights'].items()):
        print(f"  {weight_str}: {stats['avg_score']:.4f} (±{stats['std_score']:.4f})")
    
    # Train model
    model_dict = trainer.train_model(
        training_df, 
        model_type=args.model_type,
        use_polynomial_features=not args.no_polynomial if args.model_type == "classification" else False
    )
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== Model Training Summary ===")
    print(f"Model type: {args.model_type}")
    print(f"Features used: {len(model_dict['feature_columns'])}")
    
    if args.model_type == "classification":
        if model_dict.get('poly_transformer'):
            print(f"Polynomial features: Enabled (degree=2, interaction_only=True)")
        print(f"Training accuracy: {model_dict['metrics']['train_accuracy']:.4f}")
        print(f"Test accuracy: {model_dict['metrics']['test_accuracy']:.4f}")
        print(f"Cross-validation accuracy: {model_dict['metrics']['cv_accuracy_mean']:.4f} "
              f"(+/- {model_dict['metrics']['cv_accuracy_std']*2:.4f})")
        if args.model_type == "classification":
            print(f"\nJava parameters saved to: {args.output.replace('.pkl', '_java_params.json')}")
    else:
        print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
        print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    
    print(f"\nModel saved to: {args.output}")
    
    # Usage instructions
    print("\n=== Usage Instructions ===")
    print("To use this model in evaluation:")
    print(f"python dynamic_hybrid/evaluate_dynamic_hybrid_standalone.py \\")
    print(f"  -d {args.dataset} -u {args.url} \\")
    print(f"  --host {args.host} -p {args.port} \\")
    print(f"  -i {args.index} -m {args.model_id} \\")
    print(f"  --use-ml --weight-predictor-model {args.output} \\")
    print(f"  -o evaluation_results.json")


if __name__ == "__main__":
    main()
