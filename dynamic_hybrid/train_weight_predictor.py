"""
Train a linear regression model for dynamic hybrid search weight prediction.
This script collects training data by evaluating queries with different weights
and trains a model to predict optimal weights based on query features.
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
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
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
    # Fallback to the local version
    from datasets.data_loader_esci import DataLoader as ESCIDataLoader

# OpenSearch imports
from opensearchpy import OpenSearch

# Local imports
from feature_extractor import DomainAwareFeatureExtractor, get_domain_for_dataset

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class WeightPredictorTrainer:
    """Trainer for weight prediction model"""
    
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
        
        # Training data storage
        self.training_data = []
        
    def collect_training_data(self,
                            dataset_name: str,
                            data_path: str,
                            weight_grid: List[Tuple[float, float]],
                            sample_size: Optional[int] = None) -> pd.DataFrame:
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
        
        # Get domain and initialize feature extractor
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
            # ESCI uses different field names
            text_field = "text_key"
            embedding_field = "title_embedding"  # ML pipeline maps title_key to title_embedding
            text_query = {
                "multi_match": {
                    "query": query,
                    "type": "best_fields",
                    "fields": [text_field, "title_key"],
                    "tie_breaker": 0.5
                }
            }
        else:
            # Standard BEIR field names
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
                   feature_columns: Optional[List[str]] = None) -> Dict:
        """
        Train linear regression model on collected data.
        
        Args:
            training_df: DataFrame with training data
            feature_columns: List of feature columns to use (None for auto-detect)
            
        Returns:
            Dictionary with model, scaler, and evaluation metrics
        """
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
            'metrics': {
                'train_mse': train_mse,
                'test_mse': test_mse,
                'train_r2': train_r2,
                'test_r2': test_r2
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
    parser.add_argument('-u', '--url', required=True, help='Dataset URL')
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
    
    args = parser.parse_args()
    
    # Initialize trainer
    trainer = WeightPredictorTrainer(
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
        # Handle dataset loading - ESCI is special case
        if args.dataset.lower() == "esci":
            # ESCI uses local data
            data_path = os.path.join(os.getcwd(), "dynamic_hybrid", "datasets", "esci")
            logger.info(f"Using local ESCI data from {data_path}")
            
            # Check if data exists
            if not os.path.exists(data_path):
                logger.error(f"ESCI data not found at {data_path}")
                logger.error("Please run the ESCI setup script first:")
                logger.error(f"python dynamic_hybrid/test_esci_fixed.py -d esci -o ingest")
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
    print("\nOptimal weight distribution:")
    for (lex, neural), count in sorted(analysis['weight_distribution'].items()):
        print(f"  {lex}/{neural}: {count} queries")
    
    print(f"\nAverage best score: {analysis['avg_best_score']:.4f}")
    
    print("\nAverage scores by weight combination:")
    for weight_str, stats in sorted(analysis['score_by_weights'].items()):
        print(f"  {weight_str}: {stats['avg_score']:.4f} (±{stats['std_score']:.4f})")
    
    # Train model
    model_dict = trainer.train_model(training_df)
    
    # Save model
    trainer.save_model(model_dict, args.output)
    
    # Print summary
    print("\n=== Model Training Summary ===")
    print(f"Features used: {len(model_dict['feature_columns'])}")
    print(f"Training R²: {model_dict['metrics']['train_r2']:.4f}")
    print(f"Test R²: {model_dict['metrics']['test_r2']:.4f}")
    print(f"\nModel saved to: {args.output}")
    
    # Create a simple usage example
    print("\nTo use the trained model, update weight_predictor.py:")
    print("1. Load the model: model_dict = pickle.load(open('{}', 'rb'))".format(args.output))
    print("2. Use in MLWeightPredictor class for predictions")


if __name__ == "__main__":
    main()
