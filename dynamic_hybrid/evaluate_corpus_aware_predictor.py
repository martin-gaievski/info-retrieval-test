"""
Evaluate the corpus-aware weight predictor for dynamic hybrid search.
"""

import os
import sys
import json
import argparse
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
import pickle
from tqdm import tqdm

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
    from beir.dataets.data_loader_esci import DataLoader as ESCIDataLoader
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


class CorpusAwareEvaluator:
    """Evaluate corpus-aware weight predictor on a dataset"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "beir-index",
                 model_id: str = None,
                 model_path: str = None,
                 cache_term_stats: bool = True):
        """Initialize evaluator"""
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
        self.cache_term_stats = cache_term_stats
        
        # Load the trained model
        if model_path:
            with open(model_path, 'rb') as f:
                self.model_dict = pickle.load(f)
            logger.info(f"Loaded model from {model_path}")
            logger.info(f"Model type: {self.model_dict['model_type']}")
            logger.info(f"Number of features: {len(self.model_dict['feature_columns'])}")
        else:
            self.model_dict = None
    
    def evaluate(self,
                dataset_name: str,
                data_path: str,
                weight_values: List[float] = None,
                sample_size: Optional[int] = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Evaluate the model on a dataset
        
        Args:
            dataset_name: Name of the BEIR dataset
            data_path: Path to dataset files
            weight_values: List of fixed weights to compare against
            sample_size: Number of queries to evaluate (None for all)
            
        Returns:
            Tuple of (results_df, summary_dict)
        """
        logger.info(f"Evaluating on dataset: {dataset_name}")
        
        # Default weight values for comparison
        if weight_values is None:
            weight_values = [0.3, 0.5, 0.7]  # Common fixed weights
        
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
            np.random.seed(42)
            np.random.shuffle(query_ids)
            query_ids = query_ids[:sample_size]
            queries = {qid: queries[qid] for qid in query_ids}
            logger.info(f"Sampled {sample_size} queries for evaluation")
        
        # Initialize feature extractor
        if dataset_name.lower() == "esci":
            feature_extractor = ESCICorpusAwareFeatureExtractor(
                client=self.client,
                index_name=self.index_name,
                cache_term_stats=self.cache_term_stats
            )
            logger.info("Using ESCICorpusAwareFeatureExtractor")
        else:
            feature_extractor = CorpusAwareFeatureExtractor(
                client=self.client,
                index_name=self.index_name,
                field_name="passage_text",
                cache_term_stats=self.cache_term_stats
            )
            logger.info("Using CorpusAwareFeatureExtractor")
        
        # Results storage
        results = []
        
        # Process each query
        logger.info(f"Evaluating {len(queries)} queries")
        for query_id, query_text in tqdm(queries.items(), desc="Evaluating queries"):
            # Extract features
            features = feature_extractor.extract_features(query_text)
            
            # Predict optimal weight if model is loaded
            predicted_weight = None
            if self.model_dict:
                predicted_weight = self._predict_weight(features)
            
            # Evaluate with predicted weight
            if predicted_weight is not None:
                ndcg_predicted = self._evaluate_weight(
                    query_text, predicted_weight, query_id, qrels, dataset_name
                )
            else:
                ndcg_predicted = 0.0
            
            # Evaluate with fixed weights
            ndcg_fixed = {}
            for weight in weight_values:
                ndcg_fixed[f"ndcg_fixed_{weight}"] = self._evaluate_weight(
                    query_text, weight, query_id, qrels, dataset_name
                )
            
            # Find oracle weight (best possible)
            oracle_weight, ndcg_oracle = self._find_oracle_weight(
                query_text, query_id, qrels, dataset_name
            )
            
            # Store results
            result = {
                'query_id': query_id,
                'query_text': query_text[:100],  # Truncate for display
                'predicted_weight': predicted_weight,
                'oracle_weight': oracle_weight,
                'ndcg_predicted': ndcg_predicted,
                'ndcg_oracle': ndcg_oracle,
                **ndcg_fixed,
                **{f'feature_{k}': v for k, v in features.items()}
            }
            results.append(result)
        
        # Clear cache
        if hasattr(feature_extractor, 'clear_cache'):
            feature_extractor.clear_cache()
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Calculate summary statistics
        summary = self._calculate_summary(results_df, weight_values)
        
        return results_df, summary
    
    def _predict_weight(self, features: Dict[str, float]) -> float:
        """Predict optimal weight using the trained model"""
        # Prepare features in the correct order
        feature_values = []
        for col in self.model_dict['feature_columns']:
            if col in features:
                feature_values.append(features[col])
            else:
                # Handle missing features (e.g., result features)
                feature_values.append(0.0)
        
        # Scale features
        X = np.array(feature_values).reshape(1, -1)
        X_scaled = self.model_dict['scaler'].transform(X)
        
        # Predict
        weight = self.model_dict['model'].predict(X_scaled)[0]
        
        # Clip to valid range
        return np.clip(weight, 0.0, 1.0)
    
    def _evaluate_weight(self, query: str, neural_weight: float, 
                        query_id: str, qrels: Dict, dataset_name: str) -> float:
        """Evaluate a specific weight for a query"""
        lexical_weight = 1.0 - neural_weight
        
        # Run search
        results = self._run_hybrid_search(
            query, lexical_weight, neural_weight, 
            dataset_name=dataset_name
        )
        
        # Calculate NDCG
        if query_id in qrels and results:
            relevant_docs = qrels[query_id]
            return self._calculate_ndcg_at_k(results, relevant_docs, k=10)
        
        return 0.0
    
    def _find_oracle_weight(self, query: str, query_id: str, 
                           qrels: Dict, dataset_name: str) -> Tuple[float, float]:
        """Find the best possible weight for a query"""
        best_weight = 0.0
        best_ndcg = 0.0
        
        # Test weights from 0.0 to 1.0 in steps of 0.1
        for w in range(0, 11):
            weight = w / 10.0
            ndcg = self._evaluate_weight(query, weight, query_id, qrels, dataset_name)
            if ndcg > best_ndcg:
                best_ndcg = ndcg
                best_weight = weight
        
        return best_weight, best_ndcg
    
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
    
    def _calculate_summary(self, results_df: pd.DataFrame, 
                          weight_values: List[float]) -> Dict:
        """Calculate summary statistics"""
        summary = {
            'num_queries': len(results_df),
            'avg_predicted_weight': results_df['predicted_weight'].mean() if 'predicted_weight' in results_df else None,
            'avg_oracle_weight': results_df['oracle_weight'].mean(),
            'avg_ndcg_predicted': results_df['ndcg_predicted'].mean() if 'ndcg_predicted' in results_df else None,
            'avg_ndcg_oracle': results_df['ndcg_oracle'].mean(),
        }
        
        # Add fixed weight results
        for weight in weight_values:
            col_name = f'ndcg_fixed_{weight}'
            if col_name in results_df:
                summary[f'avg_{col_name}'] = results_df[col_name].mean()
        
        # Calculate improvements
        if summary['avg_ndcg_predicted'] is not None:
            for weight in weight_values:
                col_name = f'ndcg_fixed_{weight}'
                if col_name in results_df:
                    fixed_avg = results_df[col_name].mean()
                    improvement = (summary['avg_ndcg_predicted'] - fixed_avg) / fixed_avg * 100
                    summary[f'improvement_over_{weight}'] = improvement
            
            # Oracle gap
            oracle_gap = (summary['avg_ndcg_oracle'] - summary['avg_ndcg_predicted']) / summary['avg_ndcg_oracle'] * 100
            summary['oracle_gap_percent'] = oracle_gap
        
        return summary


def main():
    parser = argparse.ArgumentParser(description='Evaluate corpus-aware weight predictor')
    parser.add_argument('-d', '--dataset', required=True, help='Dataset name')
    parser.add_argument('-u', '--url', required=True, help='Dataset URL (use "local" for ESCI)')
    parser.add_argument('--data-path', default=None, help='Path to dataset files')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', required=True, help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('--model-path', required=True, help='Path to trained model')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Number of queries to evaluate')
    parser.add_argument('--weight-values', nargs='+', type=float, 
                       default=[0.3, 0.5, 0.7],
                       help='Fixed weight values to compare against')
    parser.add_argument('--output', default='evaluation_results_corpus_aware.json',
                       help='Output file for results')
    parser.add_argument('--no-cache', action='store_true',
                       help='Disable caching of term statistics')
    
    args = parser.parse_args()
    
    # Initialize evaluator
    evaluator = CorpusAwareEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        model_path=args.model_path,
        cache_term_stats=not args.no_cache
    )
    
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
    
    # Evaluate
    results_df, summary = evaluator.evaluate(
        args.dataset,
        data_path,
        weight_values=args.weight_values,
        sample_size=args.sample_size
    )
    
    # Print summary
    print("\n=== Evaluation Summary ===")
    print(f"Number of queries: {summary['num_queries']}")
    
    if summary['avg_ndcg_predicted'] is not None:
        print(f"\nAverage NDCG@10:")
        print(f"  Predicted (corpus-aware): {summary['avg_ndcg_predicted']:.4f}")
        print(f"  Oracle (best possible): {summary['avg_ndcg_oracle']:.4f}")
        
        for weight in args.weight_values:
            print(f"  Fixed weight {weight}: {summary[f'avg_ndcg_fixed_{weight}']:.4f}")
        
        print(f"\nImprovements:")
        for weight in args.weight_values:
            improvement = summary.get(f'improvement_over_{weight}', 0)
            print(f"  Over fixed weight {weight}: {improvement:+.2f}%")
        
        print(f"\nOracle gap: {summary['oracle_gap_percent']:.2f}%")
        
        print(f"\nAverage predicted weight: {summary['avg_predicted_weight']:.3f}")
        print(f"Average oracle weight: {summary['avg_oracle_weight']:.3f}")
    else:
        print("\nNo model predictions available (model not loaded)")
    
    # Save results
    with open(args.output, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Save detailed results
    results_csv = args.output.replace('.json', '_detailed.csv')
    results_df.to_csv(results_csv, index=False)
    
    logger.info(f"Results saved to {args.output}")
    logger.info(f"Detailed results saved to {results_csv}")


if __name__ == "__main__":
    main()
