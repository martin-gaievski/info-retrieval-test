"""
Evaluate dynamic hybrid search on BEIR datasets.
Clean version for public repository.
"""

import os
import sys
import json
import argparse
import logging
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import numpy as np

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# BEIR imports
from beir import util, LoggingHandler
from beir.datasets.data_loader import GenericDataLoader
from beir.retrieval.evaluation import EvaluateRetrieval

# Special handling for ESCI - requires additional module
try:
    from beir.datasets.data_loader_esci import DataLoader as ESCIDataLoader
    ESCI_AVAILABLE = True
except ImportError:
    # Try alternative import path
    try:
        import sys
        import os
        beir_path = os.path.join(os.path.dirname(__file__), '..', 'beir')
        sys.path.insert(0, beir_path)
        from datasets.data_loader_esci import DataLoader as ESCIDataLoader
        ESCI_AVAILABLE = True
    except ImportError:
        ESCI_AVAILABLE = False
        print("Warning: ESCI DataLoader not available. Install from: https://github.com/beir-cellar/beir")

# OpenSearch imports
from opensearchpy import OpenSearch

# Local imports
from feature_extractor import DomainAwareFeatureExtractor, get_domain_for_dataset
from weight_predictor import get_predictor_for_dataset

# Configure logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S',
                   level=logging.INFO,
                   handlers=[LoggingHandler()])
logger = logging.getLogger(__name__)


class DynamicHybridSearchEvaluator:
    """Evaluator for dynamic hybrid search on BEIR datasets"""
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "beir-index",
                 model_id: str = None,
                 use_ml_predictor: bool = False,
                 model_path: Optional[str] = None):
        """
        Initialize evaluator.
        
        Args:
            host: OpenSearch host
            port: OpenSearch port
            index_name: Name of the index
            model_id: Neural model ID for semantic search
            use_ml_predictor: Whether to use ML-based weight prediction
            model_path: Path to weight predictor model file
        """
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
        self.use_ml_predictor = use_ml_predictor
        self.model_path = model_path
        
        # Results storage
        self.query_weights = {}
        self.query_features = {}
    
    def evaluate_dataset(self, 
                        dataset_name: str,
                        data_path: str,
                        static_weights: Optional[Tuple[float, float]] = None,
                        k_values: List[int] = [1, 3, 5, 10, 100, 1000],
                        max_queries: Optional[int] = None,
                        corpus: Optional[Dict] = None,
                        queries: Optional[Dict] = None,
                        qrels: Optional[Dict] = None) -> Dict:
        """
        Evaluate on a BEIR dataset with dynamic or static weights.
        
        Args:
            dataset_name: Name of the BEIR dataset
            data_path: Path to dataset files
            static_weights: If provided, use static weights instead of dynamic
            k_values: k values for metrics
            max_queries: Maximum number of queries to evaluate
            corpus: Pre-loaded corpus (optional, to avoid reloading)
            queries: Pre-loaded queries (optional, to avoid reloading)
            qrels: Pre-loaded qrels (optional, to avoid reloading)
            
        Returns:
            Dictionary containing evaluation results
        """
        # Load dataset if not provided
        if corpus is None or queries is None or qrels is None:
            logger.info(f"Loading dataset: {dataset_name}")
            
            # Load dataset - handle ESCI as special case
            if dataset_name.lower() == "esci":
                if not ESCI_AVAILABLE:
                    raise ImportError("ESCI dataset requires special loader. See DEPENDENCIES.md")
                loader = ESCIDataLoader(
                    data_folder=data_path,
                    language="us",
                    small_version=True
                )
                corpus, queries, qrels = loader.load(split="test")
            else:
                corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split="test")
        else:
            logger.info(f"Using pre-loaded dataset: {dataset_name}")
        
        # Get domain and initialize components
        domain = get_domain_for_dataset(dataset_name)
        feature_extractor = DomainAwareFeatureExtractor(domain)
        weight_predictor = get_predictor_for_dataset(dataset_name, self.use_ml_predictor, self.model_path)
        
        logger.info(f"Dataset domain: {domain.value}")
        logger.info(f"Total queries in dataset: {len(queries)}")
        
        # Limit queries if max_queries is specified
        if max_queries is not None and max_queries < len(queries):
            # Take first N queries
            limited_queries = dict(list(queries.items())[:max_queries])
            logger.info(f"Limited to {max_queries} queries for faster evaluation")
        else:
            limited_queries = queries
        
        logger.info(f"Processing {len(limited_queries)} queries...")
        
        # Process queries and get results
        all_results = {}
        weight_distribution = defaultdict(int)
        
        for query_id, query_text in limited_queries.items():
            # Extract features
            features = feature_extractor.extract_features(query_text)
            self.query_features[query_id] = features
            
            # Predict weights or use static
            if static_weights:
                lexical_weight, neural_weight = static_weights
            else:
                lexical_weight, neural_weight = weight_predictor.predict_weights(features)
            
            self.query_weights[query_id] = (lexical_weight, neural_weight)
            
            # Track weight distribution
            weight_bucket = f"{lexical_weight:.1f}/{neural_weight:.1f}"
            weight_distribution[weight_bucket] += 1
            
            # Run hybrid search with predicted weights
            results = self._run_hybrid_search(
                query_text, 
                lexical_weight, 
                neural_weight,
                top_k=max(k_values)
            )
            
            all_results[query_id] = results
        
        # Log weight distribution
        logger.info("Weight distribution across queries:")
        for bucket, count in sorted(weight_distribution.items()):
            logger.info(f"  {bucket}: {count} queries")
        
        # Evaluate results
        evaluator = EvaluateRetrieval()
        ndcg, _map, recall, precision = evaluator.evaluate(
            qrels, all_results, k_values
        )
        
        # Compile results
        evaluation_results = {
            "dataset": dataset_name,
            "domain": domain.value,
            "num_queries": len(queries),
            "metrics": {
                "ndcg": ndcg,
                "map": _map,
                "recall": recall,
                "precision": precision
            },
            "weight_distribution": dict(weight_distribution),
            "is_dynamic": static_weights is None
        }
        
        # Add average metrics
        evaluation_results["average_metrics"] = {
            "ndcg@10": ndcg.get("NDCG@10", 0.0),
            "map@10": _map.get("MAP@10", 0.0),
            "recall@10": recall.get("Recall@10", 0.0),
            "precision@10": precision.get("P@10", 0.0)
        }
        
        return evaluation_results
    
    def _run_hybrid_search(self, 
                          query: str, 
                          lexical_weight: float,
                          neural_weight: float,
                          top_k: int = 1000) -> Dict[str, float]:
        """
        Run hybrid search with specified weights.
        
        Args:
            query: Query text
            lexical_weight: Weight for lexical search
            neural_weight: Weight for neural search
            top_k: Number of results to retrieve
            
        Returns:
            Dictionary mapping doc_id to score
        """
        # Build hybrid query - handle ESCI field names
        if hasattr(self, 'dataset_name') and self.dataset_name.lower() == "esci":
            # ESCI uses different field names
            #text_field = "text_key"
            embedding_field = "title_embedding"
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
            text_field = "passage_text"
            embedding_field = "title_embedding"
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
        
        # Execute search with dynamic inline pipeline
        try:
            response = self.client.search(
                index=self.index_name,
                body=hybrid_query
            )
            
            # Extract results
            results = {}
            for hit in response['hits']['hits']:
                doc_id = hit['_id']
                score = hit['_score']
                results[doc_id] = score
            
            return results
            
        except Exception as e:
            logger.error(f"Search failed for query: {query[:50]}... Error: {e}")
            logger.error(f"Query was: {json.dumps(hybrid_query, indent=2)}")
            return {}
    
    def compare_static_vs_dynamic(self,
                                 dataset_name: str,
                                 data_path: str,
                                 static_weights: List[Tuple[float, float]],
                                 k_values: List[int] = [10],
                                 max_queries: Optional[int] = None) -> Dict:
        """
        Compare static and dynamic weight approaches.
        
        Args:
            dataset_name: Name of the BEIR dataset
            data_path: Path to dataset files
            static_weights: List of static weight configurations to test
            k_values: k values for metrics
            
        Returns:
            Comparison results
        """
        results = {
            "dataset": dataset_name,
            "comparisons": []
        }
        
        # Load dataset once at the beginning
        logger.info(f"Loading dataset once for all comparisons: {dataset_name}")
        
        if dataset_name.lower() == "esci":
            if not ESCI_AVAILABLE:
                raise ImportError("ESCI dataset requires special loader. See DEPENDENCIES.md")
            loader = ESCIDataLoader(
                data_folder=data_path,
                language="us",
                small_version=True
            )
            corpus, queries, qrels = loader.load(split="test")
        else:
            corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split="test")
        
        logger.info(f"Dataset loaded. Corpus size: {len(corpus)}, Queries: {len(queries)}")
        
        # Evaluate with dynamic weights
        logger.info("Evaluating with dynamic weights...")
        dynamic_results = self.evaluate_dataset(
            dataset_name, data_path, 
            static_weights=None, 
            k_values=k_values, 
            max_queries=max_queries,
            corpus=corpus,
            queries=queries,
            qrels=qrels
        )
        
        results["dynamic"] = dynamic_results
        
        # Evaluate with each static weight configuration
        for lex_weight, neural_weight in static_weights:
            logger.info(f"Evaluating with static weights: {lex_weight}/{neural_weight}")
            
            static_results = self.evaluate_dataset(
                dataset_name, 
                data_path, 
                static_weights=(lex_weight, neural_weight),
                k_values=k_values,
                max_queries=max_queries,
                corpus=corpus,
                queries=queries,
                qrels=qrels
            )
            
            # Calculate improvements
            improvements = {}
            for metric in ["ndcg@10", "map@10", "recall@10", "precision@10"]:
                dynamic_val = dynamic_results["average_metrics"][metric]
                static_val = static_results["average_metrics"][metric]
                improvement = ((dynamic_val - static_val) / static_val * 100) if static_val > 0 else 0
                improvements[metric] = {
                    "dynamic": dynamic_val,
                    "static": static_val,
                    "improvement_pct": improvement
                }
            
            results["comparisons"].append({
                "static_weights": f"{lex_weight}/{neural_weight}",
                "static_results": static_results,
                "improvements": improvements
            })
        
        return results
    
    def analyze_query_performance(self) -> Dict:
        """
        Analyze performance by query characteristics.
        
        Returns:
            Analysis results
        """
        # Group queries by predicted weights
        weight_groups = defaultdict(list)
        for query_id, (lex_w, neural_w) in self.query_weights.items():
            bucket = f"{lex_w:.1f}/{neural_w:.1f}"
            weight_groups[bucket].append(query_id)
        
        # Analyze features by weight group
        analysis = {
            "weight_groups": {}
        }
        
        for bucket, query_ids in weight_groups.items():
            features_in_group = [self.query_features[qid] for qid in query_ids]
            
            # Calculate average features
            avg_features = {}
            feature_names = features_in_group[0].keys() if features_in_group else []
            
            for feature in feature_names:
                values = [f.get(feature, 0) for f in features_in_group]
                avg_features[feature] = np.mean(values)
            
            analysis["weight_groups"][bucket] = {
                "num_queries": len(query_ids),
                "avg_features": avg_features,
                "sample_queries": query_ids[:5]  # First 5 as samples
            }
        
        return analysis


def main():
    parser = argparse.ArgumentParser(description='Evaluate dynamic hybrid search on BEIR datasets')
    parser.add_argument('-d', '--dataset', required=True, help='Dataset name')
    parser.add_argument('-u', '--url', required=True, help='Dataset URL')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('-p', '--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('-i', '--index', required=True, help='Index name')
    parser.add_argument('-m', '--model-id', required=True, help='Neural model ID')
    parser.add_argument('-o', '--output', required=True, help='Output file for results')
    parser.add_argument('--compare', action='store_true', help='Compare with static weights')
    parser.add_argument('--static-weights', nargs='+', default=None,
                       help='Static weight configurations (format: lex,neural). If not specified with --compare, tests all combinations.')
    parser.add_argument('--use-ml', action='store_true', help='Use ML predictor instead of heuristics')
    parser.add_argument('--weight-predictor-model', type=str, default=None,
                       help='Path to weight predictor model file (e.g., esci_weight_predictor_linear.pkl)')
    parser.add_argument('--weight-step', type=float, default=0.1,
                       help='Step size for weight grid when testing all combinations (default: 0.1)')
    parser.add_argument('-q', '--max-queries', type=int, default=None, 
                       help='Maximum number of queries to evaluate (default: all)')
    
    args = parser.parse_args()
    
    # Handle dataset loading - ESCI is special case
    if args.dataset.lower() == "esci":
        # ESCI uses local data - check both possible locations
        data_path = os.path.join(os.getcwd(), "esci_data")
        if not os.path.exists(data_path):
            # Try alternative location
            data_path = os.path.join(os.getcwd(), "datasets", "esci")
        
        logger.info(f"Using local ESCI data from {data_path}")
        
        # Check if data exists
        if not os.path.exists(data_path):
            logger.error(f"ESCI data not found at {data_path}")
            logger.error("Please ensure ESCI data exists in either 'esci_data' or 'datasets/esci' folder")
            sys.exit(1)
    else:
        # Download regular BEIR dataset
        logger.info(f"Downloading dataset from {args.url}")
        out_dir = os.path.join(os.getcwd(), "datasets")
        data_path = util.download_and_unzip(args.url, out_dir)
    
    # Initialize evaluator
    evaluator = DynamicHybridSearchEvaluator(
        host=args.host,
        port=args.port,
        index_name=args.index,
        model_id=args.model_id,
        use_ml_predictor=args.use_ml,
        model_path=args.weight_predictor_model
    )
    
    # Store dataset name for search field mapping
    evaluator.dataset_name = args.dataset
    
    # Run evaluation
    if args.compare:
        # Parse static weights
        static_weights = []
        if args.static_weights:
            # Use provided weights
            for weight_str in args.static_weights:
                lex, neural = map(float, weight_str.split(','))
                static_weights.append((lex, neural))
        else:
            # Generate all combinations with given step
            logger.info(f"Generating all weight combinations with step {args.weight_step}")
            for lex in np.arange(args.weight_step, 1.0, args.weight_step):
                neural = round(1.0 - lex, 2)  # Round to avoid floating point issues
                lex = round(lex, 2)
                static_weights.append((lex, neural))
            # Add the edge cases
            static_weights.append((0.1, 0.9))
            static_weights.append((0.9, 0.1))
            logger.info(f"Testing {len(static_weights)} weight combinations")
        
        results = evaluator.compare_static_vs_dynamic(
            args.dataset,
            data_path,
            static_weights,
            max_queries=args.max_queries
        )
    else:
        results = evaluator.evaluate_dataset(
            args.dataset, 
            data_path, 
            max_queries=args.max_queries
        )
    
    # Add query analysis
    results["query_analysis"] = evaluator.analyze_query_performance()
    
    # Save results
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Results saved to {args.output}")
    
    # Print summary
    if args.compare:
        print("\n=== Comparison Summary ===")
        for comp in results["comparisons"]:
            print(f"\nStatic weights: {comp['static_weights']}")
            for metric, data in comp["improvements"].items():
                print(f"  {metric}: {data['dynamic']:.4f} vs {data['static']:.4f} "
                      f"({data['improvement_pct']:+.1f}%)")
    else:
        print("\n=== Evaluation Summary ===")
        print(f"Dataset: {results['dataset']}")
        print(f"Domain: {results['domain']}")
        print(f"Number of queries: {results['num_queries']}")
        print("\nAverage metrics:")
        for metric, value in results['average_metrics'].items():
            print(f"  {metric}: {value:.4f}")


if __name__ == "__main__":
    main()
