#!/usr/bin/env python3
"""
Integrated IR Experiment Runner
Combines OpenSearch automation with BEIR evaluation framework
"""

import json
import logging
import argparse
import sys
import os
from typing import Dict, Optional
from opensearch_experiment_automation import (
    OpenSearchExperimentManager,
    load_config_from_file
)
from beir import util, LoggingHandler
from beir.datasets.data_loader import GenericDataLoader
from beir.hybrid.evaluation import EvaluateRetrieval
from beir.hybrid.search import RetrievalOpenSearch
from beir.hybrid.data_ingestor import OpenSearchDataIngestor

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[LoggingHandler()]
)
logger = logging.getLogger(__name__)


class IRExperimentRunner:
    """Runs complete IR experiments with OpenSearch"""
    
    def __init__(self, config_file: str):
        """Initialize experiment runner"""
        self.config = load_config_from_file(config_file)
        self.manager = OpenSearchExperimentManager(self.config)
        self.setup_results = None
        
    def setup_infrastructure(self) -> Dict:
        """Set up OpenSearch infrastructure"""
        logger.info("Setting up OpenSearch infrastructure...")
        self.setup_results = self.manager.setup_experiment()
        
        if self.setup_results['status'] != 'success':
            raise Exception(f"Infrastructure setup failed: {self.setup_results.get('error')}")
            
        return self.setup_results
    
    def load_dataset(self, dataset_name: str, data_path: Optional[str] = None) -> tuple:
        """Load BEIR dataset"""
        if not data_path:
            # Download dataset if not provided
            url = f"https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{dataset_name}.zip"
            out_dir = os.path.join(os.path.dirname(__file__), "datasets")
            data_path = util.download_and_unzip(url, out_dir)
        
        logger.info(f"Loading dataset from {data_path}")
        corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split="test")
        
        return corpus, queries, qrels
    
    def ingest_data(self, corpus: Dict, batch_size: int = 400) -> None:
        """Ingest corpus data into OpenSearch index"""
        logger.info(f"Ingesting {len(corpus)} documents into index {self.config.index.name}")
        
        ingestor = OpenSearchDataIngestor(
            endpoint=self.config.opensearch.host,
            port=str(self.config.opensearch.port)
        )
        ingestor.bulk_size = batch_size
        ingestor.ingest(corpus, index=self.config.index.name)
        
        logger.info("Data ingestion completed")
    
    def evaluate(self, corpus: Dict, queries: Dict, qrels: Dict,
                search_methods: list = ['bm25', 'neural', 'hybrid'],
                k_values: list = [5, 10, 100],
                num_runs: int = 1) -> Dict:
        """Run evaluation with specified search methods"""
        
        results = {}
        model_id = self.setup_results.get('model_id', self.config.reuse_model_id)
        pipeline_name = self.setup_results.get('pipeline', self.config.reuse_pipeline_name)
        
        for method in search_methods:
            logger.info(f"Evaluating search method: {method}")
            
            os_retrieval = RetrievalOpenSearch(
                endpoint=self.config.opensearch.host,
                port=str(self.config.opensearch.port),
                index_name=self.config.index.name,
                model_id=model_id,
                search_method=method,
                pipeline_name=pipeline_name if method == 'hybrid' else None
            )
            
            retriever = EvaluateRetrieval(os_retrieval, k_values)
            
            # Run evaluation
            if method == 'bm25':
                search_results = os_retrieval.search_bm25(
                    corpus, queries, top_k=max(k_values)
                )
            else:
                all_took_times = []
                for run in range(num_runs):
                    search_results, took_time = os_retrieval.search_vector(
                        corpus, queries, 
                        top_k=max(k_values),
                        result_size=max(k_values)
                    )
                    all_took_times.append(took_time)
                
                if num_runs > 1:
                    retriever.evaluate_time(all_took_times)
            
            # Calculate metrics
            ndcg, _map, recall, precision = retriever.evaluate(qrels, search_results, k_values)
            
            results[method] = {
                'ndcg': ndcg,
                'map': _map,
                'recall': recall,
                'precision': precision
            }
            
            logger.info(f"Results for {method}:")
            logger.info(f"  NDCG: {ndcg}")
            logger.info(f"  MAP: {_map}")
            logger.info(f"  Recall: {recall}")
            logger.info(f"  Precision: {precision}")
        
        return results
    
    def save_results(self, results: Dict, output_file: str) -> None:
        """Save evaluation results to file"""
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to {output_file}")
    
    def cleanup(self, delete_index: bool = False,
                delete_model: bool = False,
                delete_pipeline: bool = False) -> None:
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        self.manager.cleanup_resources(
            delete_index=delete_index,
            delete_model=delete_model,
            delete_pipeline=delete_pipeline
        )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Run complete IR experiment with OpenSearch'
    )
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to experiment configuration JSON file'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        help='Dataset name (e.g., nfcorpus, trec-covid, arguana, fiqa)'
    )
    parser.add_argument(
        '--data-path',
        type=str,
        help='Path to dataset (will download if not provided)'
    )
    parser.add_argument(
        '--methods',
        type=str,
        default='bm25,neural,hybrid',
        help='Comma-separated list of search methods to evaluate'
    )
    parser.add_argument(
        '--k-values',
        type=str,
        default='5,10,100',
        help='Comma-separated list of k values for evaluation'
    )
    parser.add_argument(
        '--num-runs',
        type=int,
        default=1,
        help='Number of runs for timing evaluation'
    )
    parser.add_argument(
        '--skip-setup',
        action='store_true',
        help='Skip infrastructure setup (assume already configured)'
    )
    parser.add_argument(
        '--skip-ingest',
        action='store_true',
        help='Skip data ingestion (assume data already in index)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='experiment_results.json',
        help='Output file for results'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up resources after experiment'
    )
    parser.add_argument(
        '--cleanup-all',
        action='store_true',
        help='Delete all resources (index, model, pipeline) during cleanup'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    
    # Parse methods and k-values
    methods = args.methods.split(',')
    k_values = [int(k) for k in args.k_values.split(',')]
    
    try:
        # Initialize runner
        runner = IRExperimentRunner(args.config)
        
        # Setup infrastructure
        if not args.skip_setup:
            setup_results = runner.setup_infrastructure()
            print(f"Infrastructure setup complete: {json.dumps(setup_results, indent=2)}")
        
        # Load dataset
        corpus, queries, qrels = runner.load_dataset(args.dataset, args.data_path)
        logger.info(f"Dataset loaded: {len(corpus)} documents, {len(queries)} queries")
        
        # Ingest data
        if not args.skip_ingest:
            runner.ingest_data(corpus)
        
        # Run evaluation
        results = runner.evaluate(
            corpus, queries, qrels,
            search_methods=methods,
            k_values=k_values,
            num_runs=args.num_runs
        )
        
        # Add metadata to results
        final_results = {
            'experiment': runner.config.name,
            'dataset': args.dataset,
            'index': runner.config.index.name,
            'methods': methods,
            'k_values': k_values,
            'num_runs': args.num_runs,
            'results': results
        }
        
        # Save results
        runner.save_results(final_results, args.output)
        
        # Print summary
        print("\n" + "="*50)
        print("EXPERIMENT SUMMARY")
        print("="*50)
        for method, metrics in results.items():
            print(f"\n{method.upper()}:")
            for k in k_values:
                print(f"  NDCG@{k}: {metrics['ndcg'].get(f'NDCG@{k}', 'N/A'):.4f}")
        
        # Cleanup if requested
        if args.cleanup:
            runner.cleanup(
                delete_index=args.cleanup_all,
                delete_model=args.cleanup_all,
                delete_pipeline=args.cleanup_all
            )
        
        logger.info("Experiment completed successfully")
        
    except Exception as e:
        logger.error(f"Experiment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
