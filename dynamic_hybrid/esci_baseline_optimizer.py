#!/usr/bin/env python3
"""
ESCI Baseline Hybrid Weight Optimizer for OpenSearch
Finds optimal global weights for combining lexical and semantic search.
This creates a baseline for comparison with dynamic (per-query) optimization.
Version: Working version that handles query_string only format.
"""

import os
import json
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
import logging
from datetime import datetime
import argparse
from opensearchpy import OpenSearch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ESCIBaselineOptimizer:
    """Baseline optimizer for ESCI hybrid search using global weights"""
    
    def __init__(self, 
                 host: str,
                 port: int,
                 index_name: str,
                 model_id: str,
                 queries_file: str,
                 ratings_file: str):
        """
        Initialize the baseline optimizer.
        
        Args:
            host: OpenSearch host
            port: OpenSearch port
            index_name: Name of the index
            model_id: Model ID for neural search
            queries_file: Path to queries CSV
            ratings_file: Path to ratings CSV/TSV
        """
        # Initialize OpenSearch client (HTTP, no auth)
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        self.index_name = index_name
        self.model_id = model_id
        self.queries_file = queries_file
        self.ratings_file = ratings_file
        
        # Load data
        self._load_data()
    
    def _load_data(self):
        """Load queries and ratings data"""
        # Load queries - only has query_string column
        self.queries_df = pd.read_csv(self.queries_file)
        logger.info(f"Loaded {len(self.queries_df)} queries from {self.queries_file}")
        logger.info(f"Query columns: {self.queries_df.columns.tolist()}")
        
        # Load ratings - tab-separated file with no headers
        # First column is the query text (not ID), then product_id, relevance, query_num
        self.ratings_df = pd.read_csv(
            self.ratings_file, 
            sep='\t', 
            header=None,  # No headers in file
            names=['query_text', 'product_id', 'relevance', 'query_num'],  # First column is query text
            on_bad_lines='skip'
        )
        
        logger.info(f"Loaded {len(self.ratings_df)} ratings from {self.ratings_file}")
        logger.info(f"Ratings columns: {self.ratings_df.columns.tolist()}")
        
        # CRITICAL: Map integer ratings to ESCI/Golden Standard relevance scores
        # The ratings file contains integers 0,1,2,3 but the golden standard expects:
        # EXACT, SUBSTITUTE, COMPLEMENT, IRRELEVANT mapped to 1.0, 0.7, 0.3, 0.0
        # Based on ESCI standard mapping:
        RATING_TO_RELEVANCE = {
            3.0: 1.0,  # EXACT (perfect match)
            2.0: 0.7,  # SUBSTITUTE (good alternative)
            1.0: 0.3,  # COMPLEMENT (somewhat related)
            0.0: 0.0   # IRRELEVANT (not related)
        }
        
        # Create relevance mapping per query text
        self.relevance_map = {}
        for _, row in self.ratings_df.iterrows():
            try:
                query_text = str(row['query_text']).strip()  # Use query text as key
                product_id = str(row['product_id'])
                integer_rating = float(row['relevance'])  # This is 0, 1, 2, or 3
                
                # Map integer rating to golden standard relevance score
                relevance = RATING_TO_RELEVANCE.get(integer_rating, 0.0)
                
                if query_text not in self.relevance_map:
                    self.relevance_map[query_text] = {}
                self.relevance_map[query_text][product_id] = relevance
            except Exception as e:
                logger.debug(f"Skipping malformed row: {e}")
                continue
        
        # Convert query strings to string and strip whitespace for consistent matching
        self.queries_df['query_string'] = self.queries_df['query_string'].astype(str).str.strip()
        
        # Filter queries that have relevance judgments
        queries_with_ratings = set(self.relevance_map.keys())
        self.queries_df = self.queries_df[self.queries_df['query_string'].isin(queries_with_ratings)]
        logger.info(f"Using {len(self.queries_df)} queries with relevance judgments")
        
        # Log sample query to verify matching
        if len(self.queries_df) > 0:
            sample_query = self.queries_df.iloc[0]['query_string']
            logger.info(f"Sample query: '{sample_query}'")
            if sample_query in self.relevance_map:
                logger.info(f"Sample query has {len(self.relevance_map[sample_query])} relevance judgments")
    
    def hybrid_search(self, 
                     query_text: str,
                     lexical_weight: float,
                     neural_weight: float,
                     normalization_technique: str = "min_max",
                     combination_technique: str = "arithmetic_mean",
                     size: int = 10) -> List[Dict]:
        """
        Perform hybrid search with given weights and techniques.
        
        Args:
            query_text: Query text
            lexical_weight: Weight for lexical search
            neural_weight: Weight for neural search
            normalization_technique: Normalization technique ("min_max" or "l2")
            combination_technique: Combination technique ("arithmetic_mean", "geometric_mean", or "harmonic_mean")
            size: Number of results to return
            
        Returns:
            List of search results
        """
        # Normalize weights
        total_weight = lexical_weight + neural_weight
        if total_weight > 0:
            norm_lexical = lexical_weight / total_weight
            norm_neural = neural_weight / total_weight
        else:
            norm_lexical = 0.5
            norm_neural = 0.5
        
        # Build hybrid query - neural first as per OpenSearch convention
        hybrid_query = {
            "size": size,  # Fetch 100 results for proper normalization, then use top 10 for NDCG
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "neural": {
                                "title_embedding": {
                                    "query_text": query_text,
                                    "model_id": self.model_id,
                                    "k": 200
                                }
                            }
                        },
                        {
                            "multi_match": {
                                "query": query_text,
                                "type":       "best_fields",
                                "fields":     [
                                    "product_id^100",
                                    "product_bullet_point^3",
                                    "product_color^2",
                                    "product_brand^5",
                                    "product_description",
                                    "product_title^10"
                                ],
                                "operator":   "and"
                            }
                        }
                    ]
                }
            },
            "_source": ["product_id", "product_title"],
            "search_pipeline": {
                "description": "Dynamic post processor for hybrid search",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {
                                "technique": normalization_technique  
                            },
                            "combination": {
                                "technique": combination_technique,
                                "parameters": {
                                    "weights": [neural_weight,lexical_weight]
                                }
                            }
                        }
                    }
                ]
            }
        }
        
        try:
            response = self.client.search(index=self.index_name, body=hybrid_query)
            results = []
            for hit in response['hits']['hits']:
                results.append({
                    'product_id': hit['_source']['product_id'],
                    'title': hit['_source'].get('product_title', ''),
                    'score': hit['_score']
                })
            return results
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []
    
    def calculate_ndcg(self, ranked_ids: List[str], relevance_scores: Dict[str, float], k: int = 10) -> float:
        """
        Calculate NDCG@k for a ranked list using the golden standard formula.
        
        This implementation matches the golden standard Java tool exactly:
        - Uses raw relevance scores directly (0.0, 0.3, 0.7, 1.0) without mapping to integer ratings
        - Uses log2(i + 2) for the discount factor (where i starts from 0)
        - Uses (2^relevance - 1) for the gain, with raw relevance values
        
        This is different from traditional NDCG that maps relevance to integer ratings (0,1,2,3).
        
        Args:
            ranked_ids: List of document IDs in ranked order
            relevance_scores: Dictionary mapping doc ID to relevance score (0.0, 0.3, 0.7, 1.0)
            k: Cutoff for NDCG calculation
            
        Returns:
            NDCG@k score
        """
        # Calculate DCG using exponential gain formula with raw relevance scores
        # This matches the golden standard Java implementation exactly:
        # dcg += (Math.pow(2, relevance) - 1) / (Math.log(i + 2) / Math.log(2))
        dcg = 0.0
        size = min(k, len(ranked_ids))
        
        for i in range(size):
            doc_id = str(ranked_ids[i])  # Convert to string for matching
            if doc_id in relevance_scores:
                relevance = relevance_scores[doc_id]  # Use raw score (0.0, 0.3, 0.7, 1.0) directly
                # Golden standard formula: (2^relevance - 1) / log2(position + 2)
                # For relevance=0.7: gain = 2^0.7 - 1 = 0.6245 (NOT 2^2 - 1 = 3)
                # For relevance=1.0: gain = 2^1.0 - 1 = 1.0000 (NOT 2^3 - 1 = 7)
                dcg += (np.power(2, relevance) - 1) / np.log2(i + 2)
        
        # Calculate IDCG using ALL relevance scores (not just retrieved items)
        # This matches the Java implementation which uses all judgmentScores.values()
        all_relevance_scores = list(relevance_scores.values())
        all_relevance_scores.sort(reverse=True)
        
        # Truncate to k for IDCG calculation
        ideal_scores = all_relevance_scores[:k]
        
        idcg = 0.0
        for i in range(len(ideal_scores)):
            # Exponential gain formula for ideal ranking with raw relevance scores
            idcg += (np.power(2, ideal_scores[i]) - 1) / np.log2(i + 2)
        
        # Return NDCG (rounded to match golden standard precision)
        ndcg = dcg / idcg if idcg > 0 else 0.0
        return round(ndcg, 4)  # Round to 4 decimal places to match golden standard
    
    def evaluate_weights(self, 
                         lexical_weight: float, 
                         neural_weight: float,
                         normalization_technique: str = "min_max",
                         combination_technique: str = "arithmetic_mean",
                         sample_size: Optional[int] = None) -> Dict[str, float]:
        """
        Evaluate a specific weight configuration on all queries.
        
        Args:
            lexical_weight: Weight for lexical search
            neural_weight: Weight for neural search
            normalization_technique: Normalization technique
            combination_technique: Combination technique
            sample_size: Optional sample size for evaluation
            
        Returns:
            Dictionary of evaluation metrics
        """
        ndcg_scores = []
        
        # Sample queries if requested - use first N queries sequentially
        # to match golden standard tool behavior
        queries_to_eval = self.queries_df
        if sample_size and sample_size < len(self.queries_df):
            queries_to_eval = self.queries_df.head(sample_size)
        
        for _, query_row in queries_to_eval.iterrows():
            # Use query_string column (the only column we have)
            query_text = query_row['query_string']
            
            # Skip if no relevance judgments
            if query_text not in self.relevance_map:
                continue
            
            # Perform hybrid search
            results = self.hybrid_search(
                query_text=query_text,
                lexical_weight=lexical_weight,
                neural_weight=neural_weight,
                normalization_technique=normalization_technique,
                combination_technique=combination_technique,
                size=10
            )
            
            # Get ranked product IDs
            ranked_ids = [r['product_id'] for r in results]
            
            # Get relevance scores for this query
            relevance_scores = self.relevance_map[query_text]
            
            # Calculate NDCG
            ndcg = self.calculate_ndcg(ranked_ids, relevance_scores, k=10)
            ndcg_scores.append(ndcg)
        
        return {
            'ndcg@10': np.mean(ndcg_scores) if ndcg_scores else 0.0,
            'num_queries': len(ndcg_scores),
            'std_ndcg': np.std(ndcg_scores) if ndcg_scores else 0.0
        }
    
    def grid_search_with_techniques(self,
                                    weight_range: Tuple[float, float] = (0.0, 1.0),
                                    #weight_range: Tuple[float, float] = (0.5, 0.6),
                                    step_size: float = 0.1,
                                    normalization_techniques: List[str] = ["min_max", "l2"],
                                    #normalization_techniques: List[str] = ["min_max"],
                                    combination_techniques: List[str] = ["arithmetic_mean", "geometric_mean", "harmonic_mean"],
                                    #combination_techniques: List[str] = ["arithmetic_mean"],
                                    sample_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform grid search across weights and techniques.
        
        Args:
            weight_range: Range of weights to test (min, max)
            step_size: Step size for grid search
            normalization_techniques: List of normalization techniques to test
            combination_techniques: List of combination techniques to test
            sample_size: Optional sample size for faster evaluation
            
        Returns:
            Dictionary with optimal configuration and results
        """
        results = []
        weights = np.arange(weight_range[0], weight_range[1] + step_size, step_size)
        
        total_combinations = len(weights) * len(normalization_techniques) * len(combination_techniques)
        logger.info(f"Testing {total_combinations} combinations (weights × normalization × combination)...")
        
        # Test each combination
        for norm_tech in normalization_techniques:
            for comb_tech in combination_techniques:
                logger.info(f"\nTesting {norm_tech} + {comb_tech}...")
                
                for weight in weights:
                    lexical_weight = weight
                    neural_weight = 1.0 - weight
                    
                    # Evaluate this configuration
                    metrics = self.evaluate_weights(
                        lexical_weight, 
                        neural_weight,
                        normalization_technique=norm_tech,
                        combination_technique=comb_tech,
                        sample_size=sample_size
                    )
                    
                    results.append({
                        'lexical_weight': lexical_weight,
                        'neural_weight': neural_weight,
                        'normalization': norm_tech,
                        'combination': comb_tech,
                        'ndcg@10': metrics['ndcg@10'],
                        'std_ndcg': metrics['std_ndcg'],
                        'num_queries': metrics['num_queries']
                    })
                    
                    logger.info(f"  L:{lexical_weight:.2f}, N:{neural_weight:.2f} -> NDCG:{metrics['ndcg@10']:.4f}")
        
        # Find best configuration
        results_df = pd.DataFrame(results)
        best_idx = results_df['ndcg@10'].idxmax()
        best_config = results_df.iloc[best_idx]
        
        # Get best results per technique combination
        technique_summary = results_df.groupby(['normalization', 'combination'])['ndcg@10'].max().reset_index()
        
        return {
            'best_config': best_config.to_dict(),
            'all_results': results_df,
            'technique_summary': technique_summary,
            'optimization_method': 'grid_search_with_techniques',
            'dataset': 'ESCI-products',
            'timestamp': datetime.now().isoformat()
        }
    
    def save_results(self, optimization_results: Dict[str, Any], output_file: str):
        """
        Save optimization results to JSON file.
        
        Args:
            optimization_results: Results from optimization
            output_file: Path to save results
        """
        # Prepare results for saving
        save_data = {
            'baseline_configuration': {
                'lexical_weight': optimization_results['best_config']['lexical_weight'],
                'neural_weight': optimization_results['best_config']['neural_weight'],
                'normalization_technique': optimization_results['best_config'].get('normalization', 'min_max'),
                'combination_technique': optimization_results['best_config'].get('combination', 'arithmetic_mean')
            },
            'baseline_metrics': {
                'ndcg@10': optimization_results['best_config']['ndcg@10'],
                'num_queries': optimization_results['best_config']['num_queries'],
                'std_ndcg': optimization_results['best_config'].get('std_ndcg', 0.0)
            },
            'experiment_details': {
                'dataset': 'ESCI-products',
                'index_name': self.index_name,
                'model_id': self.model_id,
                'total_queries': len(self.queries_df),
                'queries_with_judgments': len(self.relevance_map),
                'optimization_method': optimization_results['optimization_method'],
                'timestamp': optimization_results['timestamp']
            }
        }
        
        # Add technique summary if available
        if 'technique_summary' in optimization_results:
            save_data['technique_summary'] = optimization_results['technique_summary'].to_dict('records')
        
        # Add all results if available
        if 'all_results' in optimization_results:
            save_data['all_weight_configurations'] = optimization_results['all_results'].to_dict('records')
        
        with open(output_file, 'w') as f:
            json.dump(save_data, f, indent=2)
        
        logger.info(f"Saved baseline results to {output_file}")


def main():
    """Main function for baseline optimization"""
    parser = argparse.ArgumentParser(description='ESCI Baseline Hybrid Search Weight Optimizer')
    
    # OpenSearch connection parameters
    parser.add_argument('--host', type=str, default='localhost',
                       help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200,
                       help='OpenSearch port')
    
    # Index and model parameters
    parser.add_argument('--index-name', type=str, default='esci-products',
                       help='Name of the ESCI index')
    parser.add_argument('--model-id', type=str, required=True,
                       help='Model ID for neural search')
    
    # Data files
    parser.add_argument('--queries-file', type=str, required=True,
                       help='Path to queries CSV file')
    parser.add_argument('--ratings-file', type=str, required=True,
                       help='Path to ratings CSV/TSV file')
    
    # Optimization parameters
    parser.add_argument('--step-size', type=float, default=0.1,
                       help='Step size for grid search (0.1 = test 11 weights)')
    parser.add_argument('--sample-size', type=int, default=None,
                       help='Sample size for faster evaluation (None = use all queries)')
    
    # Technique parameters
    parser.add_argument('--normalization-techniques', type=str, nargs='+',
                       default=['min_max', 'l2'],
                       choices=['min_max', 'l2'],
                       help='Normalization techniques to test')
    parser.add_argument('--combination-techniques', type=str, nargs='+',
                       default=['arithmetic_mean', 'geometric_mean', 'harmonic_mean'],
                       choices=['arithmetic_mean', 'geometric_mean', 'harmonic_mean'],
                       help='Combination techniques to test')
    
    # Output parameters
    parser.add_argument('--output-file', type=str, default='esci_baseline_results.json',
                       help='Output file for baseline results')
    
    args = parser.parse_args()
    
    try:
        # Initialize optimizer
        optimizer = ESCIBaselineOptimizer(
            host=args.host,
            port=args.port,
            index_name=args.index_name,
            model_id=args.model_id,
            queries_file=args.queries_file,
            ratings_file=args.ratings_file
        )
        
        # Run optimization with all technique combinations
        logger.info("Running grid search optimization with multiple techniques...")
        results = optimizer.grid_search_with_techniques(
            step_size=args.step_size,
            normalization_techniques=args.normalization_techniques,
            combination_techniques=args.combination_techniques,
            sample_size=args.sample_size
        )
        
        # Save results
        optimizer.save_results(results, args.output_file)
        
        # Print summary
        print("\n" + "="*70)
        print("ESCI BASELINE OPTIMIZATION RESULTS")
        print("="*70)
        print(f"Optimal Configuration:")
        print(f"  Lexical Weight:     {results['best_config']['lexical_weight']:.3f}")
        print(f"  Neural Weight:      {results['best_config']['neural_weight']:.3f}")
        print(f"  Normalization:      {results['best_config']['normalization']}")
        print(f"  Combination:        {results['best_config']['combination']}")
        print(f"  Baseline NDCG@10:   {results['best_config']['ndcg@10']:.4f}")
        print(f"  Number of Queries:  {results['best_config']['num_queries']}")
        print("="*70)
        
        # Print technique summary
        if 'technique_summary' in results:
            print("\nTechnique Performance Summary:")
            print("-"*50)
            for _, row in results['technique_summary'].iterrows():
                print(f"{row['normalization']:10s} + {row['combination']:15s}: {row['ndcg@10']:.4f}")
        
        print(f"\nResults saved to: {args.output_file}")
        
    except Exception as e:
        logger.error(f"Error in baseline optimization: {e}")
        raise


if __name__ == "__main__":
    main()
