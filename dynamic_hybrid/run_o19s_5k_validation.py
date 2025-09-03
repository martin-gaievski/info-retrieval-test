#!/usr/bin/env python3
"""
O19S 5K Scale Validation Runner

This script runs the O19S validation at the proper scale:
- 5,000 total queries (matching O19S methodology)
- 4,000 queries for training
- 1,000 queries for testing

Usage:
    python run_o19s_5k_validation.py [OPTIONS]
    
    Options:
        --host HOST           OpenSearch host (default: localhost)
        --port PORT           OpenSearch port (default: 9200)
        --index INDEX         OpenSearch index name (default: esci_products)
        --model-id MODEL_ID   Neural search model ID (default: sentence-transformers/all-MiniLM-L6-v2)
        --help               Show this help message
        
    Examples:
        python run_o19s_5k_validation.py
        python run_o19s_5k_validation.py --host my-opensearch.com --port 443
        python run_o19s_5k_validation.py --model-id my-custom-model
"""

import sys
import logging
import argparse
from pathlib import Path
import json

# Add current directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir.parent))  # Add parent directory for beir imports

from dynamic_hybrid.o19s_validation_implementation import O19SValidationFramework
from beir.hybrid.search import RetrievalOpenSearch
from beir.hybrid.evaluation import EvaluateRetrieval

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="O19S Hybrid Search Optimization Validation at 5K Scale",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --host my-opensearch.com --port 443
  %(prog)s --model-id my-custom-model
  %(prog)s --host localhost --port 9200 --index my_products --model-id sentence-transformers/all-MiniLM-L6-v2
        """
    )
    
    parser.add_argument(
        '--host',
        default='opense-clust-5yzcawchk9ld-def67170c05919a4.elb.us-east-1.amazonaws.com',
        help='OpenSearch host (default: AWS OpenSearch cluster)'
    )
    
    parser.add_argument(
        '--port',
        default='80',
        help='OpenSearch port (default: 80)'
    )
    
    parser.add_argument(
        '--index',
        default='esci-products',
        help='OpenSearch index name (default: esci-products)'
    )
    
    parser.add_argument(
        '--model-id',
        default='sentence-transformers/all-MiniLM-L6-v2',
        help='Neural search model ID (default: sentence-transformers/all-MiniLM-L6-v2)'
    )
    
    parser.add_argument(
        '--data-path',
        default='datasets/esci',
        help='Path to ESCI data files folder (default: datasets/esci)'
    )
    
    parser.add_argument(
        '--extraction-method',
        choices=['o19s', 'corpus', 'corpus_search', 'all'],
        default='all',
        help='Feature extraction method to use (default: all)'
    )
    
    parser.add_argument(
        '--total-queries',
        type=int,
        default=5000,
        help='Total number of queries to use for validation (default: 5000)'
    )
    
    parser.add_argument(
        '--train-ratio',
        type=float,
        default=0.8,
        help='Ratio of queries to use for training (default: 0.8 for 80/20 split)'
    )
    
    return parser.parse_args()

def main():
    """
    Run O19S validation at proper 5K scale.
    """
    # Parse command line arguments
    args = parse_arguments()
    
    print("="*80)
    print("O19S HYBRID SEARCH OPTIMIZATION VALIDATION")
    print("5,000 Query Scale (4K Train + 1K Test)")
    print("="*80)
    print(f"Configuration:")
    print(f"  OpenSearch Host: {args.host}")
    print(f"  OpenSearch Port: {args.port}")
    print(f"  OpenSearch Index: {args.index}")
    print(f"  Model ID: {args.model_id}")
    print(f"  Data Path: {args.data_path}")
    print("="*80)
    
    try:
        # Initialize components with configurable parameters
        logger.info("Initializing search components...")
        searcher = RetrievalOpenSearch(
            endpoint=args.host,
            port=args.port,
            index_name=args.index,
            model_id=args.model_id,
            search_method="hybrid"
        )
        
        evaluator = EvaluateRetrieval(searcher)
        
        # Create validation framework with configurable parameters
        validator = O19SValidationFramework(
            searcher, 
            evaluator, 
            data_path=args.data_path,
            total_queries=args.total_queries,
            train_ratio=args.train_ratio
        )
        
        # Run O19S-scale validation with selected extraction method
        logger.info(f"Starting O19S-scale validation ({args.total_queries} queries, {args.train_ratio:.1%} train) with extraction method: {args.extraction_method}")
        results = validator.run_o19s_scale_validation(extraction_method=args.extraction_method)
        
        # Save results
        results_file = validator.save_results(results, "o19s_5k_scale_validation_results.json")
        
        # Print comprehensive summary
        print_validation_summary(results)
        
        print(f"\n{'='*80}")
        print(f"VALIDATION COMPLETE")
        print(f"Detailed results saved to: {results_file}")
        print(f"{'='*80}")
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        print(f"\nERROR: {e}")
        sys.exit(1)

def print_validation_summary(results):
    """Print comprehensive validation summary."""
    
    print(f"\n{'='*80}")
    print("VALIDATION RESULTS SUMMARY")
    print(f"{'='*80}")
    
    # Experiment configuration
    config = results.get('experiment_config', {})
    print(f"\nDataset Configuration:")
    print(f"  Total queries: {config.get('total_queries', 'N/A')}")
    print(f"  Training queries: {config.get('train_queries', 'N/A')}")
    print(f"  Test queries: {config.get('test_queries', 'N/A')}")
    
    # Results by approach
    approaches = results.get('approaches', {})
    
    if not approaches:
        print("\nNo approach results available.")
        return
    
    # Compare approaches
    comparison_data = {}
    
    for approach_name, approach_results in approaches.items():
        if 'error' in approach_results:
            print(f"\n{approach_name.upper()} APPROACH: ERROR")
            print(f"  Error: {approach_results['error']}")
            continue
        
        print(f"\n{approach_name.upper()} APPROACH RESULTS:")
        print(f"  Training samples: {approach_results.get('training_samples', 'N/A')}")
        
        approach_data = {}
        
        for model_name, model_results in approach_results.get('models', {}).items():
            search_perf = model_results.get('search_performance', {})
            
            print(f"\n  {model_name.replace('_', ' ').title()} Model:")
            print(f"    Training time: {model_results.get('training_time', 0):.2f}s")
            print(f"    Average NDCG@10: {search_perf.get('avg_ndcg', 0):.4f}")
            print(f"    Average predicted weight: {search_perf.get('avg_predicted_weight', 0):.3f}")
            print(f"    Queries evaluated: {search_perf.get('num_queries_evaluated', 0)}")
            
            # Store for comparison
            approach_data[model_name] = {
                'ndcg': search_perf.get('avg_ndcg', 0),
                'weight': search_perf.get('avg_predicted_weight', 0),
                'training_time': model_results.get('training_time', 0)
            }
            
            # Show O19S baseline comparisons
            print(f"\n    O19S Baseline Comparisons:")
            if 'o19s_baseline' in search_perf:
                print(f"      O19S Baseline (multi_match): {search_perf['o19s_baseline']:.4f}")
            if 'best_static_hybrid' in search_perf:
                best_weight = search_perf.get('best_static_weight', 'unknown')
                print(f"      Best Static Hybrid (weight {best_weight}): {search_perf['best_static_hybrid']:.4f}")
            
            print(f"\n    Fixed Weight Baselines:")
            for key, value in search_perf.items():
                if key.startswith('fixed_'):
                    baseline_weight = key.replace('fixed_', '')
                    print(f"      Fixed weight {baseline_weight}: {value:.4f}")
            
            print(f"\n    Improvements over Baselines:")
            if 'o19s_baseline' in search_perf and search_perf['o19s_baseline'] > 0:
                improvement = ((search_perf.get('avg_ndcg', 0) - search_perf['o19s_baseline']) / search_perf['o19s_baseline']) * 100
                print(f"      Over O19S baseline: {improvement:+.2f}%")
            if 'best_static_hybrid' in search_perf and search_perf['best_static_hybrid'] > 0:
                improvement = ((search_perf.get('avg_ndcg', 0) - search_perf['best_static_hybrid']) / search_perf['best_static_hybrid']) * 100
                print(f"      Over best static hybrid: {improvement:+.2f}%")
                
            for key, value in search_perf.items():
                if key.startswith('improvement_over_fixed_'):
                    baseline_weight = key.replace('improvement_over_fixed_', '')
                    print(f"      Over fixed weight {baseline_weight}: {value:+.2f}%")
        
        comparison_data[approach_name] = approach_data
    
    # Print comparison summary
    print(f"\n{'='*80}")
    print("APPROACH COMPARISON SUMMARY")
    print(f"{'='*80}")
    
    if len(comparison_data) >= 2:
        print_approach_comparison(comparison_data)
    else:
        print("Need at least 2 approaches for comparison.")
    
    # Print O19S claims validation
    print(f"\n{'='*80}")
    print("O19S CLAIMS VALIDATION")
    print(f"{'='*80}")
    
    validate_o19s_claims(comparison_data)

def print_approach_comparison(comparison_data):
    """Print detailed comparison between approaches."""
    
    approaches = list(comparison_data.keys())
    if len(approaches) < 2:
        return
    
    print(f"\nDirect Comparison: {approaches[0].upper()} vs {approaches[1].upper()}")
    print("-" * 60)
    
    for model_name in ['linear_regression', 'random_forest']:
        if (model_name in comparison_data[approaches[0]] and 
            model_name in comparison_data[approaches[1]]):
            
            data1 = comparison_data[approaches[0]][model_name]
            data2 = comparison_data[approaches[1]][model_name]
            
            print(f"\n{model_name.replace('_', ' ').title()} Model:")
            print(f"  {approaches[0].upper()}: NDCG {data1['ndcg']:.4f}, Weight {data1['weight']:.3f}, Time {data1['training_time']:.2f}s")
            print(f"  {approaches[1].upper()}: NDCG {data2['ndcg']:.4f}, Weight {data2['weight']:.3f}, Time {data2['training_time']:.2f}s")
            
            # Calculate differences
            ndcg_diff = ((data2['ndcg'] - data1['ndcg']) / data1['ndcg'] * 100) if data1['ndcg'] > 0 else 0
            time_diff = ((data2['training_time'] - data1['training_time']) / data1['training_time'] * 100) if data1['training_time'] > 0 else 0
            
            print(f"  NDCG Difference: {ndcg_diff:+.2f}% ({approaches[1]} vs {approaches[0]})")
            print(f"  Training Time Difference: {time_diff:+.2f}% ({approaches[1]} vs {approaches[0]})")

def validate_o19s_claims(comparison_data):
    """Validate O19S claims against our results."""
    
    # O19S claims
    o19s_claims = {
        'baseline_ndcg': 0.26,
        'best_hybrid_ndcg': 0.27,
        'dynamic_linear_ndcg': 0.29,
        'dynamic_rf_ndcg': 0.29,
        'improvement_over_baseline': 11.54,  # %
        'improvement_over_best_hybrid': 7.41  # %
    }
    
    print(f"\nO19S Original Claims:")
    print(f"  Baseline NDCG: {o19s_claims['baseline_ndcg']}")
    print(f"  Best Hybrid NDCG: {o19s_claims['best_hybrid_ndcg']}")
    print(f"  Dynamic Linear NDCG: {o19s_claims['dynamic_linear_ndcg']}")
    print(f"  Dynamic RF NDCG: {o19s_claims['dynamic_rf_ndcg']}")
    print(f"  Improvement over baseline: {o19s_claims['improvement_over_baseline']:.2f}%")
    
    # Compare with our O19S approach results
    if 'o19s' in comparison_data:
        o19s_results = comparison_data['o19s']
        
        print(f"\nOur O19S Replication Results:")
        for model_name, model_data in o19s_results.items():
            model_display = model_name.replace('_', ' ').title()
            print(f"  {model_display} NDCG: {model_data['ndcg']:.4f}")
        
        # Validation assessment
        print(f"\nValidation Assessment:")
        
        if 'linear_regression' in o19s_results:
            our_linear = o19s_results['linear_regression']['ndcg']
            claimed_linear = o19s_claims['dynamic_linear_ndcg']
            linear_diff = abs(our_linear - claimed_linear) / claimed_linear * 100
            
            print(f"  Linear Regression:")
            print(f"    Our result: {our_linear:.4f}")
            print(f"    O19S claim: {claimed_linear:.4f}")
            print(f"    Difference: {linear_diff:.2f}%")
            
            if linear_diff < 10:
                print(f"    ✅ VALIDATED: Results match within 10%")
            else:
                print(f"    ❌ DISCREPANCY: Results differ by more than 10%")
        
        if 'random_forest' in o19s_results:
            our_rf = o19s_results['random_forest']['ndcg']
            claimed_rf = o19s_claims['dynamic_rf_ndcg']
            rf_diff = abs(our_rf - claimed_rf) / claimed_rf * 100
            
            print(f"  Random Forest:")
            print(f"    Our result: {our_rf:.4f}")
            print(f"    O19S claim: {claimed_rf:.4f}")
            print(f"    Difference: {rf_diff:.2f}%")
            
            if rf_diff < 10:
                print(f"    ✅ VALIDATED: Results match within 10%")
            else:
                print(f"    ❌ DISCREPANCY: Results differ by more than 10%")
    
    else:
        print(f"\n❌ O19S approach results not available for validation")
    
    # Compare with corpus-aware approach
    if 'corpus' in comparison_data:
        corpus_results = comparison_data['corpus']
        
        print(f"\nCorpus-Aware Approach Comparison:")
        for model_name, model_data in corpus_results.items():
            model_display = model_name.replace('_', ' ').title()
            print(f"  {model_display} NDCG: {model_data['ndcg']:.4f}")
            
            # Compare with O19S claims
            if model_name == 'linear_regression':
                improvement = ((model_data['ndcg'] - o19s_claims['baseline_ndcg']) / o19s_claims['baseline_ndcg']) * 100
                print(f"    Improvement over O19S baseline: {improvement:+.2f}%")

if __name__ == "__main__":
    main()
