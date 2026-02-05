#!/usr/bin/env python3
"""
Inline LLM Judgment POC for Hybrid Search Optimization.

This script demonstrates the inline judgment approach where LLM judgments
are generated on-the-fly during hybrid search optimization experiments.
"""

import argparse
import json
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

# Add parent for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from inline_judgment import PoolBuilder, LLMJudge, CoverageAnalyzer
from inline_judgment.pool_builder import PoolConfig
from inline_judgment.llm_judge import MockLLMJudge
from utils.opensearch_client import OpenSearchClient


def load_queries(query_file: str) -> List[str]:
    """Load queries from a JSON file."""
    with open(query_file, 'r') as f:
        data = json.load(f)
        
    # Support different query file formats
    if isinstance(data, list):
        if isinstance(data[0], str):
            return data
        elif isinstance(data[0], dict):
            return [q.get('query', q.get('query_text', '')) for q in data]
    elif isinstance(data, dict):
        if 'queries' in data:
            return [q.get('query', q.get('query_text', '')) for q in data['queries']]
            
    raise ValueError(f"Unknown query file format: {query_file}")


def run_inline_judgment_poc(
    opensearch_url: str,
    index_name: str,
    model_id: str,
    neural_field: str,
    lexical_fields: List[str],
    queries: List[str],
    content_field: str = "text",
    title_field: Optional[str] = "title",
    use_mock_llm: bool = False,
    output_dir: str = "experiments/inline_judgment_results",
    verbose: bool = True
) -> Dict:
    """
    Run the inline judgment POC.
    
    Args:
        opensearch_url: OpenSearch cluster URL
        index_name: Name of the search index
        model_id: Neural model ID
        neural_field: Field with embeddings
        lexical_fields: Fields for BM25
        queries: List of queries to evaluate
        content_field: Document content field
        title_field: Document title field
        use_mock_llm: Use mock LLM for testing
        output_dir: Output directory for results
        verbose: Print progress
        
    Returns:
        Dict with experiment results
    """
    # Initialize components
    client = OpenSearchClient(opensearch_url)
    
    pool_builder = PoolBuilder(
        client=client,
        index_name=index_name,
        model_id=model_id,
        neural_field=neural_field,
        lexical_fields=lexical_fields
    )
    
    if use_mock_llm:
        llm_judge = MockLLMJudge(batch_size=20)
    else:
        llm_judge = LLMJudge(batch_size=20)
        
    analyzer = CoverageAnalyzer(k_values=[5, 10, 20, 50, 100])
    
    # Results storage
    all_query_results = {}
    all_judgments = {}
    total_docs_judged = 0
    total_tokens = 0
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Inline LLM Judgment POC")
        print(f"{'='*60}")
        print(f"Index: {index_name}")
        print(f"Queries: {len(queries)}")
        print(f"Pool configs: {len(pool_builder.configs)}")
        print(f"LLM: {'Mock' if use_mock_llm else 'Bedrock Claude'}")
        print(f"{'='*60}\n")
        
        # Cost estimate
        cost = llm_judge.estimate_cost(len(queries), 300, 300)
        print(f"Estimated LLM cost: ${cost['total_estimated_cost']:.2f}")
        print(f"Estimated API calls: {cost['num_api_calls']}")
        print()
    
    # Process each query
    for i, query in enumerate(queries):
        if verbose:
            print(f"\n[{i+1}/{len(queries)}] Processing: {query[:50]}...")
            
        # Step 1: Build pool from multiple configurations
        pooled_docs, pool_results = pool_builder.build_pool(
            query=query,
            fetch_sources=True,
            source_fields=[content_field, title_field] if title_field else [content_field]
        )
        
        pool_stats = pool_builder.get_config_stats(pool_results)
        
        if verbose:
            print(f"  Pooled {pool_stats['total_unique_docs']} unique docs "
                  f"(dedup ratio: {pool_stats['dedup_ratio']:.2f}x)")
            
        # Step 2: Generate LLM judgments for pooled documents
        judgment_batch = llm_judge.judge_documents(
            query=query,
            documents=pooled_docs,
            content_field=content_field,
            title_field=title_field
        )
        
        total_docs_judged += len(judgment_batch.judgments)
        total_tokens += judgment_batch.total_tokens
        
        if verbose:
            print(f"  Judged {len(judgment_batch.judgments)} docs "
                  f"({judgment_batch.latency_ms:.0f}ms, {judgment_batch.total_tokens} tokens)")
            if judgment_batch.errors:
                print(f"  Warnings: {len(judgment_batch.errors)} errors")
                
        # Step 3: Analyze coverage and calculate NDCG
        ndcg_results, coverage_stats = analyzer.analyze_pool_results(
            pool_results=pool_results,
            judgments=judgment_batch.judgments
        )
        
        # Find best config for this query
        best_config, best_ndcg = analyzer.find_best_config(ndcg_results, k=10)
        
        if verbose:
            print(f"  Coverage: {coverage_stats.coverage_ratio:.1%}")
            print(f"  Best config: {best_config} (NDCG@10: {best_ndcg:.4f})")
            
        # Store results
        all_query_results[query] = ndcg_results
        all_judgments[query] = judgment_batch.judgments
        
    # Aggregate results across all queries
    aggregated = analyzer.aggregate_results(all_query_results)
    
    # Find overall best configuration
    best_overall = None
    best_overall_ndcg = -1.0
    for config_name, scores in aggregated.items():
        ndcg_10 = scores.get(10, 0.0)
        if ndcg_10 > best_overall_ndcg:
            best_overall_ndcg = ndcg_10
            best_overall = config_name
            
    # Generate final report
    if verbose:
        print(f"\n{'='*60}")
        print(f"FINAL RESULTS")
        print(f"{'='*60}")
        print(f"\nTotal documents judged: {total_docs_judged}")
        print(f"Total tokens used: {total_tokens}")
        print(f"\nAggregated NDCG@10 by configuration:")
        
        sorted_configs = sorted(
            aggregated.items(),
            key=lambda x: x[1].get(10, 0),
            reverse=True
        )
        
        for config_name, scores in sorted_configs:
            print(f"  {config_name}: {scores.get(10, 0):.4f}")
            
        print(f"\n>>> Best overall config: {best_overall} <<<")
        print(f">>> NDCG@10: {best_overall_ndcg:.4f} <<<")
        
    # Prepare output
    results = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "index": index_name,
            "model_id": model_id,
            "num_queries": len(queries),
            "pool_configs": [c.name for c in pool_builder.configs]
        },
        "summary": {
            "total_docs_judged": total_docs_judged,
            "total_tokens": total_tokens,
            "best_config": best_overall,
            "best_ndcg_10": best_overall_ndcg
        },
        "aggregated_ndcg": aggregated,
        "per_query_results": {
            q: {k: v.ndcg_at_k for k, v in results.items()}
            for q, results in all_query_results.items()
        }
    }
    
    # Save results
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(
        output_dir,
        f"inline_judgment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
        
    if verbose:
        print(f"\nResults saved to: {output_file}")
        
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Inline LLM Judgment POC for Hybrid Search Optimization"
    )
    
    parser.add_argument(
        "--opensearch-url",
        default="http://localhost:9200",
        help="OpenSearch cluster URL"
    )
    parser.add_argument(
        "--index",
        required=True,
        help="Search index name"
    )
    parser.add_argument(
        "--model-id",
        required=True,
        help="Neural model ID for embeddings"
    )
    parser.add_argument(
        "--neural-field",
        default="embedding",
        help="Field containing document embeddings"
    )
    parser.add_argument(
        "--lexical-fields",
        nargs="+",
        default=["title", "text"],
        help="Fields to search with BM25"
    )
    parser.add_argument(
        "--query-file",
        help="JSON file containing queries"
    )
    parser.add_argument(
        "--queries",
        nargs="+",
        help="Queries to evaluate (alternative to --query-file)"
    )
    parser.add_argument(
        "--content-field",
        default="text",
        help="Document content field"
    )
    parser.add_argument(
        "--title-field",
        default="title",
        help="Document title field"
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use mock LLM for testing"
    )
    parser.add_argument(
        "--output-dir",
        default="experiments/inline_judgment_results",
        help="Output directory for results"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output"
    )
    
    args = parser.parse_args()
    
    # Load queries
    if args.query_file:
        queries = load_queries(args.query_file)
    elif args.queries:
        queries = args.queries
    else:
        # Default test queries
        queries = [
            "wireless bluetooth headphones",
            "laptop stand adjustable",
            "USB-C charging cable"
        ]
        print("Using default test queries")
        
    # Run POC
    results = run_inline_judgment_poc(
        opensearch_url=args.opensearch_url,
        index_name=args.index,
        model_id=args.model_id,
        neural_field=args.neural_field,
        lexical_fields=args.lexical_fields,
        queries=queries,
        content_field=args.content_field,
        title_field=args.title_field,
        use_mock_llm=args.mock,
        output_dir=args.output_dir,
        verbose=not args.quiet
    )
    
    return results


if __name__ == "__main__":
    main()
