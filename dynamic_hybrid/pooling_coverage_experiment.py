#!/usr/bin/env python3
"""
Pooling Coverage Validation Experiments (E1 + E2)

Validates two key assumptions from the Auto-Fill Judgment RFC:

E1 - Pool Coverage Measurement:
  Hypothesis: 5 representative pool searches at weights [0.0, 0.25, 0.5, 0.75, 1.0]
  capture ≥85% of unique documents surfaced by the full 66-variant grid search.

E2 - Metric Sensitivity to Coverage Gaps:
  Hypothesis: The optimal weight identified by grid search is stable (±0.1) when
  judgment coverage drops from 100% to 85%.

Prerequisites:
  - ESCI index on OpenSearch cluster with text + knn_vector fields
  - Existing LLM rating cache from esci_weight_grid_search.py
  - opensearch-py or requests library

Usage:
    python dynamic_hybrid/pooling_coverage_experiment.py \
        --host <opensearch_host> \
        --port 80 \
        --embedding-model-id <model_id> \
        --llm-cache-file esci_llm_cache_100q_gpt35turbo.json \
        --num-queries 100

No LLM calls required — uses existing cached ratings and search queries only.
"""

import argparse
import json
import sys
import os
import random
import numpy as np
from collections import defaultdict
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass, asdict, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.utils.opensearch_client import OpenSearchClient
from dynamic_hybrid.esci_weight_grid_search import (
    load_esci_queries, load_esci_qrels,
    HybridSearcher, LLMJudge, ESCIQuery,
    compute_ndcg, check_index_exists, get_index_doc_count
)


# ============================================================================
# GRID CONFIGURATIONS (matches HybridOptimizerExperimentProcessor defaults)
# ============================================================================

# Full grid: 2 normalizations × 3 combinations × 11 weights = 66 variants
NORMALIZATIONS = ["min_max", "l2"]
COMBINATIONS = ["arithmetic_mean", "geometric_mean", "harmonic_mean"]
WEIGHT_STEPS = [round(w * 0.1, 1) for w in range(11)]  # 0.0 to 1.0

# Pool configurations to test
POOL_CONFIGS = {
    3: [0.0, 0.5, 1.0],
    5: [0.0, 0.25, 0.5, 0.75, 1.0],
    7: [0.0, 0.15, 0.3, 0.5, 0.7, 0.85, 1.0],
}

# Pool uses fixed normalization/combination (per RFC design)
POOL_NORMALIZATION = "min_max"
POOL_COMBINATION = "arithmetic_mean"


# ============================================================================
# EXPERIMENT E1: POOL COVERAGE MEASUREMENT
# ============================================================================

@dataclass
class CoverageResult:
    """Coverage results for a single query."""
    query_id: str
    grid_doc_count: int
    pool3_doc_count: int
    pool5_doc_count: int
    pool7_doc_count: int
    pool3_coverage: float
    pool5_coverage: float
    pool7_coverage: float


def execute_hybrid_search(
    searcher: HybridSearcher,
    query_text: str,
    neural_weight: float,
    normalization: str,
    combination: str,
    size: int = 10
) -> List[str]:
    """Execute a single hybrid search variant and return document IDs.
    
    For non-default normalization/combination, builds custom pipeline.
    For pure lexical (0.0) or pure neural (1.0), uses dedicated search.
    """
    if neural_weight == 0.0:
        results = searcher._pure_lexical_search(query_text, size)
        return [doc_id for doc_id, _ in results]
    elif neural_weight == 1.0:
        results = searcher._pure_neural_search(query_text, size)
        return [doc_id for doc_id, _ in results]
    
    lexical_weight = round(1.0 - neural_weight, 1)
    
    query_body = {
        "size": size,
        "query": {
            "hybrid": {
                "queries": [
                    {
                        "multi_match": {
                            "query": query_text,
                            "fields": searcher.lexical_fields,
                            "type": "best_fields"
                        }
                    },
                    {
                        "neural": {
                            searcher.neural_field: {
                                "query_text": query_text,
                                "model_id": searcher.embedding_model_id,
                                "k": size
                            }
                        }
                    }
                ]
            }
        },
        "search_pipeline": {
            "phase_results_processors": [{
                "normalization-processor": {
                    "normalization": {"technique": normalization},
                    "combination": {
                        "technique": combination,
                        "parameters": {
                            "weights": [lexical_weight, neural_weight]
                        }
                    }
                }
            }]
        }
    }
    
    try:
        response = searcher.client.session.post(
            f"{searcher.client.base_url}/{searcher.index_name}/_search",
            json=query_body,
            timeout=(30, 120)
        )
        if response.status_code == 200:
            results = response.json()
            return [hit['_id'] for hit in results.get('hits', {}).get('hits', [])]
        else:
            return []
    except Exception as e:
        print(f"  [ERROR] Search failed: {e}")
        return []


def run_experiment_e1(
    searcher: HybridSearcher,
    queries: Dict[str, ESCIQuery],
    query_ids: List[str],
    size: int = 10
) -> List[CoverageResult]:
    """
    E1: Pool Coverage Measurement
    
    For each query, execute the full 66-variant grid and each pool configuration.
    Measure what percentage of the grid's document set is captured by each pool.
    """
    print("\n" + "=" * 80)
    print("EXPERIMENT E1: POOL COVERAGE MEASUREMENT")
    print("=" * 80)
    print(f"Queries: {len(query_ids)}, Size: {size}")
    print(f"Full grid: {len(NORMALIZATIONS)} norm × {len(COMBINATIONS)} comb × {len(WEIGHT_STEPS)} weights = "
          f"{len(NORMALIZATIONS) * len(COMBINATIONS) * len(WEIGHT_STEPS)} variants")
    print(f"Pool configs: 3={POOL_CONFIGS[3]}, 5={POOL_CONFIGS[5]}, 7={POOL_CONFIGS[7]}")
    print()
    
    results = []
    
    for i, qid in enumerate(query_ids):
        query_text = queries[qid].text
        
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  Processing query {i+1}/{len(query_ids)}: '{query_text[:50]}...'")
        
        # --- Full grid search: collect all unique doc IDs across 66 variants ---
        grid_docs: Set[str] = set()
        for norm in NORMALIZATIONS:
            for comb in COMBINATIONS:
                for weight in WEIGHT_STEPS:
                    doc_ids = execute_hybrid_search(
                        searcher, query_text, weight, norm, comb, size
                    )
                    grid_docs.update(doc_ids)
        
        # --- Pool searches: collect unique doc IDs for each pool size ---
        pool_docs = {}
        for pool_size, pool_weights in POOL_CONFIGS.items():
            docs: Set[str] = set()
            for weight in pool_weights:
                doc_ids = execute_hybrid_search(
                    searcher, query_text, weight,
                    POOL_NORMALIZATION, POOL_COMBINATION, size
                )
                docs.update(doc_ids)
            pool_docs[pool_size] = docs
        
        # --- Compute coverage ---
        grid_count = len(grid_docs)
        if grid_count == 0:
            results.append(CoverageResult(
                query_id=qid, grid_doc_count=0,
                pool3_doc_count=0, pool5_doc_count=0, pool7_doc_count=0,
                pool3_coverage=0, pool5_coverage=0, pool7_coverage=0
            ))
            continue
        
        pool3_overlap = len(pool_docs[3] & grid_docs)
        pool5_overlap = len(pool_docs[5] & grid_docs)
        pool7_overlap = len(pool_docs[7] & grid_docs)
        
        results.append(CoverageResult(
            query_id=qid,
            grid_doc_count=grid_count,
            pool3_doc_count=len(pool_docs[3]),
            pool5_doc_count=len(pool_docs[5]),
            pool7_doc_count=len(pool_docs[7]),
            pool3_coverage=pool3_overlap / grid_count,
            pool5_coverage=pool5_overlap / grid_count,
            pool7_coverage=pool7_overlap / grid_count,
        ))
    
    return results


def print_e1_results(results: List[CoverageResult]):
    """Print E1 summary statistics."""
    print("\n" + "=" * 80)
    print("E1 RESULTS: POOL COVERAGE MEASUREMENT")
    print("=" * 80)
    
    for pool_size in [3, 5, 7]:
        attr = f"pool{pool_size}_coverage"
        coverages = [getattr(r, attr) for r in results if r.grid_doc_count > 0]
        
        if not coverages:
            continue
        
        mean_cov = np.mean(coverages)
        median_cov = np.median(coverages)
        min_cov = np.min(coverages)
        max_cov = np.max(coverages)
        std_cov = np.std(coverages)
        p10 = np.percentile(coverages, 10)
        pct_above_80 = sum(1 for c in coverages if c >= 0.80) / len(coverages) * 100
        pct_above_90 = sum(1 for c in coverages if c >= 0.90) / len(coverages) * 100
        
        print(f"\n  Pool Size: {pool_size} configs (weights: {POOL_CONFIGS[pool_size]})")
        print(f"  {'Mean Coverage':>20}: {mean_cov:.4f} ({mean_cov*100:.1f}%)")
        print(f"  {'Median Coverage':>20}: {median_cov:.4f} ({median_cov*100:.1f}%)")
        print(f"  {'Min Coverage':>20}: {min_cov:.4f} ({min_cov*100:.1f}%)")
        print(f"  {'Max Coverage':>20}: {max_cov:.4f} ({max_cov*100:.1f}%)")
        print(f"  {'Std Dev':>20}: {std_cov:.4f}")
        print(f"  {'P10 Coverage':>20}: {p10:.4f} ({p10*100:.1f}%)")
        print(f"  {'% Queries ≥ 80%':>20}: {pct_above_80:.1f}%")
        print(f"  {'% Queries ≥ 90%':>20}: {pct_above_90:.1f}%")
    
    # Per-query details (first 10)
    print(f"\n  Per-Query Details (first 10 of {len(results)}):")
    print(f"  {'Query':>8} | {'Grid Docs':>10} | {'3-Pool':>8} | {'5-Pool':>8} | {'7-Pool':>8}")
    print(f"  {'-'*8}-+-{'-'*10}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}")
    for r in results[:10]:
        print(f"  {r.query_id:>8} | {r.grid_doc_count:>10} | "
              f"{r.pool3_coverage:>7.1%} | {r.pool5_coverage:>7.1%} | {r.pool7_coverage:>7.1%}")
    
    # Average doc counts
    avg_grid = np.mean([r.grid_doc_count for r in results])
    avg_pool5 = np.mean([r.pool5_doc_count for r in results])
    print(f"\n  Average unique docs per query: Grid={avg_grid:.1f}, 5-Pool={avg_pool5:.1f}")


# ============================================================================
# EXPERIMENT E2: METRIC SENSITIVITY TO COVERAGE GAPS
# ============================================================================

@dataclass
class SensitivityResult:
    """Sensitivity results for a single coverage level."""
    coverage_level: float
    mean_weight_error: float
    median_weight_error: float
    max_weight_error: float
    mean_ndcg_loss: float
    pct_within_01: float  # % of queries where weight error ≤ 0.1
    pct_within_02: float  # % of queries where weight error ≤ 0.2


def run_experiment_e2(
    searcher: HybridSearcher,
    queries: Dict[str, ESCIQuery],
    query_ids: List[str],
    llm_cache: Dict[str, Dict[str, float]],
    size: int = 10,
    num_repeats: int = 5,
    seed: int = 42
) -> List[SensitivityResult]:
    """
    E2: Metric Sensitivity to Coverage Gaps
    
    Uses existing LLM ratings. For each query, run grid search with full ratings,
    then simulate coverage gaps by randomly removing ratings and re-computing metrics.
    """
    print("\n" + "=" * 80)
    print("EXPERIMENT E2: METRIC SENSITIVITY TO COVERAGE GAPS")
    print("=" * 80)
    print(f"Queries: {len(query_ids)}, Size: {size}, Repeats: {num_repeats}")
    print()
    
    coverage_levels = [1.0, 0.95, 0.90, 0.85, 0.80, 0.75, 0.70]
    rng = random.Random(seed)
    
    # Step 1: Execute grid search once per query, collect (variant, doc_ids) pairs
    print("[Step 1] Executing grid search and collecting rankings...")
    
    # query_id -> weight -> [doc_ids]
    query_rankings: Dict[str, Dict[float, List[str]]] = {}
    
    for i, qid in enumerate(query_ids):
        if qid not in llm_cache or not llm_cache[qid]:
            continue
        
        query_text = queries[qid].text
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  Query {i+1}/{len(query_ids)}")
        
        query_rankings[qid] = {}
        for weight in WEIGHT_STEPS:
            doc_ids = execute_hybrid_search(
                searcher, query_text, weight,
                "min_max", "arithmetic_mean", size
            )
            query_rankings[qid][weight] = doc_ids
    
    valid_qids = list(query_rankings.keys())
    print(f"  Valid queries with rankings + ratings: {len(valid_qids)}")
    
    # Step 2: For each coverage level, simulate gaps and compute metrics
    print("\n[Step 2] Simulating coverage gaps...")
    
    sensitivity_results = []
    
    for coverage in coverage_levels:
        print(f"\n  Coverage: {coverage:.0%}")
        
        all_weight_errors = []
        all_ndcg_losses = []
        
        for repeat in range(num_repeats):
            for qid in valid_qids:
                full_ratings = llm_cache[qid]
                rated_doc_ids = list(full_ratings.keys())
                
                # Scale ratings for NDCG (0-1 → 0-3 like ESCI)
                full_ratings_scaled = {d: r * 3 for d, r in full_ratings.items()}
                
                # Compute baseline NDCG@10 at each weight with full ratings
                baseline_ndcg = {}
                for weight in WEIGHT_STEPS:
                    ranking = query_rankings[qid].get(weight, [])
                    baseline_ndcg[weight] = compute_ndcg(ranking, full_ratings_scaled, k=10)
                
                baseline_best_weight = max(baseline_ndcg, key=baseline_ndcg.get)
                baseline_best_ndcg = baseline_ndcg[baseline_best_weight]
                
                if coverage >= 1.0:
                    # No gap — baseline is the reference
                    all_weight_errors.append(0.0)
                    all_ndcg_losses.append(0.0)
                    continue
                
                # Simulate coverage gap: randomly keep `coverage` fraction of ratings
                num_to_keep = max(1, int(len(rated_doc_ids) * coverage))
                kept_docs = rng.sample(rated_doc_ids, num_to_keep)
                partial_ratings_scaled = {d: full_ratings_scaled[d] for d in kept_docs}
                
                # Compute NDCG@10 at each weight with partial ratings
                partial_ndcg = {}
                for weight in WEIGHT_STEPS:
                    ranking = query_rankings[qid].get(weight, [])
                    # Filter ranking to only docs with ratings (same as EvaluationMetrics.java)
                    filtered_ranking = [d for d in ranking if d in partial_ratings_scaled]
                    partial_ndcg[weight] = compute_ndcg(
                        filtered_ranking, partial_ratings_scaled, k=10
                    )
                
                partial_best_weight = max(partial_ndcg, key=partial_ndcg.get)
                
                weight_error = abs(baseline_best_weight - partial_best_weight)
                # NDCG loss: how much worse is the partial-best config when evaluated with full ratings
                ndcg_loss = baseline_best_ndcg - baseline_ndcg.get(partial_best_weight, 0)
                
                all_weight_errors.append(weight_error)
                all_ndcg_losses.append(ndcg_loss)
        
        if all_weight_errors:
            sensitivity_results.append(SensitivityResult(
                coverage_level=coverage,
                mean_weight_error=np.mean(all_weight_errors),
                median_weight_error=np.median(all_weight_errors),
                max_weight_error=np.max(all_weight_errors),
                mean_ndcg_loss=np.mean(all_ndcg_losses),
                pct_within_01=sum(1 for e in all_weight_errors if e <= 0.1) / len(all_weight_errors) * 100,
                pct_within_02=sum(1 for e in all_weight_errors if e <= 0.2) / len(all_weight_errors) * 100,
            ))
    
    return sensitivity_results


def print_e2_results(results: List[SensitivityResult]):
    """Print E2 summary."""
    print("\n" + "=" * 80)
    print("E2 RESULTS: METRIC SENSITIVITY TO COVERAGE GAPS")
    print("=" * 80)
    
    print(f"\n  {'Coverage':>10} | {'Mean W Err':>10} | {'Med W Err':>10} | {'Max W Err':>10} | "
          f"{'NDCG Loss':>10} | {'≤0.1 Err':>8} | {'≤0.2 Err':>8}")
    print(f"  {'-'*10}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}-+-{'-'*8}-+-{'-'*8}")
    
    for r in results:
        print(f"  {r.coverage_level:>9.0%} | {r.mean_weight_error:>10.4f} | "
              f"{r.median_weight_error:>10.4f} | {r.max_weight_error:>10.4f} | "
              f"{r.mean_ndcg_loss:>10.4f} | {r.pct_within_01:>7.1f}% | {r.pct_within_02:>7.1f}%")
    
    print()
    
    # Check success criteria
    for r in results:
        if r.coverage_level == 0.85:
            ok_weight = "✅" if r.mean_weight_error <= 0.1 else "❌"
            ok_ndcg = "✅" if r.mean_ndcg_loss <= 0.02 else "❌"
            print(f"  Success Criteria (85% coverage):")
            print(f"    {ok_weight} Mean weight error ≤ 0.1: {r.mean_weight_error:.4f}")
            print(f"    {ok_ndcg} Mean NDCG loss ≤ 0.02: {r.mean_ndcg_loss:.4f}")
        if r.coverage_level == 0.70:
            ok = "✅" if r.mean_weight_error <= 0.2 else "❌"
            print(f"  Success Criteria (70% coverage):")
            print(f"    {ok} Mean weight error ≤ 0.2: {r.mean_weight_error:.4f}")


# ============================================================================
# MAIN
# ============================================================================

def load_llm_cache_nested(cache_path: str) -> Dict[str, Dict[str, float]]:
    """Load LLM cache in nested format: {query_id: {doc_id: rating}}."""
    if not os.path.exists(cache_path):
        print(f"  WARNING: Cache file not found: {cache_path}")
        return {}
    
    with open(cache_path, 'r') as f:
        raw_cache = json.load(f)
    
    nested = {}
    if raw_cache:
        first_key = next(iter(raw_cache))
        first_value = raw_cache[first_key]
        
        if isinstance(first_value, dict) and 'llm_ratings' in first_value:
            # 3way format
            for qid, data in raw_cache.items():
                nested[qid] = {k: float(v) for k, v in data.get('llm_ratings', {}).items()}
        else:
            # Flat format: key = "qid_docid"
            for key, rating in raw_cache.items():
                parts = key.split('_', 1)
                if len(parts) == 2:
                    qid, doc_id = parts
                    if qid not in nested:
                        nested[qid] = {}
                    nested[qid][doc_id] = float(rating)
    
    total_ratings = sum(len(v) for v in nested.values())
    print(f"  Loaded {total_ratings} ratings for {len(nested)} queries")
    return nested


def save_results(e1_results, e2_results, output_path: str):
    """Save all results to JSON."""
    output = {
        "experiment": "Pooling Coverage Validation",
        "e1_pool_coverage": {
            "per_query": [asdict(r) for r in e1_results] if e1_results else [],
            "summary": {}
        },
        "e2_metric_sensitivity": {
            "results": [asdict(r) for r in e2_results] if e2_results else [],
        },
        "configuration": {
            "normalizations": NORMALIZATIONS,
            "combinations": COMBINATIONS,
            "weight_steps": WEIGHT_STEPS,
            "pool_configs": {str(k): v for k, v in POOL_CONFIGS.items()},
            "pool_normalization": POOL_NORMALIZATION,
            "pool_combination": POOL_COMBINATION,
        }
    }
    
    # Add E1 summary
    if e1_results:
        for pool_size in [3, 5, 7]:
            attr = f"pool{pool_size}_coverage"
            coverages = [getattr(r, attr) for r in e1_results if r.grid_doc_count > 0]
            if coverages:
                output["e1_pool_coverage"]["summary"][f"pool_{pool_size}"] = {
                    "mean": float(np.mean(coverages)),
                    "median": float(np.median(coverages)),
                    "min": float(np.min(coverages)),
                    "std": float(np.std(coverages)),
                    "p10": float(np.percentile(coverages, 10)),
                    "pct_above_80": float(sum(1 for c in coverages if c >= 0.80) / len(coverages) * 100),
                    "pct_above_90": float(sum(1 for c in coverages if c >= 0.90) / len(coverages) * 100),
                }
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Pooling Coverage Validation Experiments (E1 + E2)"
    )
    parser.add_argument("--host", required=True, help="OpenSearch host")
    parser.add_argument("--port", type=int, default=80, help="OpenSearch port")
    parser.add_argument("--index", default="esci-products", help="Index name")
    parser.add_argument("--embedding-model-id", required=True, help="Embedding model ID")
    parser.add_argument("--neural-field", default="info_embedding", help="Neural/knn_vector field name")
    parser.add_argument("--lexical-fields", default="product_title^2.0,product_description",
                        help="Comma-separated lexical field names")
    parser.add_argument("--queries-path", default="datasets/esci/esci_us_queries_100.json")
    parser.add_argument("--qrels-path", default="datasets/esci/esci_us_qrels_100.tsv")
    parser.add_argument("--llm-cache-file", default="esci_llm_cache_100q_gpt35turbo.json",
                        help="Existing LLM rating cache (no new LLM calls needed)")
    parser.add_argument("--num-queries", type=int, default=100)
    parser.add_argument("--size", type=int, default=10, help="Results per search (matches RFC default)")
    parser.add_argument("--output", default="pooling_coverage_results.json")
    parser.add_argument("--e1-only", action="store_true", help="Run only E1")
    parser.add_argument("--e2-only", action="store_true", help="Run only E2")
    parser.add_argument("--e2-repeats", type=int, default=5, help="Random sampling repeats for E2")
    
    args = parser.parse_args()
    
    run_e1 = not args.e2_only
    run_e2 = not args.e1_only
    
    print("=" * 80)
    print("POOLING COVERAGE VALIDATION EXPERIMENTS")
    print("Auto-Fill Judgment RFC — Design Assumption Validation")
    print("=" * 80)
    print(f"OpenSearch: http://{args.host}:{args.port}")
    print(f"Index: {args.index}")
    print(f"Size: {args.size} (results per query per variant)")
    print(f"Run E1: {run_e1}, Run E2: {run_e2}")
    print("=" * 80)
    
    # Load queries
    print("\n[Loading data...]")
    queries = load_esci_queries(args.queries_path)
    qrels = load_esci_qrels(args.qrels_path)
    print(f"  Loaded {len(queries)} queries, {len(qrels)} with qrels")
    
    # Initialize client
    client = OpenSearchClient(host=args.host, port=args.port)
    
    if not check_index_exists(client, args.index):
        print(f"\n[ERROR] Index '{args.index}' does not exist!")
        return 1
    
    doc_count = get_index_doc_count(client, args.index)
    print(f"  Index '{args.index}' has {doc_count} documents")
    
    # Filter valid queries
    valid_query_ids = [qid for qid in queries.keys() if qid in qrels]
    sampled_ids = valid_query_ids[:args.num_queries]
    print(f"  Using {len(sampled_ids)} queries")
    
    # Initialize searcher with configurable fields
    lexical_fields = [f.strip() for f in args.lexical_fields.split(",")]
    print(f"  Neural field: {args.neural_field}")
    print(f"  Lexical fields: {lexical_fields}")
    
    searcher = HybridSearcher(
        client=client,
        index_name=args.index,
        embedding_model_id=args.embedding_model_id,
        neural_field=args.neural_field,
        lexical_fields=lexical_fields
    )
    
    # Run experiments
    e1_results = None
    e2_results = None
    
    if run_e1:
        e1_results = run_experiment_e1(searcher, queries, sampled_ids, args.size)
        print_e1_results(e1_results)
    
    if run_e2:
        print("\n[Loading LLM cache for E2...]")
        llm_cache = load_llm_cache_nested(args.llm_cache_file)
        
        if not llm_cache:
            print("[ERROR] No LLM ratings available. Cannot run E2.")
            print("  Run esci_weight_grid_search.py first to generate ratings.")
        else:
            e2_results = run_experiment_e2(
                searcher, queries, sampled_ids, llm_cache,
                args.size, args.e2_repeats
            )
            print_e2_results(e2_results)
    
    # Save results
    save_results(e1_results, e2_results, args.output)
    
    print("\n" + "=" * 80)
    print("EXPERIMENTS COMPLETE")
    print("=" * 80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
