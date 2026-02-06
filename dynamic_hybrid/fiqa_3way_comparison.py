#!/usr/bin/env python3
"""
FiQA 3-Way Comparison Experiment with LLM Ground Truth Analysis

Compares three ranking approaches and evaluates synthetic ground truth for Finance Q&A domain:
1. Hybrid Search (per config) - direct OpenSearch hybrid query
2. LLM-Pooled - pooled docs from multiple configs, ranked by LLM
3. Human Labels - ground truth from FiQA qrels

Usage:
    # First run - collects LLM ratings and saves to cache
    python dynamic_hybrid/fiqa_3way_comparison.py \
        --host <opensearch_host> \
        --port 80 \
        --embedding-model-id <model_id> \
        --llm-model-id <model_id> \
        --num-queries 50 \
        --cache-file fiqa_llm_cache.json
    
    # Subsequent runs - uses cached ratings (no LLM tokens spent)
    python dynamic_hybrid/fiqa_3way_comparison.py \
        --host <opensearch_host> \
        --port 80 \
        --embedding-model-id <model_id> \
        --llm-model-id <model_id> \
        --num-queries 50 \
        --cache-file fiqa_llm_cache.json \
        --use-cache-only
"""

import argparse
import json
import sys
import os
import time
import numpy as np
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.inline_judgment.pool_builder import PoolBuilder, PoolConfig
from dynamic_hybrid.inline_judgment.llm_judge import LLMJudge, LLMProvider
from dynamic_hybrid.utils.opensearch_client import OpenSearchClient


# Finance/Investment Q&A domain prompt for FiQA
FINANCE_SYSTEM_PROMPT = """You are an expert personal finance and investment relevance evaluator. Your task is to rate how relevant each answer/document is to a given financial question.

Rating Scale (0.0 to 1.0 with 0.2 increments):
- 1.0: Perfect Match - Answer directly and thoroughly addresses the financial question
- 0.8: Excellent - Highly relevant answer, provides substantial information for the question
- 0.6: Good - Relevant answer, partially addresses the financial question
- 0.4: Fair - Some relevance but only tangentially related to the question
- 0.2: Poor - Marginally related answer, mostly off-topic
- 0.0: Not Relevant - Completely unrelated to the financial question

Instructions:
1. Evaluate each answer independently based on its content
2. Consider practical relevance to the personal finance/investment question
3. Answers about related financial topics may be partially relevant
4. Output ONLY a JSON object with document IDs as keys and ratings as values
5. Do not include any explanation - only the JSON object"""


@dataclass
class FiQAQuery:
    """FiQA query with metadata."""
    query_id: str
    text: str
    narrative: Optional[str] = None


@dataclass
class ComparisonResult:
    """Result of 3-way comparison for a single query."""
    query_id: str
    query_text: str
    human_ndcg: Dict[str, float]  # config_name -> NDCG vs human labels
    llm_ndcg: float  # LLM-ranked vs human labels
    human_llm_correlation: float  # Spearman correlation
    best_hybrid_config: str
    best_hybrid_ndcg: float
    # LLM-as-ground-truth metrics
    llm_ground_truth_ndcg: Dict[str, float]  # config_name -> NDCG vs LLM ratings
    best_config_llm_gt: str  # Best config when using LLM as ground truth
    best_ndcg_llm_gt: float  # Best NDCG when using LLM as ground truth


@dataclass
class CacheEntry:
    """Cache entry for a single query."""
    query_id: str
    query_text: str
    llm_ratings: Dict[str, float]  # doc_id -> LLM rating
    config_rankings: Dict[str, List[str]]  # config_name -> [doc_ids]
    timestamp: str


class ResultCache:
    """Manages caching of LLM ratings and results to avoid repeated API calls."""
    
    def __init__(self, cache_file: Optional[str] = None):
        self.cache_file = cache_file
        self.cache: Dict[str, CacheEntry] = {}
        if cache_file and os.path.exists(cache_file):
            self._load()
    
    def _load(self):
        """Load cache from file."""
        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
            for qid, entry_data in data.items():
                self.cache[qid] = CacheEntry(**entry_data)
            print(f"  Loaded {len(self.cache)} entries from cache: {self.cache_file}")
        except Exception as e:
            print(f"  Warning: Could not load cache: {e}")
            self.cache = {}
    
    def _save(self):
        """Save cache to file."""
        if not self.cache_file:
            return
        try:
            data = {qid: asdict(entry) for qid, entry in self.cache.items()}
            with open(self.cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"  Warning: Could not save cache: {e}")
    
    def has(self, query_id: str) -> bool:
        """Check if query is in cache."""
        return query_id in self.cache
    
    def get(self, query_id: str) -> Optional[CacheEntry]:
        """Get cached entry for query."""
        return self.cache.get(query_id)
    
    def put(self, entry: CacheEntry):
        """Add entry to cache and save."""
        self.cache[entry.query_id] = entry
        self._save()
    
    def get_all_query_ids(self) -> List[str]:
        """Get all cached query IDs."""
        return list(self.cache.keys())


def load_fiqa_queries(queries_path: str) -> Dict[str, FiQAQuery]:
    """Load FiQA queries from JSONL file."""
    queries = {}
    with open(queries_path, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            query_id = data['_id']
            queries[query_id] = FiQAQuery(
                query_id=query_id,
                text=data['text'],
                narrative=data.get('metadata', {}).get('narrative')
            )
    return queries


def load_fiqa_qrels(qrels_path: str) -> Dict[str, Dict[str, int]]:
    """Load FiQA relevance judgments (qrels)."""
    qrels = defaultdict(dict)
    with open(qrels_path, 'r') as f:
        for i, line in enumerate(f):
            # Skip header line (first line)
            if i == 0 and ('query-id' in line.lower() or 'corpus-id' in line.lower()):
                continue
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                query_id = parts[0]
                doc_id = parts[1]
                try:
                    score = int(parts[2])
                    qrels[query_id][doc_id] = score
                except ValueError:
                    continue  # Skip non-numeric scores
    return dict(qrels)


def compute_ndcg(ranking: List[str], relevance: Dict[str, float], k: int = 10) -> float:
    """
    Compute NDCG@k for a given ranking against relevance judgments.
    """
    if not ranking or not relevance:
        return 0.0
    
    dcg = 0.0
    for i, doc_id in enumerate(ranking[:k]):
        rel = relevance.get(doc_id, 0)
        dcg += (2**rel - 1) / np.log2(i + 2)
    
    ideal_ranking = sorted(relevance.keys(), key=lambda x: relevance[x], reverse=True)
    idcg = 0.0
    for i, doc_id in enumerate(ideal_ranking[:k]):
        rel = relevance[doc_id]
        idcg += (2**rel - 1) / np.log2(i + 2)
    
    if idcg == 0:
        return 0.0
    
    return dcg / idcg


def compute_spearman_correlation(
    llm_scores: Dict[str, float],
    human_scores: Dict[str, int]
) -> float:
    """Compute Spearman rank correlation between LLM and human scores."""
    common_docs = set(llm_scores.keys()) & set(human_scores.keys())
    if len(common_docs) < 2:
        return 0.0
    
    llm_vals = [llm_scores[doc] for doc in common_docs]
    human_vals = [human_scores[doc] for doc in common_docs]
    
    from scipy import stats
    try:
        corr, _ = stats.spearmanr(llm_vals, human_vals)
        return corr if not np.isnan(corr) else 0.0
    except:
        return 0.0


class FiQAComparison:
    """3-way comparison engine for FiQA dataset."""
    
    def __init__(
        self,
        opensearch_host: str,
        opensearch_port: int,
        index_name: str,
        embedding_model_id: str,
        llm_model_id: str,
        cache: Optional[ResultCache] = None,
        debug: bool = False
    ):
        self.opensearch_url = f"http://{opensearch_host}:{opensearch_port}"
        self.index_name = index_name
        self.embedding_model_id = embedding_model_id
        self.llm_model_id = llm_model_id
        self.cache = cache
        self.debug = debug
        
        # Field mapping for FiQA index (same structure as TREC-COVID)
        self.neural_field = "passage_embedding"
        self.text_field = "text_key"
        self.title_field = "title_key"
        self.lexical_fields = ["title_key", "text_key"]  # Atomic fields only
        
        # Hybrid search configurations to compare
        self.hybrid_configs = {
            "lexical_heavy": {"neural_weight": 0.3, "lexical_weight": 0.7},
            "balanced": {"neural_weight": 0.5, "lexical_weight": 0.5},
            "neural_heavy": {"neural_weight": 0.7, "lexical_weight": 0.3},
            "neural_dominant": {"neural_weight": 0.8, "lexical_weight": 0.2},
            "lexical_dominant": {"neural_weight": 0.2, "lexical_weight": 0.8},
        }
        
        # Initialize OpenSearch client and pool builder
        self.client = OpenSearchClient(host=opensearch_host, port=opensearch_port)
        
        # Build pool configs
        self.pool_configs = [
            PoolConfig("lexical_heavy", 0.3, 0.7, "min_max", "arithmetic_mean", 150),
            PoolConfig("balanced", 0.5, 0.5, "min_max", "arithmetic_mean", 150),
            PoolConfig("neural_heavy", 0.7, 0.3, "min_max", "arithmetic_mean", 150),
            PoolConfig("neural_dominant", 0.8, 0.2, "min_max", "arithmetic_mean", 150),
            PoolConfig("lexical_dominant", 0.2, 0.8, "min_max", "arithmetic_mean", 150),
        ]
        
        self.pool_builder = PoolBuilder(
            client=self.client,
            index_name=index_name,
            model_id=embedding_model_id,
            neural_field=self.neural_field,
            lexical_fields=self.lexical_fields,
            configs=self.pool_configs
        )
        
        # Initialize LLM judge with finance domain prompt
        self.llm_judge = LLMJudge(
            opensearch_url=self.opensearch_url,
            llm_model_id=llm_model_id,
            provider=LLMProvider.ML_COMMONS,
            temperature=0.0,
            batch_size=15,
            debug=debug
        )
        # Override with finance prompt
        self.llm_judge.SYSTEM_PROMPT = FINANCE_SYSTEM_PROMPT
    
    def run_comparison(
        self,
        query: FiQAQuery,
        human_relevance: Dict[str, int],
        pool_depth: int = 50,
        use_cache_only: bool = False
    ) -> ComparisonResult:
        """
        Run 3-way comparison for a single query.
        """
        if self.debug:
            print(f"\n[Query {query.query_id}] {query.text[:80]}...")
        
        # Check cache first
        cached = self.cache.get(query.query_id) if self.cache else None
        
        if cached:
            if self.debug:
                print(f"  Using cached results ({len(cached.llm_ratings)} ratings)")
            llm_scores = cached.llm_ratings
            config_rankings = cached.config_rankings
        else:
            if use_cache_only:
                raise ValueError(f"Query {query.query_id} not in cache and use_cache_only=True")
            
            # Step 1: Build pool from all hybrid configs
            pooled_docs, pool_results = self.pool_builder.build_pool(
                query=query.text,
                fetch_sources=True,
                source_fields=[self.title_field, self.text_field],
                verbose=self.debug
            )
            
            if self.debug:
                print(f"  Pool: {len(pooled_docs)} unique docs")
            
            # Step 2: Get LLM judgments for pooled documents
            docs_for_llm = {}
            for doc_id, doc in pooled_docs.items():
                docs_for_llm[doc_id] = {
                    "title": doc.get(self.title_field, ""),
                    "text": doc.get(self.text_field, "")[:800]  # Truncate long text
                }
            
            llm_result = self.llm_judge.judge_documents(
                query=query.text,
                documents=docs_for_llm,
                content_field="text",
                title_field="title"
            )
            
            llm_scores = llm_result.judgments
            
            if self.debug:
                print(f"  LLM judged: {len(llm_scores)} docs, latency: {llm_result.latency_ms:.0f}ms")
            
            # Extract config rankings
            config_rankings = {}
            for result in pool_results:
                config_rankings[result.config.name] = result.doc_ids
            
            # Save to cache
            if self.cache:
                from datetime import datetime
                cache_entry = CacheEntry(
                    query_id=query.query_id,
                    query_text=query.text,
                    llm_ratings=llm_scores,
                    config_rankings=config_rankings,
                    timestamp=datetime.now().isoformat()
                )
                self.cache.put(cache_entry)
                if self.debug:
                    print(f"  Saved to cache")
        
        # Step 3: Compute NDCG for each hybrid config vs HUMAN labels
        human_ndcg = {}
        for config_name, ranking in config_rankings.items():
            ndcg = compute_ndcg(ranking, human_relevance, k=10)
            human_ndcg[config_name] = ndcg
            if self.debug:
                print(f"  {config_name} vs Human: NDCG@10 = {ndcg:.4f}")
        
        # Step 4: Rank documents by LLM scores and compute NDCG vs human
        llm_ranking = sorted(llm_scores.keys(), key=lambda x: llm_scores[x], reverse=True)
        llm_ndcg = compute_ndcg(llm_ranking, human_relevance, k=10)
        
        if self.debug:
            print(f"  LLM-ranked vs Human: NDCG@10 = {llm_ndcg:.4f}")
        
        # Step 5: Compute LLM-Human correlation
        correlation = compute_spearman_correlation(llm_scores, human_relevance)
        
        if self.debug:
            print(f"  LLM-Human correlation: {correlation:.4f}")
        
        # Find best hybrid config (vs human)
        best_config = max(human_ndcg.keys(), key=lambda x: human_ndcg[x])
        
        # Step 6: Compute NDCG for each config vs LLM ratings as ground truth
        llm_ground_truth = {doc_id: score * 2 for doc_id, score in llm_scores.items()}
        
        llm_ground_truth_ndcg = {}
        for config_name, ranking in config_rankings.items():
            ndcg = compute_ndcg(ranking, llm_ground_truth, k=10)
            llm_ground_truth_ndcg[config_name] = ndcg
            if self.debug:
                print(f"  {config_name} vs LLM-GT: NDCG@10 = {ndcg:.4f}")
        
        # Find best config when using LLM as ground truth
        best_config_llm_gt = max(llm_ground_truth_ndcg.keys(), key=lambda x: llm_ground_truth_ndcg[x])
        
        return ComparisonResult(
            query_id=query.query_id,
            query_text=query.text,
            human_ndcg=human_ndcg,
            llm_ndcg=llm_ndcg,
            human_llm_correlation=correlation,
            best_hybrid_config=best_config,
            best_hybrid_ndcg=human_ndcg[best_config],
            llm_ground_truth_ndcg=llm_ground_truth_ndcg,
            best_config_llm_gt=best_config_llm_gt,
            best_ndcg_llm_gt=llm_ground_truth_ndcg[best_config_llm_gt]
        )


def print_summary(results: List[ComparisonResult], configs: List[str]):
    """Print summary statistics across all queries."""
    
    print("\n" + "="*80)
    print("3-WAY COMPARISON SUMMARY")
    print("="*80)
    
    # Average NDCG per config (vs HUMAN ground truth)
    print("\n[1] Average NDCG@10 per Hybrid Configuration (vs Human Labels):")
    print("-" * 60)
    
    config_scores = defaultdict(list)
    for result in results:
        for config, ndcg in result.human_ndcg.items():
            config_scores[config].append(ndcg)
    
    sorted_configs = sorted(config_scores.keys(), key=lambda x: np.mean(config_scores[x]), reverse=True)
    for config in sorted_configs:
        scores = config_scores[config]
        print(f"    {config:20s}: {np.mean(scores):.4f} (±{np.std(scores):.4f})")
    
    # LLM performance (vs human)
    print("\n[2] LLM-Ranked Pool Performance (vs Human Labels):")
    print("-" * 60)
    llm_scores = [r.llm_ndcg for r in results]
    print(f"    Average NDCG@10: {np.mean(llm_scores):.4f} (±{np.std(llm_scores):.4f})")
    
    # Correlation
    correlations = [r.human_llm_correlation for r in results]
    print(f"    Avg LLM-Human Correlation: {np.mean(correlations):.4f}")
    
    # Best config frequency (human ground truth)
    print("\n[3] Best Configuration per Query (using Human Ground Truth):")
    print("-" * 60)
    best_counts_human = defaultdict(int)
    for result in results:
        best_counts_human[result.best_hybrid_config] += 1
    
    for config, count in sorted(best_counts_human.items(), key=lambda x: x[1], reverse=True):
        pct = 100.0 * count / len(results)
        print(f"    {config:20s}: {count:3d} queries ({pct:.1f}%)")
    
    # LLM vs Best Hybrid comparison
    print("\n[4] LLM-Ranked vs Best Hybrid Config (Human GT):")
    print("-" * 60)
    llm_wins = 0
    hybrid_wins = 0
    ties = 0
    for result in results:
        if result.llm_ndcg > result.best_hybrid_ndcg + 0.01:
            llm_wins += 1
        elif result.best_hybrid_ndcg > result.llm_ndcg + 0.01:
            hybrid_wins += 1
        else:
            ties += 1
    
    print(f"    LLM wins: {llm_wins} ({100.0*llm_wins/len(results):.1f}%)")
    print(f"    Hybrid wins: {hybrid_wins} ({100.0*hybrid_wins/len(results):.1f}%)")
    print(f"    Ties (±0.01): {ties} ({100.0*ties/len(results):.1f}%)")
    
    # ============================================================
    # LLM as Ground Truth Analysis
    # ============================================================
    print("\n" + "="*80)
    print("LLM AS GROUND TRUTH ANALYSIS")
    print("(Key question: Does LLM ground truth predict same optimal weights as Human?)")
    print("="*80)
    
    # Average NDCG per config (vs LLM ground truth)
    print("\n[5] Average NDCG@10 per Hybrid Configuration (vs LLM Ground Truth):")
    print("-" * 60)
    
    config_scores_llm_gt = defaultdict(list)
    for result in results:
        for config, ndcg in result.llm_ground_truth_ndcg.items():
            config_scores_llm_gt[config].append(ndcg)
    
    sorted_configs_llm = sorted(config_scores_llm_gt.keys(), key=lambda x: np.mean(config_scores_llm_gt[x]), reverse=True)
    for config in sorted_configs_llm:
        scores = config_scores_llm_gt[config]
        print(f"    {config:20s}: {np.mean(scores):.4f} (±{np.std(scores):.4f})")
    
    # Best config frequency (LLM ground truth)
    print("\n[6] Best Configuration per Query (using LLM Ground Truth):")
    print("-" * 60)
    best_counts_llm = defaultdict(int)
    for result in results:
        best_counts_llm[result.best_config_llm_gt] += 1
    
    for config, count in sorted(best_counts_llm.items(), key=lambda x: x[1], reverse=True):
        pct = 100.0 * count / len(results)
        print(f"    {config:20s}: {count:3d} queries ({pct:.1f}%)")
    
    # KEY METRIC: Agreement between Human and LLM ground truth
    print("\n[7] AGREEMENT: Optimal Config (Human GT) vs Optimal Config (LLM GT):")
    print("-" * 60)
    
    agreement_count = 0
    for result in results:
        if result.best_hybrid_config == result.best_config_llm_gt:
            agreement_count += 1
    
    agreement_rate = 100.0 * agreement_count / len(results)
    print(f"    Exact Agreement: {agreement_count}/{len(results)} ({agreement_rate:.1f}%)")
    
    # Global winner comparison
    print("\n" + "="*80)
    print("GLOBAL OPTIMAL CONFIGURATION COMPARISON")
    print("="*80)
    
    best_human_gt = sorted_configs[0]
    best_human_gt_ndcg = np.mean(config_scores[best_human_gt])
    
    best_llm_gt = sorted_configs_llm[0]
    best_llm_gt_ndcg = np.mean(config_scores_llm_gt[best_llm_gt])
    
    print(f"\n    Using Human Ground Truth: Best config = {best_human_gt} (NDCG = {best_human_gt_ndcg:.4f})")
    print(f"    Using LLM Ground Truth:   Best config = {best_llm_gt} (NDCG = {best_llm_gt_ndcg:.4f})")
    
    if best_human_gt == best_llm_gt:
        print(f"\n    ✅ AGREEMENT: Both ground truths select '{best_human_gt}' as optimal!")
    else:
        print(f"\n    ❌ DISAGREEMENT: Human selects '{best_human_gt}', LLM selects '{best_llm_gt}'")
    
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description="FiQA 3-Way Comparison Experiment with LLM Ground Truth Analysis"
    )
    parser.add_argument("--host", required=True, help="OpenSearch host")
    parser.add_argument("--port", type=int, default=80, help="OpenSearch port")
    parser.add_argument("--index", default="fiqa", help="Index name")
    parser.add_argument("--embedding-model-id", required=True, help="Embedding model ID")
    parser.add_argument("--llm-model-id", required=True, help="LLM model ID")
    parser.add_argument("--queries-path", default="datasets/fiqa/queries.jsonl")
    parser.add_argument("--qrels-path", default="datasets/fiqa/qrels/test.tsv")
    parser.add_argument("--num-queries", type=int, default=50, help="Number of queries to evaluate")
    parser.add_argument("--pool-depth", type=int, default=50, help="Docs per config")
    parser.add_argument("--cache-file", type=str, default=None, help="Path to cache file for LLM ratings")
    parser.add_argument("--use-cache-only", action="store_true", help="Only use cached results, don't call LLM")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    
    args = parser.parse_args()
    
    print("="*80)
    print("FiQA 3-WAY COMPARISON EXPERIMENT")
    print("With LLM Ground Truth Analysis (Finance/Investment Domain)")
    print("="*80)
    print(f"OpenSearch: http://{args.host}:{args.port}")
    print(f"Index: {args.index}")
    print(f"Embedding Model: {args.embedding_model_id}")
    print(f"LLM Model: {args.llm_model_id}")
    print(f"Queries: {args.num_queries} from {args.queries_path}")
    if args.cache_file:
        print(f"Cache: {args.cache_file}")
    if args.use_cache_only:
        print("Mode: CACHE ONLY (no LLM calls)")
    print("="*80)
    
    # Initialize cache
    cache = ResultCache(args.cache_file) if args.cache_file else None
    
    # Load queries and qrels
    print("\n[Loading data...]")
    queries = load_fiqa_queries(args.queries_path)
    qrels = load_fiqa_qrels(args.qrels_path)
    
    print(f"  Loaded {len(queries)} queries")
    print(f"  Loaded qrels for {len(qrels)} queries")
    
    # Filter queries that have qrels
    valid_query_ids = [qid for qid in queries.keys() if qid in qrels]
    print(f"  Queries with qrels: {len(valid_query_ids)}")
    
    # Sample queries
    sampled_ids = valid_query_ids[:args.num_queries]
    print(f"  Sampling first {len(sampled_ids)} queries")
    
    # Initialize comparison engine
    engine = FiQAComparison(
        opensearch_host=args.host,
        opensearch_port=args.port,
        index_name=args.index,
        embedding_model_id=args.embedding_model_id,
        llm_model_id=args.llm_model_id,
        cache=cache,
        debug=args.debug
    )
    
    # Run comparison
    results = []
    for i, qid in enumerate(sampled_ids):
        print(f"\n[Query {i+1}/{len(sampled_ids)}] ID={qid}")
        query = queries[qid]
        human_rel = qrels[qid]
        
        try:
            result = engine.run_comparison(
                query=query,
                human_relevance=human_rel,
                pool_depth=args.pool_depth,
                use_cache_only=args.use_cache_only
            )
            results.append(result)
        except Exception as e:
            print(f"  ERROR: {str(e)}")
            if args.debug:
                import traceback
                traceback.print_exc()
    
    # Print summary
    if results:
        print_summary(results, list(engine.hybrid_configs.keys()))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
