#!/usr/bin/env python3
"""
TREC-COVID Weight Grid Search Experiment

Systematically evaluates hybrid search performance across 11 weight configurations
(neural_weight from 0.0 to 1.0) using both Human and LLM ground truth.

Fixed Parameters:
- Normalization: min_max
- Combination: arithmetic_mean

Metrics (at k=1, 10, 25):
- NDCG@k
- Recall@k

Usage:
    python dynamic_hybrid/trec_covid_weight_grid_search.py \
        --host <opensearch_host> \
        --port 80 \
        --embedding-model-id <model_id> \
        --cache-file trec_covid_llm_cache.json \
        --num-queries 50
"""

import argparse
import json
import sys
import os
import numpy as np
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.utils.opensearch_client import OpenSearchClient


# ============================================================================
# DATA LOADING
# ============================================================================

@dataclass
class TRECCovidQuery:
    """TREC-COVID query with metadata."""
    query_id: str
    text: str


@dataclass
class CacheEntry:
    """Cache entry from previous experiment."""
    query_id: str
    query_text: str
    llm_ratings: Dict[str, float]
    config_rankings: Dict[str, List[str]]
    timestamp: str


def load_trec_covid_queries(queries_path: str) -> Dict[str, TRECCovidQuery]:
    """Load TREC-COVID queries from JSONL file."""
    queries = {}
    with open(queries_path, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            query_id = data['_id']
            queries[query_id] = TRECCovidQuery(
                query_id=query_id,
                text=data['text']
            )
    return queries


def load_trec_covid_qrels(qrels_path: str) -> Dict[str, Dict[str, int]]:
    """Load TREC-COVID relevance judgments (qrels)."""
    qrels = defaultdict(dict)
    with open(qrels_path, 'r') as f:
        for i, line in enumerate(f):
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
                    continue
    return dict(qrels)


def load_cache(cache_path: str) -> Dict[str, CacheEntry]:
    """Load LLM ratings cache from previous experiment."""
    cache = {}
    with open(cache_path, 'r') as f:
        data = json.load(f)
    for qid, entry_data in data.items():
        cache[qid] = CacheEntry(**entry_data)
    return cache


# ============================================================================
# METRICS
# ============================================================================

def compute_ndcg(ranking: List[str], relevance: Dict[str, float], k: int) -> float:
    """
    Compute NDCG@k for a given ranking against relevance judgments.
    
    Args:
        ranking: Ordered list of document IDs (best first)
        relevance: Dict of doc_id -> relevance score
        k: Cutoff position
        
    Returns:
        NDCG@k score
    """
    if not ranking or not relevance:
        return 0.0
    
    # Get DCG
    dcg = 0.0
    for i, doc_id in enumerate(ranking[:k]):
        rel = relevance.get(doc_id, 0)
        dcg += (2**rel - 1) / np.log2(i + 2)
    
    # Get ideal ranking (sort by relevance)
    ideal_ranking = sorted(relevance.keys(), key=lambda x: relevance[x], reverse=True)
    idcg = 0.0
    for i, doc_id in enumerate(ideal_ranking[:k]):
        rel = relevance[doc_id]
        idcg += (2**rel - 1) / np.log2(i + 2)
    
    if idcg == 0:
        return 0.0
    
    return dcg / idcg


def compute_recall(
    ranking: List[str], 
    relevance: Dict[str, float], 
    k: int,
    relevance_threshold: float = 1.0
) -> float:
    """
    Compute Recall@k.
    
    Recall@k = |{relevant docs in top-k}| / |{total relevant docs in corpus}|
    
    Args:
        ranking: Ordered list of document IDs (best first)
        relevance: Dict of doc_id -> relevance score
        k: Cutoff position
        relevance_threshold: Minimum score to be considered relevant
        
    Returns:
        Recall@k score
    """
    if not ranking or not relevance:
        return 0.0
    
    # Count relevant docs in top-k
    relevant_in_topk = 0
    for doc_id in ranking[:k]:
        if relevance.get(doc_id, 0) >= relevance_threshold:
            relevant_in_topk += 1
    
    # Count total relevant docs in corpus/pool
    total_relevant = sum(1 for score in relevance.values() if score >= relevance_threshold)
    
    if total_relevant == 0:
        return 0.0
    
    return relevant_in_topk / total_relevant


# ============================================================================
# HYBRID SEARCH
# ============================================================================

class HybridSearcher:
    """Execute hybrid searches with configurable weights."""
    
    def __init__(
        self,
        client: OpenSearchClient,
        index_name: str,
        embedding_model_id: str,
        neural_field: str = "passage_embedding",
        lexical_fields: List[str] = None
    ):
        self.client = client
        self.index_name = index_name
        self.embedding_model_id = embedding_model_id
        self.neural_field = neural_field
        self.lexical_fields = lexical_fields or ["title_key", "text_key"]
    
    def search(
        self,
        query: str,
        neural_weight: float,
        lexical_weight: float,
        size: int = 100
    ) -> List[str]:
        """
        Execute hybrid search with specified weights.
        
        Returns list of doc_ids in ranked order.
        """
        # Handle edge cases
        if neural_weight == 0.0:
            return self._pure_lexical_search(query, size)
        elif lexical_weight == 0.0:
            return self._pure_neural_search(query, size)
        
        # Standard hybrid search
        query_body = {
            "size": size,
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": self.lexical_fields,
                                "type": "best_fields"
                            }
                        },
                        {
                            "neural": {
                                self.neural_field: {
                                    "query_text": query,
                                    "model_id": self.embedding_model_id,
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
                        "normalization": {"technique": "min_max"},
                        "combination": {
                            "technique": "arithmetic_mean",
                            "parameters": {
                                "weights": [lexical_weight, neural_weight]
                            }
                        }
                    }
                }]
            }
        }
        
        try:
            response = self.client.session.post(
                f"{self.client.base_url}/{self.index_name}/_search",
                json=query_body,
                timeout=(30, 120)
            )
            
            if response.status_code == 200:
                results = response.json()
                return [hit['_id'] for hit in results.get('hits', {}).get('hits', [])]
            else:
                print(f"  [ERROR] Search failed: {response.status_code}")
                return []
        except Exception as e:
            print(f"  [ERROR] Search exception: {str(e)}")
            return []
    
    def _pure_lexical_search(self, query: str, size: int) -> List[str]:
        """Pure BM25 search (no neural component)."""
        query_body = {
            "size": size,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": self.lexical_fields,
                    "type": "best_fields"
                }
            }
        }
        
        try:
            response = self.client.session.post(
                f"{self.client.base_url}/{self.index_name}/_search",
                json=query_body,
                timeout=(30, 60)
            )
            
            if response.status_code == 200:
                results = response.json()
                return [hit['_id'] for hit in results.get('hits', {}).get('hits', [])]
            return []
        except Exception as e:
            print(f"  [ERROR] Lexical search exception: {str(e)}")
            return []
    
    def _pure_neural_search(self, query: str, size: int) -> List[str]:
        """Pure neural search (no BM25 component)."""
        query_body = {
            "size": size,
            "query": {
                "neural": {
                    self.neural_field: {
                        "query_text": query,
                        "model_id": self.embedding_model_id,
                        "k": size
                    }
                }
            }
        }
        
        try:
            response = self.client.session.post(
                f"{self.client.base_url}/{self.index_name}/_search",
                json=query_body,
                timeout=(30, 120)
            )
            
            if response.status_code == 200:
                results = response.json()
                return [hit['_id'] for hit in results.get('hits', {}).get('hits', [])]
            return []
        except Exception as e:
            print(f"  [ERROR] Neural search exception: {str(e)}")
            return []


# ============================================================================
# GRID SEARCH ENGINE
# ============================================================================

@dataclass
class WeightMetrics:
    """Metrics for a single weight configuration."""
    neural_weight: float
    ndcg_1: float
    ndcg_10: float
    ndcg_25: float
    recall_1: float
    recall_10: float
    recall_25: float


class GridSearchEngine:
    """Run weight grid search experiment."""
    
    # Weight configurations: 0.0 to 1.0 with step 0.1
    WEIGHTS = [round(w * 0.1, 1) for w in range(11)]  # [0.0, 0.1, ..., 1.0]
    
    # LLM relevance threshold (rating >= 0.6 is "relevant")
    LLM_RELEVANCE_THRESHOLD = 0.6
    
    def __init__(
        self,
        searcher: HybridSearcher,
        queries: Dict[str, TRECCovidQuery],
        human_qrels: Dict[str, Dict[str, int]],
        llm_cache: Dict[str, CacheEntry],
        debug: bool = False
    ):
        self.searcher = searcher
        self.queries = queries
        self.human_qrels = human_qrels
        self.llm_cache = llm_cache
        self.debug = debug
        
        # Cache for search results: (query_id, neural_weight) -> [doc_ids]
        self.search_cache: Dict[Tuple[str, float], List[str]] = {}
        
        # Pre-populate from existing cache (approximate weights)
        self._load_cached_rankings()
    
    def _load_cached_rankings(self):
        """Load rankings from existing cache where weights approximately match."""
        weight_mapping = {
            "lexical_dominant": 0.2,
            "lexical_heavy": 0.3,
            "balanced": 0.5,
            "neural_heavy": 0.7,
            "neural_dominant": 0.8,
        }
        
        for qid, entry in self.llm_cache.items():
            for config_name, doc_ids in entry.config_rankings.items():
                if config_name in weight_mapping:
                    weight = weight_mapping[config_name]
                    self.search_cache[(qid, weight)] = doc_ids
        
        if self.debug:
            print(f"  Pre-loaded {len(self.search_cache)} rankings from cache")
    
    def run(self, query_ids: List[str]) -> Tuple[List[WeightMetrics], List[WeightMetrics]]:
        """
        Run grid search for specified queries.
        
        Returns:
            Tuple of (human_gt_metrics, llm_gt_metrics)
        """
        human_metrics = []
        llm_metrics = []
        
        for neural_weight in self.WEIGHTS:
            lexical_weight = round(1.0 - neural_weight, 1)
            
            print(f"\n[Weight: neural={neural_weight}, lexical={lexical_weight}]")
            
            # Collect per-query metrics
            human_ndcg_1, human_ndcg_10, human_ndcg_25 = [], [], []
            human_recall_1, human_recall_10, human_recall_25 = [], [], []
            llm_ndcg_1, llm_ndcg_10, llm_ndcg_25 = [], [], []
            llm_recall_1, llm_recall_10, llm_recall_25 = [], [], []
            
            for i, qid in enumerate(query_ids):
                if self.debug and i % 10 == 0:
                    print(f"  Query {i+1}/{len(query_ids)}...")
                
                query = self.queries[qid]
                
                # Get ranking (from cache or search)
                ranking = self._get_ranking(qid, query.text, neural_weight)
                
                if not ranking:
                    continue
                
                # Human ground truth metrics
                human_rel = self.human_qrels.get(qid, {})
                if human_rel:
                    human_ndcg_1.append(compute_ndcg(ranking, human_rel, k=1))
                    human_ndcg_10.append(compute_ndcg(ranking, human_rel, k=10))
                    human_ndcg_25.append(compute_ndcg(ranking, human_rel, k=25))
                    human_recall_1.append(compute_recall(ranking, human_rel, k=1, relevance_threshold=1))
                    human_recall_10.append(compute_recall(ranking, human_rel, k=10, relevance_threshold=1))
                    human_recall_25.append(compute_recall(ranking, human_rel, k=25, relevance_threshold=1))
                
                # LLM ground truth metrics
                if qid in self.llm_cache:
                    llm_rel = self.llm_cache[qid].llm_ratings
                    # Scale LLM ratings to match human scale (0-1 -> 0-2)
                    llm_rel_scaled = {doc_id: score * 2 for doc_id, score in llm_rel.items()}
                    
                    llm_ndcg_1.append(compute_ndcg(ranking, llm_rel_scaled, k=1))
                    llm_ndcg_10.append(compute_ndcg(ranking, llm_rel_scaled, k=10))
                    llm_ndcg_25.append(compute_ndcg(ranking, llm_rel_scaled, k=25))
                    # For recall, use threshold of 0.6 (in original 0-1 scale)
                    llm_recall_1.append(compute_recall(ranking, llm_rel, k=1, relevance_threshold=self.LLM_RELEVANCE_THRESHOLD))
                    llm_recall_10.append(compute_recall(ranking, llm_rel, k=10, relevance_threshold=self.LLM_RELEVANCE_THRESHOLD))
                    llm_recall_25.append(compute_recall(ranking, llm_rel, k=25, relevance_threshold=self.LLM_RELEVANCE_THRESHOLD))
            
            # Aggregate metrics
            human_metrics.append(WeightMetrics(
                neural_weight=neural_weight,
                ndcg_1=np.mean(human_ndcg_1) if human_ndcg_1 else 0,
                ndcg_10=np.mean(human_ndcg_10) if human_ndcg_10 else 0,
                ndcg_25=np.mean(human_ndcg_25) if human_ndcg_25 else 0,
                recall_1=np.mean(human_recall_1) if human_recall_1 else 0,
                recall_10=np.mean(human_recall_10) if human_recall_10 else 0,
                recall_25=np.mean(human_recall_25) if human_recall_25 else 0,
            ))
            
            llm_metrics.append(WeightMetrics(
                neural_weight=neural_weight,
                ndcg_1=np.mean(llm_ndcg_1) if llm_ndcg_1 else 0,
                ndcg_10=np.mean(llm_ndcg_10) if llm_ndcg_10 else 0,
                ndcg_25=np.mean(llm_ndcg_25) if llm_ndcg_25 else 0,
                recall_1=np.mean(llm_recall_1) if llm_recall_1 else 0,
                recall_10=np.mean(llm_recall_10) if llm_recall_10 else 0,
                recall_25=np.mean(llm_recall_25) if llm_recall_25 else 0,
            ))
            
            print(f"  Human NDCG@10: {human_metrics[-1].ndcg_10:.4f}, LLM NDCG@10: {llm_metrics[-1].ndcg_10:.4f}")
        
        return human_metrics, llm_metrics
    
    def _get_ranking(self, query_id: str, query_text: str, neural_weight: float) -> List[str]:
        """Get ranking from cache or execute search."""
        cache_key = (query_id, neural_weight)
        
        if cache_key in self.search_cache:
            return self.search_cache[cache_key]
        
        # Execute search
        lexical_weight = round(1.0 - neural_weight, 1)
        ranking = self.searcher.search(
            query=query_text,
            neural_weight=neural_weight,
            lexical_weight=lexical_weight,
            size=100
        )
        
        self.search_cache[cache_key] = ranking
        return ranking


# ============================================================================
# OUTPUT
# ============================================================================

def print_results(human_metrics: List[WeightMetrics], llm_metrics: List[WeightMetrics]):
    """Print comprehensive results tables."""
    
    print("\n" + "="*100)
    print("WEIGHT GRID SEARCH RESULTS")
    print("Configuration: min_max + arithmetic_mean, 50 queries")
    print("="*100)
    
    # Table A: Human Ground Truth
    print("\n[A] HUMAN GROUND TRUTH")
    print("-"*100)
    print(f"{'Weight':>8} | {'NDCG@1':>10} | {'NDCG@10':>10} | {'NDCG@25':>10} | {'Recall@1':>10} | {'Recall@10':>10} | {'Recall@25':>10}")
    print("-"*100)
    
    for m in human_metrics:
        print(f"{m.neural_weight:>8.1f} | {m.ndcg_1:>10.4f} | {m.ndcg_10:>10.4f} | {m.ndcg_25:>10.4f} | {m.recall_1:>10.4f} | {m.recall_10:>10.4f} | {m.recall_25:>10.4f}")
    
    print("-"*100)
    
    # Find best weights for human GT
    best_human = {
        'ndcg_1': max(human_metrics, key=lambda x: x.ndcg_1),
        'ndcg_10': max(human_metrics, key=lambda x: x.ndcg_10),
        'ndcg_25': max(human_metrics, key=lambda x: x.ndcg_25),
        'recall_1': max(human_metrics, key=lambda x: x.recall_1),
        'recall_10': max(human_metrics, key=lambda x: x.recall_10),
        'recall_25': max(human_metrics, key=lambda x: x.recall_25),
    }
    
    print("\nBest Weights (Human GT):")
    print(f"  NDCG@1:   {best_human['ndcg_1'].neural_weight:.1f} ({best_human['ndcg_1'].ndcg_1:.4f})")
    print(f"  NDCG@10:  {best_human['ndcg_10'].neural_weight:.1f} ({best_human['ndcg_10'].ndcg_10:.4f})")
    print(f"  NDCG@25:  {best_human['ndcg_25'].neural_weight:.1f} ({best_human['ndcg_25'].ndcg_25:.4f})")
    print(f"  Recall@1: {best_human['recall_1'].neural_weight:.1f} ({best_human['recall_1'].recall_1:.4f})")
    print(f"  Recall@10:{best_human['recall_10'].neural_weight:.1f} ({best_human['recall_10'].recall_10:.4f})")
    print(f"  Recall@25:{best_human['recall_25'].neural_weight:.1f} ({best_human['recall_25'].recall_25:.4f})")
    
    # Table B: LLM Ground Truth
    print("\n\n[B] LLM GROUND TRUTH (relevance threshold >= 0.6)")
    print("-"*100)
    print(f"{'Weight':>8} | {'NDCG@1':>10} | {'NDCG@10':>10} | {'NDCG@25':>10} | {'Recall@1':>10} | {'Recall@10':>10} | {'Recall@25':>10}")
    print("-"*100)
    
    for m in llm_metrics:
        print(f"{m.neural_weight:>8.1f} | {m.ndcg_1:>10.4f} | {m.ndcg_10:>10.4f} | {m.ndcg_25:>10.4f} | {m.recall_1:>10.4f} | {m.recall_10:>10.4f} | {m.recall_25:>10.4f}")
    
    print("-"*100)
    
    # Find best weights for LLM GT
    best_llm = {
        'ndcg_1': max(llm_metrics, key=lambda x: x.ndcg_1),
        'ndcg_10': max(llm_metrics, key=lambda x: x.ndcg_10),
        'ndcg_25': max(llm_metrics, key=lambda x: x.ndcg_25),
        'recall_1': max(llm_metrics, key=lambda x: x.recall_1),
        'recall_10': max(llm_metrics, key=lambda x: x.recall_10),
        'recall_25': max(llm_metrics, key=lambda x: x.recall_25),
    }
    
    print("\nBest Weights (LLM GT):")
    print(f"  NDCG@1:   {best_llm['ndcg_1'].neural_weight:.1f} ({best_llm['ndcg_1'].ndcg_1:.4f})")
    print(f"  NDCG@10:  {best_llm['ndcg_10'].neural_weight:.1f} ({best_llm['ndcg_10'].ndcg_10:.4f})")
    print(f"  NDCG@25:  {best_llm['ndcg_25'].neural_weight:.1f} ({best_llm['ndcg_25'].ndcg_25:.4f})")
    print(f"  Recall@1: {best_llm['recall_1'].neural_weight:.1f} ({best_llm['recall_1'].recall_1:.4f})")
    print(f"  Recall@10:{best_llm['recall_10'].neural_weight:.1f} ({best_llm['recall_10'].recall_10:.4f})")
    print(f"  Recall@25:{best_llm['recall_25'].neural_weight:.1f} ({best_llm['recall_25'].recall_25:.4f})")
    
    # Table C: Agreement Summary
    print("\n\n[C] AGREEMENT SUMMARY")
    print("="*60)
    print(f"{'Metric':<12} | {'Human Best':>12} | {'LLM Best':>12} | {'Match?':>10}")
    print("-"*60)
    
    metrics_to_compare = [
        ('NDCG@1', 'ndcg_1'),
        ('NDCG@10', 'ndcg_10'),
        ('NDCG@25', 'ndcg_25'),
        ('Recall@1', 'recall_1'),
        ('Recall@10', 'recall_10'),
        ('Recall@25', 'recall_25'),
    ]
    
    agreements = 0
    for name, key in metrics_to_compare:
        human_best_w = best_human[key].neural_weight
        llm_best_w = best_llm[key].neural_weight
        match = "✅" if human_best_w == llm_best_w else "❌"
        if human_best_w == llm_best_w:
            agreements += 1
        print(f"{name:<12} | {human_best_w:>12.1f} | {llm_best_w:>12.1f} | {match:>10}")
    
    print("-"*60)
    print(f"\nTotal Agreement: {agreements}/6 metrics ({100*agreements/6:.1f}%)")
    print("="*60)
    
    return best_human, best_llm


def save_results(
    human_metrics: List[WeightMetrics],
    llm_metrics: List[WeightMetrics],
    output_path: str
):
    """Save results to JSON file."""
    results = {
        "human_ground_truth": [asdict(m) for m in human_metrics],
        "llm_ground_truth": [asdict(m) for m in llm_metrics],
        "configuration": {
            "normalization": "min_max",
            "combination": "arithmetic_mean",
            "llm_relevance_threshold": 0.6,
            "num_weights": 11,
            "cutoffs": [1, 10, 25]
        }
    }
    
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: {output_path}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="TREC-COVID Weight Grid Search Experiment"
    )
    parser.add_argument("--host", required=True, help="OpenSearch host")
    parser.add_argument("--port", type=int, default=80, help="OpenSearch port")
    parser.add_argument("--index", default="trec-covid", help="Index name")
    parser.add_argument("--embedding-model-id", required=True, help="Embedding model ID")
    parser.add_argument("--cache-file", required=True, help="Path to LLM cache file")
    parser.add_argument("--queries-path", default="datasets/trec-covid/queries.jsonl")
    parser.add_argument("--qrels-path", default="datasets/trec-covid/qrels/test.tsv")
    parser.add_argument("--num-queries", type=int, default=50, help="Number of queries")
    parser.add_argument("--output", default="trec_covid_weight_grid_results.json", help="Output file")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    
    args = parser.parse_args()
    
    print("="*80)
    print("TREC-COVID WEIGHT GRID SEARCH EXPERIMENT")
    print("="*80)
    print(f"OpenSearch: http://{args.host}:{args.port}")
    print(f"Index: {args.index}")
    print(f"Embedding Model: {args.embedding_model_id}")
    print(f"Cache File: {args.cache_file}")
    print(f"Queries: {args.num_queries}")
    print("="*80)
    
    # Load data
    print("\n[Loading data...]")
    queries = load_trec_covid_queries(args.queries_path)
    qrels = load_trec_covid_qrels(args.qrels_path)
    llm_cache = load_cache(args.cache_file)
    
    print(f"  Loaded {len(queries)} queries")
    print(f"  Loaded qrels for {len(qrels)} queries")
    print(f"  Loaded LLM cache with {len(llm_cache)} entries")
    
    # Filter to queries with both qrels and LLM cache
    valid_query_ids = [qid for qid in queries.keys() if qid in qrels and qid in llm_cache]
    sampled_ids = valid_query_ids[:args.num_queries]
    
    print(f"  Valid queries (with qrels + LLM cache): {len(valid_query_ids)}")
    print(f"  Using first {len(sampled_ids)} queries")
    
    # Initialize searcher
    client = OpenSearchClient(host=args.host, port=args.port)
    searcher = HybridSearcher(
        client=client,
        index_name=args.index,
        embedding_model_id=args.embedding_model_id,
        neural_field="passage_embedding",
        lexical_fields=["title_key", "text_key"]
    )
    
    # Run grid search
    print("\n[Running grid search...]")
    engine = GridSearchEngine(
        searcher=searcher,
        queries=queries,
        human_qrels=qrels,
        llm_cache=llm_cache,
        debug=args.debug
    )
    
    human_metrics, llm_metrics = engine.run(sampled_ids)
    
    # Print results
    print_results(human_metrics, llm_metrics)
    
    # Save results
    save_results(human_metrics, llm_metrics, args.output)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
