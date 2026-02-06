#!/usr/bin/env python3
"""
SciFact Weight Grid Search Experiment

Third dataset validation for LLM Ground Truth experiment.
SciFact domain: Scientific claim verification

Systematically evaluates hybrid search performance across 11 weight configurations
(neural_weight from 0.0 to 1.0) using both Human and LLM ground truth.

Fixed Parameters:
- Normalization: min_max
- Combination: arithmetic_mean

Usage:
    python dynamic_hybrid/scifact_weight_grid_search.py \
        --host <opensearch_host> \
        --port 80 \
        --embedding-model-id <model_id> \
        --num-queries 100
"""

import argparse
import json
import sys
import os
import numpy as np
import time
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
from openai import OpenAI

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.utils.opensearch_client import OpenSearchClient


# ============================================================================
# DATA LOADING
# ============================================================================

@dataclass
class SciFacQuery:
    """SciFact query (scientific claim)."""
    query_id: str
    text: str


@dataclass
class SciFacDoc:
    """SciFact document (scientific abstract)."""
    doc_id: str
    title: str
    text: str


def load_scifact_queries(queries_path: str) -> Dict[str, SciFacQuery]:
    """Load SciFact queries from JSONL file."""
    queries = {}
    with open(queries_path, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            query_id = data['_id']
            queries[query_id] = SciFacQuery(
                query_id=query_id,
                text=data['text']
            )
    return queries


def load_scifact_corpus(corpus_path: str) -> Dict[str, SciFacDoc]:
    """Load SciFact corpus from JSONL file."""
    corpus = {}
    with open(corpus_path, 'r') as f:
        for line in f:
            data = json.loads(line.strip())
            doc_id = data['_id']
            corpus[doc_id] = SciFacDoc(
                doc_id=doc_id,
                title=data.get('title', ''),
                text=data.get('text', '')
            )
    return corpus


def load_scifact_qrels(qrels_path: str) -> Dict[str, Dict[str, int]]:
    """Load SciFact relevance judgments (qrels)."""
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


# ============================================================================
# INDEX MANAGEMENT
# ============================================================================

def check_index_exists(client: OpenSearchClient, index_name: str) -> bool:
    """Check if index exists."""
    try:
        response = client.session.head(f"{client.base_url}/{index_name}")
        return response.status_code == 200
    except:
        return False


def get_index_doc_count(client: OpenSearchClient, index_name: str) -> int:
    """Get document count in index."""
    try:
        response = client.session.get(f"{client.base_url}/{index_name}/_count")
        if response.status_code == 200:
            return response.json().get('count', 0)
    except:
        pass
    return 0


def create_scifact_index(client: OpenSearchClient, index_name: str, embedding_dim: int = 768):
    """Create SciFact index with embedding field."""
    mapping = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100
            }
        },
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "text": {"type": "text"},
                "passage_embedding": {
                    "type": "knn_vector",
                    "dimension": embedding_dim,
                    "method": {
                        "name": "hnsw",
                        "space_type": "l2",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                }
            }
        }
    }
    
    response = client.session.put(
        f"{client.base_url}/{index_name}",
        json=mapping
    )
    
    if response.status_code in [200, 201]:
        print(f"  Created index: {index_name}")
        return True
    else:
        print(f"  Failed to create index: {response.status_code}")
        return False


def ingest_scifact_corpus(
    client: OpenSearchClient,
    index_name: str,
    corpus: Dict[str, SciFacDoc],
    embedding_model_id: str,
    batch_size: int = 50
):
    """Ingest SciFact corpus with embeddings."""
    doc_ids = list(corpus.keys())
    total = len(doc_ids)
    
    for i in range(0, total, batch_size):
        batch_ids = doc_ids[i:i + batch_size]
        
        # Prepare bulk request
        bulk_body = []
        for doc_id in batch_ids:
            doc = corpus[doc_id]
            bulk_body.append({"index": {"_index": index_name, "_id": doc_id}})
            bulk_body.append({
                "title": doc.title,
                "text": doc.text
            })
        
        # Execute bulk insert (without embeddings first)
        response = client.session.post(
            f"{client.base_url}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data="\n".join(json.dumps(item) for item in bulk_body) + "\n"
        )
        
        if response.status_code == 200:
            print(f"  Ingested {min(i + batch_size, total)}/{total} documents")
        else:
            print(f"  Bulk insert failed: {response.status_code}")
        
        time.sleep(0.1)
    
    # Now add embeddings using ingest pipeline or ML inference
    print("  Note: Embeddings need to be added separately via ML ingest pipeline")


# ============================================================================
# METRICS
# ============================================================================

def compute_ndcg(ranking: List[str], relevance: Dict[str, float], k: int) -> float:
    """Compute NDCG@k for a given ranking against relevance judgments."""
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
    
    return dcg / idcg if idcg > 0 else 0.0


def compute_recall(
    ranking: List[str], 
    relevance: Dict[str, float], 
    k: int,
    relevance_threshold: float = 1.0
) -> float:
    """Compute Recall@k."""
    if not ranking or not relevance:
        return 0.0
    
    relevant_in_topk = sum(1 for doc_id in ranking[:k] if relevance.get(doc_id, 0) >= relevance_threshold)
    total_relevant = sum(1 for score in relevance.values() if score >= relevance_threshold)
    
    return relevant_in_topk / total_relevant if total_relevant > 0 else 0.0


# ============================================================================
# LLM JUDGE
# ============================================================================

class LLMJudge:
    """LLM-based relevance judge using GPT-3.5 Turbo."""
    
    def __init__(self, cache_path: str = None):
        # OpenAI client is only needed if cache miss occurs
        self.client = None
        self.cache = {}
        self.nested_cache = {}  # For 3way format: {query_id: {doc_id: rating}}
        self.cache_path = cache_path
        self.cache_hits = 0
        self.cache_misses = 0
        
        if cache_path and os.path.exists(cache_path):
            with open(cache_path, 'r') as f:
                raw_cache = json.load(f)
            
            # Detect cache format and convert if needed
            if raw_cache:
                first_key = next(iter(raw_cache))
                first_value = raw_cache[first_key]
                
                # Check if it's the 3way format (nested with llm_ratings)
                if isinstance(first_value, dict) and 'llm_ratings' in first_value:
                    # Convert 3way format to flat format
                    for qid, data in raw_cache.items():
                        self.nested_cache[qid] = data.get('llm_ratings', {})
                        for doc_id, rating in data.get('llm_ratings', {}).items():
                            self.cache[f"{qid}_{doc_id}"] = rating
                    print(f"  Loaded {len(self.cache)} cached ratings (converted from 3way format)")
                else:
                    # Already in flat format
                    self.cache = raw_cache
                    print(f"  Loaded {len(self.cache)} cached ratings")
    
    def _ensure_client(self):
        """Lazy initialization of OpenAI client."""
        if self.client is None:
            try:
                self.client = OpenAI()
            except Exception as e:
                print(f"  WARNING: Could not initialize OpenAI client: {e}")
                return False
        return True
    
    def rate_relevance(self, query: str, doc_title: str, doc_text: str, query_id: str, doc_id: str) -> float:
        """Rate relevance of document to query on 0-1 scale."""
        cache_key = f"{query_id}_{doc_id}"
        if cache_key in self.cache:
            self.cache_hits += 1
            return self.cache[cache_key]
        
        # Also check nested cache
        if query_id in self.nested_cache and doc_id in self.nested_cache[query_id]:
            self.cache_hits += 1
            return self.nested_cache[query_id][doc_id]
        
        self.cache_misses += 1
        
        # Only call LLM if client is available
        if not self._ensure_client():
            print(f"  Cache miss for {cache_key}, returning default 0.5 (no OpenAI client)")
            return 0.5
        
        prompt = f"""Rate how relevant this scientific abstract is to the given claim.

CLAIM: {query}

ABSTRACT TITLE: {doc_title}
ABSTRACT: {doc_text[:1500]}

Rate relevance from 0.0 to 1.0 where:
- 0.0: Completely irrelevant
- 0.2: Barely related topic
- 0.4: Same general topic but doesn't address the claim
- 0.6: Related and partially addresses the claim
- 0.8: Highly relevant, mostly addresses the claim
- 1.0: Perfect match, directly addresses/supports/refutes the claim

Output ONLY a single number (0.0, 0.2, 0.4, 0.6, 0.8, or 1.0):"""
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=10
            )
            
            rating_str = response.choices[0].message.content.strip()
            rating = float(rating_str)
            rating = max(0.0, min(1.0, rating))
            
            self.cache[cache_key] = rating
            return rating
            
        except Exception as e:
            print(f"  LLM error: {e}")
            return 0.5
    
    def save_cache(self):
        """Save cache to file."""
        if self.cache_path and self.cache_misses > 0:
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=2)
            print(f"  Saved {len(self.cache)} ratings to cache")
        print(f"  Cache stats: {self.cache_hits} hits, {self.cache_misses} misses")


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
        self.lexical_fields = lexical_fields or ["title", "text"]
    
    def search(
        self,
        query: str,
        neural_weight: float,
        lexical_weight: float,
        size: int = 100
    ) -> List[Tuple[str, float]]:
        """Execute hybrid search with specified weights."""
        if neural_weight == 0.0:
            return self._pure_lexical_search(query, size)
        elif lexical_weight == 0.0:
            return self._pure_neural_search(query, size)
        
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
                return [(hit['_id'], hit['_score']) for hit in results.get('hits', {}).get('hits', [])]
            else:
                print(f"  [ERROR] Search failed: {response.status_code}")
                return []
        except Exception as e:
            print(f"  [ERROR] Search exception: {str(e)}")
            return []
    
    def _pure_lexical_search(self, query: str, size: int) -> List[Tuple[str, float]]:
        """Pure BM25 search."""
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
                return [(hit['_id'], hit['_score']) for hit in results.get('hits', {}).get('hits', [])]
            return []
        except Exception as e:
            print(f"  [ERROR] Lexical search exception: {str(e)}")
            return []
    
    def _pure_neural_search(self, query: str, size: int) -> List[Tuple[str, float]]:
        """Pure neural search."""
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
                return [(hit['_id'], hit['_score']) for hit in results.get('hits', {}).get('hits', [])]
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
    
    WEIGHTS = [round(w * 0.1, 1) for w in range(11)]  # [0.0, 0.1, ..., 1.0]
    LLM_RELEVANCE_THRESHOLD = 0.6
    
    def __init__(
        self,
        searcher: HybridSearcher,
        queries: Dict[str, SciFacQuery],
        corpus: Dict[str, SciFacDoc],
        human_qrels: Dict[str, Dict[str, int]],
        llm_judge: LLMJudge,
        debug: bool = False
    ):
        self.searcher = searcher
        self.queries = queries
        self.corpus = corpus
        self.human_qrels = human_qrels
        self.llm_judge = llm_judge
        self.debug = debug
        
        self.search_cache: Dict[Tuple[str, float], List[Tuple[str, float]]] = {}
        self.llm_ratings_cache: Dict[str, Dict[str, float]] = {}
    
    def run(self, query_ids: List[str]) -> Tuple[List[WeightMetrics], List[WeightMetrics]]:
        """Run grid search for specified queries."""
        human_metrics = []
        llm_metrics = []
        
        # First pass: collect all search results and LLM ratings
        print("\n[Collecting search results and LLM ratings...]")
        for i, qid in enumerate(query_ids):
            if i % 10 == 0:
                print(f"  Processing query {i+1}/{len(query_ids)}...")
            
            query = self.queries[qid]
            
            # Get pooled documents from all configurations
            pooled_docs = set()
            for neural_weight in self.WEIGHTS:
                lexical_weight = round(1.0 - neural_weight, 1)
                results = self._get_ranking(qid, query.text, neural_weight)
                for doc_id, _ in results[:25]:
                    pooled_docs.add(doc_id)
            
            # Get LLM ratings for pooled documents
            if qid not in self.llm_ratings_cache:
                self.llm_ratings_cache[qid] = {}
            
            for doc_id in pooled_docs:
                if doc_id in self.llm_ratings_cache[qid]:
                    continue
                if doc_id in self.corpus:
                    doc = self.corpus[doc_id]
                    rating = self.llm_judge.rate_relevance(
                        query.text, doc.title, doc.text, qid, doc_id
                    )
                    self.llm_ratings_cache[qid][doc_id] = rating
            
            # Save cache periodically
            if i % 20 == 0:
                self.llm_judge.save_cache()
        
        self.llm_judge.save_cache()
        
        # Second pass: compute metrics for each weight
        for neural_weight in self.WEIGHTS:
            lexical_weight = round(1.0 - neural_weight, 1)
            
            print(f"\n[Weight: neural={neural_weight}, lexical={lexical_weight}]")
            
            human_ndcg_1, human_ndcg_10, human_ndcg_25 = [], [], []
            human_recall_1, human_recall_10, human_recall_25 = [], [], []
            llm_ndcg_1, llm_ndcg_10, llm_ndcg_25 = [], [], []
            llm_recall_1, llm_recall_10, llm_recall_25 = [], [], []
            
            for qid in query_ids:
                results = self._get_ranking(qid, self.queries[qid].text, neural_weight)
                ranking = [doc_id for doc_id, _ in results]
                
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
                llm_rel = self.llm_ratings_cache.get(qid, {})
                if llm_rel:
                    llm_rel_scaled = {doc_id: score * 2 for doc_id, score in llm_rel.items()}
                    
                    llm_ndcg_1.append(compute_ndcg(ranking, llm_rel_scaled, k=1))
                    llm_ndcg_10.append(compute_ndcg(ranking, llm_rel_scaled, k=10))
                    llm_ndcg_25.append(compute_ndcg(ranking, llm_rel_scaled, k=25))
                    llm_recall_1.append(compute_recall(ranking, llm_rel, k=1, relevance_threshold=self.LLM_RELEVANCE_THRESHOLD))
                    llm_recall_10.append(compute_recall(ranking, llm_rel, k=10, relevance_threshold=self.LLM_RELEVANCE_THRESHOLD))
                    llm_recall_25.append(compute_recall(ranking, llm_rel, k=25, relevance_threshold=self.LLM_RELEVANCE_THRESHOLD))
            
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
    
    def _get_ranking(self, query_id: str, query_text: str, neural_weight: float) -> List[Tuple[str, float]]:
        """Get ranking from cache or execute search."""
        cache_key = (query_id, neural_weight)
        
        if cache_key in self.search_cache:
            return self.search_cache[cache_key]
        
        lexical_weight = round(1.0 - neural_weight, 1)
        results = self.searcher.search(
            query=query_text,
            neural_weight=neural_weight,
            lexical_weight=lexical_weight,
            size=100
        )
        
        self.search_cache[cache_key] = results
        return results


# ============================================================================
# OUTPUT
# ============================================================================

def print_results(human_metrics: List[WeightMetrics], llm_metrics: List[WeightMetrics], num_queries: int):
    """Print comprehensive results tables."""
    
    print("\n" + "="*100)
    print("SCIFACT WEIGHT GRID SEARCH RESULTS")
    print(f"Configuration: min_max + arithmetic_mean, {num_queries} queries")
    print("="*100)
    
    # Table A: Human Ground Truth
    print("\n[A] HUMAN GROUND TRUTH")
    print("-"*100)
    print(f"{'Weight':>8} | {'NDCG@1':>10} | {'NDCG@10':>10} | {'NDCG@25':>10} | {'Recall@1':>10} | {'Recall@10':>10} | {'Recall@25':>10}")
    print("-"*100)
    
    for m in human_metrics:
        print(f"{m.neural_weight:>8.1f} | {m.ndcg_1:>10.4f} | {m.ndcg_10:>10.4f} | {m.ndcg_25:>10.4f} | {m.recall_1:>10.4f} | {m.recall_10:>10.4f} | {m.recall_25:>10.4f}")
    
    print("-"*100)
    
    best_human = {
        'ndcg_1': max(human_metrics, key=lambda x: x.ndcg_1),
        'ndcg_10': max(human_metrics, key=lambda x: x.ndcg_10),
        'ndcg_25': max(human_metrics, key=lambda x: x.ndcg_25),
        'recall_1': max(human_metrics, key=lambda x: x.recall_1),
        'recall_10': max(human_metrics, key=lambda x: x.recall_10),
        'recall_25': max(human_metrics, key=lambda x: x.recall_25),
    }
    
    print("\nBest Weights (Human GT):")
    for metric, obj in best_human.items():
        val = getattr(obj, metric)
        print(f"  {metric.upper()}: {obj.neural_weight:.1f} ({val:.4f})")
    
    # Table B: LLM Ground Truth
    print("\n\n[B] LLM GROUND TRUTH (GPT-3.5 Turbo, relevance threshold >= 0.6)")
    print("-"*100)
    print(f"{'Weight':>8} | {'NDCG@1':>10} | {'NDCG@10':>10} | {'NDCG@25':>10} | {'Recall@1':>10} | {'Recall@10':>10} | {'Recall@25':>10}")
    print("-"*100)
    
    for m in llm_metrics:
        print(f"{m.neural_weight:>8.1f} | {m.ndcg_1:>10.4f} | {m.ndcg_10:>10.4f} | {m.ndcg_25:>10.4f} | {m.recall_1:>10.4f} | {m.recall_10:>10.4f} | {m.recall_25:>10.4f}")
    
    print("-"*100)
    
    best_llm = {
        'ndcg_1': max(llm_metrics, key=lambda x: x.ndcg_1),
        'ndcg_10': max(llm_metrics, key=lambda x: x.ndcg_10),
        'ndcg_25': max(llm_metrics, key=lambda x: x.ndcg_25),
        'recall_1': max(llm_metrics, key=lambda x: x.recall_1),
        'recall_10': max(llm_metrics, key=lambda x: x.recall_10),
        'recall_25': max(llm_metrics, key=lambda x: x.recall_25),
    }
    
    print("\nBest Weights (LLM GT):")
    for metric, obj in best_llm.items():
        val = getattr(obj, metric)
        print(f"  {metric.upper()}: {obj.neural_weight:.1f} ({val:.4f})")
    
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
    adjacent = 0
    for name, key in metrics_to_compare:
        human_best_w = best_human[key].neural_weight
        llm_best_w = best_llm[key].neural_weight
        diff = abs(human_best_w - llm_best_w)
        
        if diff == 0:
            match = "✅ Exact"
            agreements += 1
            adjacent += 1
        elif diff <= 0.1:
            match = "🔶 ±0.1"
            adjacent += 1
        else:
            match = "❌"
        
        print(f"{name:<12} | {human_best_w:>12.1f} | {llm_best_w:>12.1f} | {match:>10}")
    
    print("-"*60)
    print(f"\nExact Agreement: {agreements}/6 metrics ({100*agreements/6:.1f}%)")
    print(f"Adjacent (±0.1): {adjacent}/6 metrics ({100*adjacent/6:.1f}%)")
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
            "dataset": "SciFact",
            "domain": "Scientific claim verification",
            "normalization": "min_max",
            "combination": "arithmetic_mean",
            "llm_model": "gpt-3.5-turbo",
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
        description="SciFact Weight Grid Search Experiment"
    )
    parser.add_argument("--host", required=True, help="OpenSearch host")
    parser.add_argument("--port", type=int, default=80, help="OpenSearch port")
    parser.add_argument("--index", default="scifact", help="Index name")
    parser.add_argument("--embedding-model-id", required=True, help="Embedding model ID")
    parser.add_argument("--queries-path", default="datasets/scifact/queries.jsonl")
    parser.add_argument("--corpus-path", default="datasets/scifact/corpus.jsonl")
    parser.add_argument("--qrels-path", default="datasets/scifact/qrels/test.tsv")
    parser.add_argument("--cache-file", default="scifact_llm_cache_gpt35turbo.json", help="LLM cache file")
    parser.add_argument("--num-queries", type=int, default=100, help="Number of queries")
    parser.add_argument("--output", default="scifact_weight_grid_results.json", help="Output file")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    
    args = parser.parse_args()
    
    print("="*80)
    print("SCIFACT WEIGHT GRID SEARCH EXPERIMENT")
    print("Third Dataset Validation for LLM Ground Truth")
    print("="*80)
    print(f"OpenSearch: http://{args.host}:{args.port}")
    print(f"Index: {args.index}")
    print(f"Embedding Model: {args.embedding_model_id}")
    print(f"Queries: {args.num_queries}")
    print("="*80)
    
    # Load data
    print("\n[Loading data...]")
    queries = load_scifact_queries(args.queries_path)
    corpus = load_scifact_corpus(args.corpus_path)
    qrels = load_scifact_qrels(args.qrels_path)
    
    print(f"  Loaded {len(queries)} queries")
    print(f"  Loaded {len(corpus)} documents")
    print(f"  Loaded qrels for {len(qrels)} queries")
    
    # Initialize client
    client = OpenSearchClient(host=args.host, port=args.port)
    
    # Check if index exists
    if not check_index_exists(client, args.index):
        print(f"\n[WARNING] Index '{args.index}' does not exist!")
        print("  Please create and populate the index first.")
        print("  Example: Use beir/hybrid/data_ingestor.py")
        return 1
    
    doc_count = get_index_doc_count(client, args.index)
    print(f"  Index '{args.index}' has {doc_count} documents")
    
    # Filter to queries with qrels
    valid_query_ids = [qid for qid in queries.keys() if qid in qrels]
    sampled_ids = valid_query_ids[:args.num_queries]
    
    print(f"  Valid queries (with qrels): {len(valid_query_ids)}")
    print(f"  Using first {len(sampled_ids)} queries")
    
    # Initialize components
    searcher = HybridSearcher(
        client=client,
        index_name=args.index,
        embedding_model_id=args.embedding_model_id,
        neural_field="passage_embedding",
        lexical_fields=["passage_text", "title_key", "text_key"]
    )
    
    llm_judge = LLMJudge(cache_path=args.cache_file)
    
    # Run grid search
    print("\n[Running grid search...]")
    engine = GridSearchEngine(
        searcher=searcher,
        queries=queries,
        corpus=corpus,
        human_qrels=qrels,
        llm_judge=llm_judge,
        debug=args.debug
    )
    
    human_metrics, llm_metrics = engine.run(sampled_ids)
    
    # Print results
    print_results(human_metrics, llm_metrics, len(sampled_ids))
    
    # Save results
    save_results(human_metrics, llm_metrics, args.output)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
