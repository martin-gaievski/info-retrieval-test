"""
Pool Builder for Inline LLM Judgment.

This module implements multi-configuration pooling to gather documents
from diverse hybrid search configurations for LLM judgment.
"""

import sys
import os
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from collections import OrderedDict

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.opensearch_client import OpenSearchClient


@dataclass
class PoolConfig:
    """Configuration for a single pool variant."""
    name: str
    neural_weight: float
    lexical_weight: float
    normalization: str
    combination: str
    pool_size: int = 150
    # RRF-specific parameters
    is_rrf: bool = False
    rrf_rank_constant: int = 60


@dataclass 
class PoolResult:
    """Result from a single pool configuration."""
    config: PoolConfig
    doc_ids: List[str]
    doc_sources: Dict[str, Dict]  # doc_id -> source content
    raw_scores: Dict[str, float]  # doc_id -> score (if available)


class PoolBuilder:
    """
    Builds a pooled document set from multiple hybrid search configurations.
    
    The pool builder executes multiple diverse hybrid search configurations
    and collects their results into a deduplicated pool for LLM judgment.
    """
    
    # Default pool configurations - 6 diverse variants
    DEFAULT_CONFIGS = [
        # Standard weighted combinations
        PoolConfig("lexical_heavy", 0.3, 0.7, "min_max", "arithmetic_mean", 150),
        PoolConfig("balanced", 0.5, 0.5, "min_max", "arithmetic_mean", 150),
        PoolConfig("neural_heavy", 0.7, 0.3, "min_max", "arithmetic_mean", 150),
        
        # Different normalization/combination techniques
        PoolConfig("l2_harmonic", 0.5, 0.5, "l2", "harmonic_mean", 150),
        
        # RRF variants (rank-based, no weights)
        PoolConfig("rrf_k60", 0.5, 0.5, "rrf", "rrf", 150, is_rrf=True, rrf_rank_constant=60),
        PoolConfig("rrf_k20", 0.5, 0.5, "rrf", "rrf", 150, is_rrf=True, rrf_rank_constant=20),
    ]
    
    def __init__(
        self,
        client: OpenSearchClient,
        index_name: str,
        model_id: str,
        neural_field: str,
        lexical_fields: List[str],
        configs: Optional[List[PoolConfig]] = None
    ):
        """
        Initialize the pool builder.
        
        Args:
            client: OpenSearch client instance
            index_name: Name of the index to search
            model_id: Neural model ID for embedding
            neural_field: Field containing document embeddings
            lexical_fields: Fields to search with BM25
            configs: List of pool configurations (uses defaults if None)
        """
        self.client = client
        self.index_name = index_name
        self.model_id = model_id
        self.neural_field = neural_field
        self.lexical_fields = lexical_fields
        self.configs = configs or self.DEFAULT_CONFIGS
        
    def build_pool(
        self,
        query: str,
        fetch_sources: bool = True,
        source_fields: Optional[List[str]] = None,
        verbose: bool = False
    ) -> Tuple[Dict[str, Dict], List[PoolResult]]:
        """
        Build a pooled document set for a single query.
        
        Args:
            query: The query string
            fetch_sources: Whether to fetch document source content
            source_fields: Specific fields to fetch (None = all)
            verbose: Print progress for each config
            
        Returns:
            Tuple of:
                - pooled_docs: Dict[doc_id] -> {source content}
                - pool_results: List of PoolResult for each config
        """
        import time
        pooled_docs = OrderedDict()  # Maintains insertion order
        pool_results = []
        
        for i, config in enumerate(self.configs):
            if verbose:
                print(f"      [{i+1}/{len(self.configs)}] {config.name}...", end=" ", flush=True)
            
            start = time.time()
            result = self._execute_pool_config(
                query, config, fetch_sources, source_fields
            )
            elapsed = time.time() - start
            pool_results.append(result)
            
            if verbose:
                print(f"{len(result.doc_ids)} docs in {elapsed:.1f}s")
            
            # Add new documents to pool
            for doc_id in result.doc_ids:
                if doc_id not in pooled_docs:
                    pooled_docs[doc_id] = result.doc_sources.get(doc_id, {})
                    
        return dict(pooled_docs), pool_results
    
    def build_pool_batch(
        self,
        queries: List[str],
        fetch_sources: bool = True,
        source_fields: Optional[List[str]] = None
    ) -> Dict[str, Tuple[Dict[str, Dict], List[PoolResult]]]:
        """
        Build pooled document sets for multiple queries.
        
        Args:
            queries: List of query strings
            fetch_sources: Whether to fetch document source content
            source_fields: Specific fields to fetch
            
        Returns:
            Dict[query] -> (pooled_docs, pool_results)
        """
        results = {}
        for query in queries:
            results[query] = self.build_pool(query, fetch_sources, source_fields)
        return results
    
    def _execute_pool_config(
        self,
        query: str,
        config: PoolConfig,
        fetch_sources: bool,
        source_fields: Optional[List[str]]
    ) -> PoolResult:
        """Execute a single pool configuration."""
        
        if config.is_rrf:
            doc_ids, doc_sources, raw_scores = self._execute_rrf_search(
                query, config, fetch_sources, source_fields
            )
        else:
            doc_ids, doc_sources, raw_scores = self._execute_hybrid_search(
                query, config, fetch_sources, source_fields
            )
            
        return PoolResult(
            config=config,
            doc_ids=doc_ids,
            doc_sources=doc_sources,
            raw_scores=raw_scores
        )
    
    def _execute_hybrid_search(
        self,
        query: str,
        config: PoolConfig,
        fetch_sources: bool,
        source_fields: Optional[List[str]]
    ) -> Tuple[List[str], Dict[str, Dict], Dict[str, float]]:
        """Execute standard hybrid search with normalization pipeline."""
        
        query_body = self._build_hybrid_query(query, config, fetch_sources, source_fields)
        
        try:
            # Timeout: 30s connect, 120s read (neural queries can be slow)
            response = self.client.session.post(
                f"{self.client.base_url}/{self.index_name}/_search",
                json=query_body,
                timeout=(30, 120)
            )
            
            if response.status_code == 200:
                results = response.json()
                return self._parse_search_results(results, fetch_sources)
            else:
                # Print the actual error for debugging
                print(f"    [DEBUG] {config.name} search failed - HTTP {response.status_code}")
                try:
                    error_body = response.json()
                    error_type = error_body.get('error', {}).get('type', 'unknown')
                    error_reason = error_body.get('error', {}).get('reason', response.text[:200])
                    print(f"    [DEBUG] Error: {error_type}: {error_reason[:150]}")
                except:
                    print(f"    [DEBUG] Response: {response.text[:200]}")
        except Exception as e:
            print(f"    [DEBUG] Hybrid search exception for {config.name}: {str(e)}")
            
        return [], {}, {}
    
    def _execute_rrf_search(
        self,
        query: str,
        config: PoolConfig,
        fetch_sources: bool,
        source_fields: Optional[List[str]]
    ) -> Tuple[List[str], Dict[str, Dict], Dict[str, float]]:
        """Execute RRF (Reciprocal Rank Fusion) hybrid search."""
        
        query_body = self._build_rrf_query(query, config, fetch_sources, source_fields)
        
        try:
            # Timeout: 30s connect, 120s read (neural queries can be slow)
            response = self.client.session.post(
                f"{self.client.base_url}/{self.index_name}/_search",
                json=query_body,
                timeout=(30, 120)
            )
            
            if response.status_code == 200:
                results = response.json()
                return self._parse_search_results(results, fetch_sources)
            else:
                print(f"    [DEBUG] RRF {config.name} search failed - HTTP {response.status_code}")
                try:
                    error_body = response.json()
                    error_type = error_body.get('error', {}).get('type', 'unknown')
                    error_reason = error_body.get('error', {}).get('reason', response.text[:200])
                    print(f"    [DEBUG] Error: {error_type}: {error_reason[:150]}")
                except:
                    print(f"    [DEBUG] Response: {response.text[:200]}")
        except Exception as e:
            print(f"    [DEBUG] RRF search exception for {config.name}: {str(e)}")
            
        return [], {}, {}
    
    def _build_hybrid_query(
        self,
        query: str,
        config: PoolConfig,
        fetch_sources: bool,
        source_fields: Optional[List[str]]
    ) -> Dict:
        """Build hybrid query with normalization pipeline."""
        
        # Build normalization-processor config
        norm_processor = {
            "normalization-processor": {
                "normalization": {
                    "technique": config.normalization
                },
                "combination": {
                    "technique": config.combination,
                    "parameters": {
                        "weights": [config.lexical_weight, config.neural_weight]
                    }
                }
            }
        }
        
        query_body = {
            "size": config.pool_size,
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
                                    "model_id": self.model_id,
                                    "k": config.pool_size
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "phase_results_processors": [norm_processor]
            }
        }
        
        # Configure source fields
        if not fetch_sources:
            query_body["_source"] = False
        elif source_fields:
            query_body["_source"] = source_fields
            
        return query_body
    
    def _build_rrf_query(
        self,
        query: str,
        config: PoolConfig,
        fetch_sources: bool,
        source_fields: Optional[List[str]]
    ) -> Dict:
        """Build RRF hybrid query."""
        
        # RRF uses empty normalization/combination (defaults to RRF behavior)
        norm_processor = {
            "normalization-processor": {
                "normalization": {},
                "combination": {}
            }
        }
        
        query_body = {
            "size": config.pool_size,
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
                                    "model_id": self.model_id,
                                    "k": config.pool_size
                                }
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "phase_results_processors": [norm_processor]
            }
        }
        
        # Configure source fields
        if not fetch_sources:
            query_body["_source"] = False
        elif source_fields:
            query_body["_source"] = source_fields
            
        return query_body
    
    def _parse_search_results(
        self,
        results: Dict,
        fetch_sources: bool
    ) -> Tuple[List[str], Dict[str, Dict], Dict[str, float]]:
        """Parse search results into doc IDs, sources, and scores."""
        
        doc_ids = []
        doc_sources = {}
        raw_scores = {}
        
        for hit in results.get('hits', {}).get('hits', []):
            doc_id = hit['_id']
            doc_ids.append(doc_id)
            
            if fetch_sources and '_source' in hit:
                doc_sources[doc_id] = hit['_source']
                
            if '_score' in hit:
                raw_scores[doc_id] = hit['_score']
                
        return doc_ids, doc_sources, raw_scores
    
    def get_config_stats(self, pool_results: List[PoolResult]) -> Dict:
        """
        Calculate statistics about pool coverage across configurations.
        
        Args:
            pool_results: Results from build_pool()
            
        Returns:
            Dict with coverage statistics
        """
        all_docs = set()
        config_docs = {}
        
        for result in pool_results:
            config_docs[result.config.name] = set(result.doc_ids)
            all_docs.update(result.doc_ids)
            
        # Calculate unique contribution per config
        unique_contributions = {}
        for config_name, docs in config_docs.items():
            other_docs = set()
            for other_name, other_set in config_docs.items():
                if other_name != config_name:
                    other_docs.update(other_set)
            unique_contributions[config_name] = len(docs - other_docs)
            
        return {
            "total_unique_docs": len(all_docs),
            "config_doc_counts": {k: len(v) for k, v in config_docs.items()},
            "unique_contributions": unique_contributions,
            "raw_total": sum(len(r.doc_ids) for r in pool_results),
            "dedup_ratio": sum(len(r.doc_ids) for r in pool_results) / len(all_docs) if all_docs else 0
        }
