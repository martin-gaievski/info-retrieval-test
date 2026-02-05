"""
Coverage Analyzer for Inline LLM Judgment.

Analyzes judgment coverage and calculates NDCG metrics for hybrid
search configurations using pooled judgments.
"""

import math
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

from .pool_builder import PoolResult


@dataclass
class NDCGResult:
    """NDCG calculation result for a single configuration."""
    config_name: str
    ndcg_at_k: Dict[int, float]  # k -> NDCG@k
    coverage: float  # Fraction of top-k docs with judgments
    unjudged_count: int


@dataclass
class CoverageStats:
    """Coverage statistics for a query's judgment pool."""
    total_pooled_docs: int
    judged_docs: int
    unjudged_docs: int
    coverage_ratio: float
    config_coverage: Dict[str, float]  # per-config coverage


class CoverageAnalyzer:
    """
    Analyzes judgment coverage and calculates evaluation metrics.
    
    Handles the calculation of NDCG and other metrics while properly
    accounting for unjudged documents in the pooled judgment approach.
    """
    
    def __init__(
        self,
        k_values: List[int] = [5, 10, 20, 50, 100],
        default_unjudged_rating: float = 0.0
    ):
        """
        Initialize the coverage analyzer.
        
        Args:
            k_values: K values for NDCG@K calculation
            default_unjudged_rating: Rating for unjudged documents
        """
        self.k_values = k_values
        self.default_unjudged_rating = default_unjudged_rating
        
    def calculate_ndcg(
        self,
        ranked_doc_ids: List[str],
        judgments: Dict[str, float],
        k: int
    ) -> Tuple[float, int]:
        """
        Calculate NDCG@K for a ranked list.
        
        Args:
            ranked_doc_ids: Document IDs in ranked order
            judgments: Dict[doc_id] -> relevance rating
            k: Cutoff position
            
        Returns:
            Tuple of (NDCG score, count of unjudged docs)
        """
        # Get relevance scores for ranked docs
        relevances = []
        unjudged_count = 0
        
        for doc_id in ranked_doc_ids[:k]:
            if doc_id in judgments:
                relevances.append(judgments[doc_id])
            else:
                relevances.append(self.default_unjudged_rating)
                unjudged_count += 1
                
        # Calculate DCG
        dcg = self._dcg(relevances)
        
        # Calculate ideal DCG (best possible ranking)
        all_ratings = list(judgments.values())
        all_ratings.sort(reverse=True)
        ideal_relevances = all_ratings[:k]
        idcg = self._dcg(ideal_relevances)
        
        # Avoid division by zero
        if idcg == 0:
            return 0.0, unjudged_count
            
        return dcg / idcg, unjudged_count
    
    def _dcg(self, relevances: List[float]) -> float:
        """Calculate Discounted Cumulative Gain."""
        dcg = 0.0
        for i, rel in enumerate(relevances):
            # Using standard DCG formula: rel_i / log2(i + 2)
            dcg += rel / math.log2(i + 2)
        return dcg
    
    def analyze_pool_results(
        self,
        pool_results: List[PoolResult],
        judgments: Dict[str, float]
    ) -> Tuple[Dict[str, NDCGResult], CoverageStats]:
        """
        Analyze NDCG for all configurations and coverage statistics.
        
        Args:
            pool_results: Results from PoolBuilder
            judgments: Dict[doc_id] -> relevance rating
            
        Returns:
            Tuple of (config_name -> NDCGResult, CoverageStats)
        """
        # Calculate coverage statistics
        all_pooled = set()
        for pr in pool_results:
            all_pooled.update(pr.doc_ids)
            
        judged = set(judgments.keys())
        judged_in_pool = judged.intersection(all_pooled)
        
        config_coverage = {}
        for pr in pool_results:
            config_docs = set(pr.doc_ids)
            covered = len(config_docs.intersection(judged))
            config_coverage[pr.config.name] = covered / len(config_docs) if config_docs else 0.0
            
        coverage_stats = CoverageStats(
            total_pooled_docs=len(all_pooled),
            judged_docs=len(judged_in_pool),
            unjudged_docs=len(all_pooled) - len(judged_in_pool),
            coverage_ratio=len(judged_in_pool) / len(all_pooled) if all_pooled else 0.0,
            config_coverage=config_coverage
        )
        
        # Calculate NDCG for each configuration
        ndcg_results = {}
        for pr in pool_results:
            ndcg_at_k = {}
            total_unjudged = 0
            
            for k in self.k_values:
                ndcg, unjudged = self.calculate_ndcg(pr.doc_ids, judgments, k)
                ndcg_at_k[k] = ndcg
                if k == max(self.k_values):
                    total_unjudged = unjudged
                    
            ndcg_results[pr.config.name] = NDCGResult(
                config_name=pr.config.name,
                ndcg_at_k=ndcg_at_k,
                coverage=config_coverage[pr.config.name],
                unjudged_count=total_unjudged
            )
            
        return ndcg_results, coverage_stats
    
    def find_best_config(
        self,
        ndcg_results: Dict[str, NDCGResult],
        k: int = 10
    ) -> Tuple[str, float]:
        """
        Find the best configuration based on NDCG@K.
        
        Args:
            ndcg_results: Results from analyze_pool_results
            k: K value for comparison
            
        Returns:
            Tuple of (best config name, NDCG score)
        """
        best_config = None
        best_score = -1.0
        
        for config_name, result in ndcg_results.items():
            score = result.ndcg_at_k.get(k, 0.0)
            if score > best_score:
                best_score = score
                best_config = config_name
                
        return best_config, best_score
    
    def aggregate_results(
        self,
        query_results: Dict[str, Dict[str, NDCGResult]]
    ) -> Dict[str, Dict[int, float]]:
        """
        Aggregate NDCG results across multiple queries.
        
        Args:
            query_results: Dict[query] -> Dict[config_name -> NDCGResult]
            
        Returns:
            Dict[config_name] -> Dict[k -> avg_ndcg]
        """
        config_scores = {}
        
        for query, configs in query_results.items():
            for config_name, result in configs.items():
                if config_name not in config_scores:
                    config_scores[config_name] = {k: [] for k in self.k_values}
                    
                for k, score in result.ndcg_at_k.items():
                    config_scores[config_name][k].append(score)
                    
        # Calculate averages
        aggregated = {}
        for config_name, k_scores in config_scores.items():
            aggregated[config_name] = {
                k: sum(scores) / len(scores) if scores else 0.0
                for k, scores in k_scores.items()
            }
            
        return aggregated
    
    def generate_report(
        self,
        ndcg_results: Dict[str, NDCGResult],
        coverage_stats: CoverageStats,
        query: str
    ) -> str:
        """Generate a human-readable report."""
        
        lines = [
            f"=== Coverage Analysis Report ===",
            f"Query: {query}",
            f"",
            f"--- Coverage Statistics ---",
            f"Total pooled documents: {coverage_stats.total_pooled_docs}",
            f"Judged documents: {coverage_stats.judged_docs}",
            f"Unjudged documents: {coverage_stats.unjudged_docs}",
            f"Coverage ratio: {coverage_stats.coverage_ratio:.2%}",
            f"",
            f"--- Per-Config Coverage ---"
        ]
        
        for config_name, coverage in sorted(coverage_stats.config_coverage.items()):
            lines.append(f"  {config_name}: {coverage:.2%}")
            
        lines.extend([
            f"",
            f"--- NDCG Results ---"
        ])
        
        # Sort by NDCG@10
        sorted_results = sorted(
            ndcg_results.items(),
            key=lambda x: x[1].ndcg_at_k.get(10, 0),
            reverse=True
        )
        
        for config_name, result in sorted_results:
            lines.append(f"\n  {config_name}:")
            for k, score in sorted(result.ndcg_at_k.items()):
                lines.append(f"    NDCG@{k}: {score:.4f}")
                
        # Best config
        best_config, best_score = self.find_best_config(ndcg_results)
        lines.extend([
            f"",
            f"--- Best Configuration ---",
            f"  {best_config}: NDCG@10 = {best_score:.4f}"
        ])
        
        return "\n".join(lines)
