"""
Inline LLM Judgment Module for Hybrid Search Optimization.

This module implements the inline judgment approach where LLM judgments are
generated on-the-fly during hybrid search optimization experiments. Instead
of pre-generating judgments and using exhaustive search, this approach:

1. Pools results from multiple hybrid search configurations
2. Deduplicates documents across configurations
3. Generates LLM judgments for the pooled document set
4. Evaluates each configuration using the same judgment set

This enables faster iteration and lower LLM costs while maintaining
evaluation quality comparable to exhaustive approaches.
"""

from .pool_builder import PoolBuilder
from .llm_judge import LLMJudge
from .coverage_analyzer import CoverageAnalyzer

__all__ = ['PoolBuilder', 'LLMJudge', 'CoverageAnalyzer']
