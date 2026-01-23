#!/usr/bin/env python3
"""
Proof of Concept: Offline Hybrid Search Optimization

This script demonstrates the "single query run + offline post-processing" approach:
1. Run lexical and neural queries ONCE on full corpus
2. Test ALL normalization/combination/weight configurations OFFLINE (pure math)
3. Compare results with OpenSearch's hybrid query for validation

Benefits:
- Run 2 queries per query instead of 11+ (one for each weight)
- All weight optimization is pure computation (milliseconds vs hours)
- Exact score reproduction when matching OpenSearch's normalization/combination

Based on: CORPUS_SUBSET_OPTIMIZATION_ANALYSIS.md Section 7
"""

import json
import pandas as pd
import numpy as np
import argparse
import os
import random
import requests
from collections import defaultdict
from opensearchpy import OpenSearch
from tqdm import tqdm
import time
import warnings
warnings.filterwarnings('ignore')

# Import BEIR dataset utilities from evaluate_generic_dynamic_predictor
from evaluate_generic_dynamic_predictor import (
    BEIR_DATASETS, download_beir_dataset, dataset_exists,
    load_test_queries, load_ratings
)


class OfflineHybridOptimizer:
    """
    Offline Hybrid Search Optimizer
    
    This class collects raw scores from separate neural/lexical queries,
    then performs normalization and combination OFFLINE to test different
    weight configurations without re-running OpenSearch queries.
    """
    
    def __init__(self, opensearch_host, opensearch_port, index_name, model_id,
                 neural_field='passage_embedding', lexical_fields=None):
        self.host = opensearch_host
        self.port = opensearch_port
        self.index_name = index_name
        self.model_id = model_id
        self.neural_field = neural_field
        self.lexical_fields = lexical_fields or ["title_key^2", "text_key"]
        
        self.client = OpenSearch(
            hosts=[{'host': opensearch_host, 'port': opensearch_port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        
        # Verify connection
        if not self.client.ping():
            raise Exception(f"Cannot connect to OpenSearch at {opensearch_host}:{opensearch_port}")
        
        print(f"Connected to OpenSearch at {opensearch_host}:{opensearch_port}")
        print(f"Index: {index_name}, Model: {model_id}")
        
        # Raw scores collected from queries: {query_id: {'neural': {doc_id: score}, 'lexical': {doc_id: score}}}
        self.raw_scores = {}
    
    # =========================================================================
    # PART 1: Raw Score Collection (runs queries once)
    # =========================================================================
    
    def execute_neural_search(self, query_text, size=100):
        """Execute neural-only search, return {doc_id: raw_score}"""
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            "_source": ["_id"],
            "query": {
                "neural": {
                    self.neural_field: {
                        "query_text": query_text,
                        "model_id": self.model_id,
                        "k": size
                    }
                }
            },
            "size": size
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            hits = result.get('hits', {}).get('hits', [])
            return {hit['_id']: hit.get('_score', 0.0) for hit in hits}
        except Exception as e:
            print(f"Neural search error: {e}")
            return {}
    
    def execute_lexical_search(self, query_text, size=100):
        """Execute lexical-only (BM25 multi_match) search, return {doc_id: raw_score}"""
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            "_source": ["_id"],
            "query": {
                "multi_match": {
                    "query": query_text,
                    "type": "best_fields",
                    "operator": "or",
                    "fields": self.lexical_fields
                }
            },
            "size": size
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            hits = result.get('hits', {}).get('hits', [])
            return {hit['_id']: hit.get('_score', 0.0) for hit in hits}
        except Exception as e:
            print(f"Lexical search error: {e}")
            return {}
    
    def execute_opensearch_hybrid(self, query_text, neural_weight, 
                                  normalization='l2', combination='arithmetic_mean', size=100):
        """Execute OpenSearch's hybrid query for comparison/validation"""
        lexical_weight = round(1.0 - neural_weight, 2)
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        # Handle edge cases
        payload = {
                "_source": {"excludes": [self.neural_field]},
                "query": {
                    "hybrid": {
                        "queries": [
                            {
                                "neural": {
                                    self.neural_field: {
                                        "query_text": query_text,
                                        "model_id": self.model_id,
                                        "k": 100
                                    }
                                }
                            },
                            {
                                "multi_match": {
                                    "query": query_text,
                                    "type": "best_fields",
                                    "operator": "or",
                                    "fields": self.lexical_fields
                                }
                            }
                        ]
                    }
                },
                "search_pipeline": {
                    "phase_results_processors": [
                        {
                            "normalization-processor": {
                                "normalization": {"technique": normalization},
                                "combination": {
                                    "technique": combination,
                                    "parameters": {"weights": [neural_weight, lexical_weight]}
                                }
                            }
                        }
                    ]
                },
                "size": size
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            hits = result.get('hits', {}).get('hits', [])
            # Return list of (doc_id, score) for ranking comparison
            return [(hit['_id'], hit.get('_score', 0.0)) for hit in hits]
        except Exception as e:
            print(f"Hybrid search error: {e}")
            return []
    
    def collect_raw_scores(self, queries, size=200):
        """
        Collect raw scores for all queries (ONE TIME operation).
        This is the only part that runs OpenSearch queries.
        
        Args:
            queries: dict of {query_id: query_text}
            size: number of results to retrieve per query
        
        Returns:
            raw_scores: {query_id: {'neural': {doc_id: score}, 'lexical': {doc_id: score}}}
        """
        print(f"\n{'='*70}")
        print("COLLECTING RAW SCORES (One-time OpenSearch queries)")
        print(f"{'='*70}")
        print(f"Running 2 queries per query (neural + lexical) for {len(queries)} queries...")
        print(f"This replaces {len(queries) * 11} queries needed for 11 weight combinations.\n")
        
        for query_id, query_text in tqdm(queries.items(), desc="Collecting scores"):
            neural_scores = self.execute_neural_search(query_text, size)
            lexical_scores = self.execute_lexical_search(query_text, size)
            
            self.raw_scores[query_id] = {
                'neural': neural_scores,
                'lexical': lexical_scores,
                'query_text': query_text
            }
        
        # Statistics
        total_neural_docs = sum(len(v['neural']) for v in self.raw_scores.values())
        total_lexical_docs = sum(len(v['lexical']) for v in self.raw_scores.values())
        avg_neural = total_neural_docs / len(self.raw_scores) if self.raw_scores else 0
        avg_lexical = total_lexical_docs / len(self.raw_scores) if self.raw_scores else 0
        
        print(f"\nScore collection complete!")
        print(f"  Queries: {len(self.raw_scores)}")
        print(f"  Avg neural docs/query: {avg_neural:.1f}")
        print(f"  Avg lexical docs/query: {avg_lexical:.1f}")
        
        return self.raw_scores
    
    # =========================================================================
    # PART 2: Offline Normalization (Python implementations matching OpenSearch)
    # Based on: neural-search/src/main/java/org/opensearch/neuralsearch/processor/normalization/
    # =========================================================================
    
    # Java constants
    L2_MIN_SCORE = 0.0  # From L2ScoreNormalizationTechnique.java
    MINMAX_MIN_SCORE = 0.001  # From MinMaxScoreNormalizationTechnique.java
    MINMAX_MAX_SCORE = 1.0  # From MinMaxScoreNormalizationTechnique.java
    MINMAX_SINGLE_RESULT_SCORE = 1.0  # From MinMaxScoreNormalizationTechnique.java
    
    @staticmethod
    def normalize_l2(scores):
        """
        L2 normalization - matches OpenSearch's L2ScoreNormalizationTechnique.java
        
        Java source: L2ScoreNormalizationTechnique.java
        Formula: n_score_i = score_i / sqrt(score1^2 + score2^2 + ... + scoren^2)
        
        Key behavior:
        - If l2_norm == 0, return MIN_SCORE (0.0)
        """
        if not scores:
            return {}
        
        values = np.array(list(scores.values()))
        l2_norm = np.sqrt(np.sum(values ** 2))
        
        if l2_norm == 0:
            return {doc_id: OfflineHybridOptimizer.L2_MIN_SCORE for doc_id in scores}
        
        return {doc_id: float(score / l2_norm) for doc_id, score in scores.items()}
    
    @staticmethod
    def normalize_min_max(scores):
        """
        Min-Max normalization - matches OpenSearch's MinMaxScoreNormalizationTechnique.java
        
        Java source: MinMaxScoreNormalizationTechnique.java
        Formula: nscore = (score - min_score) / (max_score - min_score)
        
        Key behaviors from Java:
        - MIN_SCORE = 0.001f (NOT 0.0!)
        - MAX_SCORE = 1.0f
        - SINGLE_RESULT_SCORE = 1.0f (when all scores are same)
        - If normalized score equals 0.0, return MIN_SCORE (0.001)
        """
        if not scores:
            return {}
        
        values = list(scores.values())
        min_score = min(values)
        max_score = max(values)
        
        # Edge case: single score or all scores are same
        if max_score == min_score:
            return {doc_id: OfflineHybridOptimizer.MINMAX_SINGLE_RESULT_SCORE for doc_id in scores}
        
        result = {}
        for doc_id, score in scores.items():
            normalized = (score - min_score) / (max_score - min_score)
            # Java: return normalizedScore == 0.0f ? MIN_SCORE : normalizedScore
            if normalized == 0.0:
                result[doc_id] = OfflineHybridOptimizer.MINMAX_MIN_SCORE
            else:
                result[doc_id] = float(normalized)
        
        return result
    
    # =========================================================================
    # PART 3: Offline Combination (Python implementations matching OpenSearch)
    # Based on: neural-search/src/main/java/org/opensearch/neuralsearch/processor/combination/
    # =========================================================================
    
    @staticmethod
    def combine_arithmetic_mean(neural_scores, lexical_scores, neural_weight, lexical_weight):
        """
        Arithmetic mean combination - matches OpenSearch's ArithmeticMeanScoreCombinationTechnique.java
        
        Java source: ArithmeticMeanScoreCombinationTechnique.java
        Formula: score = (weight1*score1 + weight2*score2) / (weight1 + weight2)
        
        Key behavior from Java:
        - Zero (0.0) scores ARE included (score >= 0.0 check)
        - Only weights of contributing scores (score >= 0) are summed
        - For docs appearing in only one result set, only that weight is used
        """
        all_docs = set(neural_scores.keys()) | set(lexical_scores.keys())
        
        combined = {}
        for doc_id in all_docs:
            has_neural = doc_id in neural_scores
            has_lexical = doc_id in lexical_scores
            
            n_score = neural_scores.get(doc_id, -1.0)  # -1 means not present
            l_score = lexical_scores.get(doc_id, -1.0)
            
            combined_score = 0.0
            sum_of_weights = 0.0
            
            # Java: if (score >= 0.0) - includes 0.0 scores
            if has_neural and n_score >= 0.0:
                combined_score += neural_weight * n_score
                sum_of_weights += neural_weight
            
            if has_lexical and l_score >= 0.0:
                combined_score += lexical_weight * l_score
                sum_of_weights += lexical_weight
            
            if sum_of_weights == 0.0:
                combined[doc_id] = 0.0
            else:
                combined[doc_id] = combined_score / sum_of_weights
        
        return combined
    
    @staticmethod
    def combine_geometric_mean(neural_scores, lexical_scores, neural_weight, lexical_weight):
        """
        Geometric mean combination - matches OpenSearch's GeometricMeanScoreCombinationTechnique.java
        
        Formula: combined = neural^neural_weight * lexical^lexical_weight
        
        Key behavior:
        - If any score is <= 0 (or missing), that component contributes 1.0 to product
        - Only weights of contributing scores are used
        """
        all_docs = set(neural_scores.keys()) | set(lexical_scores.keys())
        
        combined = {}
        for doc_id in all_docs:
            has_neural = doc_id in neural_scores
            has_lexical = doc_id in lexical_scores
            
            n_score = neural_scores.get(doc_id, 0.0)
            l_score = lexical_scores.get(doc_id, 0.0)
            
            # Geometric mean requires positive scores
            if has_neural and n_score > 0 and has_lexical and l_score > 0:
                # Both scores present and positive
                combined[doc_id] = (n_score ** neural_weight) * (l_score ** lexical_weight)
            elif has_neural and n_score > 0:
                # Only neural score
                combined[doc_id] = n_score ** neural_weight
            elif has_lexical and l_score > 0:
                # Only lexical score
                combined[doc_id] = l_score ** lexical_weight
            else:
                combined[doc_id] = 0.0
        
        return combined
    
    @staticmethod
    def combine_harmonic_mean(neural_scores, lexical_scores, neural_weight, lexical_weight):
        """
        Harmonic mean combination - matches OpenSearch's HarmonicMeanScoreCombinationTechnique.java
        
        Formula: combined = sum_of_weights / sum(weight_i / score_i)
        
        Key behavior:
        - Requires positive scores for contribution
        - Only weights of contributing scores are used in sum
        """
        all_docs = set(neural_scores.keys()) | set(lexical_scores.keys())
        
        combined = {}
        for doc_id in all_docs:
            has_neural = doc_id in neural_scores
            has_lexical = doc_id in lexical_scores
            
            n_score = neural_scores.get(doc_id, 0.0)
            l_score = lexical_scores.get(doc_id, 0.0)
            
            sum_of_weights = 0.0
            weighted_reciprocal_sum = 0.0
            
            if has_neural and n_score > 0:
                sum_of_weights += neural_weight
                weighted_reciprocal_sum += neural_weight / n_score
            
            if has_lexical and l_score > 0:
                sum_of_weights += lexical_weight
                weighted_reciprocal_sum += lexical_weight / l_score
            
            if weighted_reciprocal_sum == 0.0:
                combined[doc_id] = 0.0
            else:
                combined[doc_id] = sum_of_weights / weighted_reciprocal_sum
        
        return combined
    
    # =========================================================================
    # PART 4: Offline Hybrid Scoring (no OpenSearch needed!)
    # =========================================================================
    
    def compute_offline_hybrid(self, query_id, neural_weight, 
                               normalization='l2', combination='arithmetic_mean'):
        """
        Compute hybrid scores OFFLINE using collected raw scores.
        
        This is the core function that replaces OpenSearch hybrid queries.
        
        Returns:
            list of (doc_id, combined_score) sorted by score descending
        """
        if query_id not in self.raw_scores:
            raise ValueError(f"Query {query_id} not found - run collect_raw_scores first")
        
        lexical_weight = 1.0 - neural_weight
        
        # Get raw scores
        neural_raw = self.raw_scores[query_id]['neural']
        lexical_raw = self.raw_scores[query_id]['lexical']
        
        # Handle edge cases (pure neural or pure lexical)
        if neural_weight == 0.0:
            return sorted(lexical_raw.items(), key=lambda x: -x[1])
        elif lexical_weight == 0.0:
            return sorted(neural_raw.items(), key=lambda x: -x[1])
        
        # Apply normalization
        if normalization == 'l2':
            neural_norm = self.normalize_l2(neural_raw)
            lexical_norm = self.normalize_l2(lexical_raw)
        elif normalization == 'min_max':
            neural_norm = self.normalize_min_max(neural_raw)
            lexical_norm = self.normalize_min_max(lexical_raw)
        else:
            raise ValueError(f"Unknown normalization: {normalization}")
        
        # Apply combination
        if combination == 'arithmetic_mean':
            combined = self.combine_arithmetic_mean(
                neural_norm, lexical_norm, neural_weight, lexical_weight
            )
        elif combination == 'geometric_mean':
            combined = self.combine_geometric_mean(
                neural_norm, lexical_norm, neural_weight, lexical_weight
            )
        elif combination == 'harmonic_mean':
            combined = self.combine_harmonic_mean(
                neural_norm, lexical_norm, neural_weight, lexical_weight
            )
        else:
            raise ValueError(f"Unknown combination: {combination}")
        
        # Sort by combined score (descending)
        return sorted(combined.items(), key=lambda x: -x[1])
    
    # =========================================================================
    # PART 5: Evaluation
    # =========================================================================
    
    @staticmethod
    def compute_ndcg_at_k(ranked_docs, relevance_dict, k=10, binary=False):
        """Compute NDCG@k for a ranked list of documents."""
        if not ranked_docs:
            return 0.0
        
        # Extract doc_ids from (doc_id, score) tuples if needed
        if ranked_docs and isinstance(ranked_docs[0], tuple):
            ranked_docs = [doc_id for doc_id, _ in ranked_docs]
        
        relevance_scores = []
        for doc_id in ranked_docs[:k]:
            if binary:
                relevance_scores.append(1 if doc_id in relevance_dict else 0)
            else:
                relevance_scores.append(relevance_dict.get(doc_id, 0))
        
        dcg = 0.0
        for i, rel in enumerate(relevance_scores):
            dcg += (2**rel - 1) / np.log2(i + 2)
        
        if binary:
            num_relevant = len(relevance_dict)
            ideal_scores = [1] * min(k, num_relevant) + [0] * max(0, k - num_relevant)
        else:
            ideal_scores = sorted(relevance_dict.values(), reverse=True)[:k]
        
        idcg = 0.0
        for i, rel in enumerate(ideal_scores):
            idcg += (2**rel - 1) / np.log2(i + 2)
        
        return dcg / idcg if idcg > 0 else 0.0


def run_parity_validation(optimizer, queries, sample_size=10, 
                          normalizations=['l2'],
                          combinations=['arithmetic_mean'],
                          weights=[0.3, 0.5, 0.7],
                          verbose=False):
    """
    Validate that offline implementation matches OpenSearch's hybrid query results.
    """
    print(f"\n{'='*70}")
    print("PARITY VALIDATION: Offline vs OpenSearch Hybrid")
    print(f"{'='*70}")
    
    sample_queries = dict(list(queries.items())[:sample_size])
    
    total_tests = 0
    matches = 0
    mismatches = []
    detailed_analysis = []
    
    for norm in normalizations:
        for comb in combinations:
            for neural_weight in weights:
                for query_id, query_text in sample_queries.items():
                    if query_id not in optimizer.raw_scores:
                        continue
                    
                    # Get offline result
                    offline_result = optimizer.compute_offline_hybrid(
                        query_id, neural_weight, norm, comb
                    )
                    offline_ranking = [doc_id for doc_id, _ in offline_result[:10]]
                    offline_scores = {doc_id: score for doc_id, score in offline_result[:10]}
                    
                    # Get OpenSearch hybrid result
                    os_result = optimizer.execute_opensearch_hybrid(
                        query_text, neural_weight, norm, comb
                    )
                    os_ranking = [doc_id for doc_id, _ in os_result[:10]]
                    os_scores = {doc_id: score for doc_id, score in os_result[:10]}
                    
                    total_tests += 1
                    
                    # Calculate overlap metrics
                    offline_set = set(offline_ranking)
                    os_set = set(os_ranking)
                    overlap = len(offline_set & os_set)
                    
                    if offline_ranking == os_ranking:
                        matches += 1
                    else:
                        mismatches.append({
                            'query_id': query_id,
                            'neural_weight': neural_weight,
                            'normalization': norm,
                            'combination': comb,
                            'offline_top5': offline_ranking[:5],
                            'opensearch_top5': os_ranking[:5],
                            'top10_overlap': overlap
                        })
                    
                    # Store first detailed analysis
                    if len(detailed_analysis) < 3:
                        detailed_analysis.append({
                            'query_id': query_id,
                            'query_text': query_text[:50],
                            'neural_weight': neural_weight,
                            'normalization': norm,
                            'neural_docs': len(optimizer.raw_scores[query_id]['neural']),
                            'lexical_docs': len(optimizer.raw_scores[query_id]['lexical']),
                            'offline_result': offline_result[:5],
                            'os_result': os_result[:5],
                            'top10_overlap': overlap
                        })
    
    match_rate = (matches / total_tests * 100) if total_tests > 0 else 0
    
    print(f"\nResults:")
    print(f"  Total tests: {total_tests}")
    print(f"  Exact matches: {matches}")
    print(f"  Mismatches: {len(mismatches)}")
    print(f"  Match rate: {match_rate:.1f}%")
    
    # Calculate average overlap for mismatches
    if mismatches:
        avg_overlap = sum(m['top10_overlap'] for m in mismatches) / len(mismatches)
        print(f"  Avg top-10 overlap in mismatches: {avg_overlap:.1f}/10 docs")
    
    if verbose and detailed_analysis:
        print(f"\n{'='*70}")
        print("DETAILED ANALYSIS (first 3 queries)")
        print(f"{'='*70}")
        for da in detailed_analysis:
            print(f"\nQuery: {da['query_id']} ({da['query_text']}...)")
            print(f"  Config: neural_weight={da['neural_weight']}, norm={da['normalization']}")
            print(f"  Raw docs collected: neural={da['neural_docs']}, lexical={da['lexical_docs']}")
            print(f"  Top-10 overlap: {da['top10_overlap']}/10")
            print(f"\n  Offline Top-5:")
            for doc_id, score in da['offline_result']:
                print(f"    {doc_id}: {score:.6f}")
            print(f"\n  OpenSearch Top-5:")
            for doc_id, score in da['os_result']:
                print(f"    {doc_id}: {score:.6f}")
    
    return match_rate, mismatches


def run_offline_optimization(optimizer, ratings_data, 
                             normalizations=['l2', 'min_max'],
                             combinations=['arithmetic_mean'],
                             test_weights=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                             k_values=[1, 10, 25],
                             binary_relevance=False):
    """
    Run full weight optimization OFFLINE using collected raw scores.
    
    This demonstrates the massive speedup - testing all configurations
    without running any additional OpenSearch queries.
    """
    print(f"\n{'='*70}")
    print("OFFLINE WEIGHT OPTIMIZATION")
    print(f"{'='*70}")
    print("(No OpenSearch queries - pure Python computation)")
    
    # Prepare relevance data
    query_ratings = defaultdict(dict)
    for rating in ratings_data:
        qid = rating['query_id']
        did = rating['doc_id']
        if binary_relevance:
            query_ratings[qid][did] = 1
        else:
            query_ratings[qid][did] = rating['rating']
    
    # Calculate total configurations
    total_configs = len(normalizations) * len(combinations) * len(test_weights)
    
    print(f"\nConfiguration space:")
    print(f"  Normalizations: {normalizations}")
    print(f"  Combinations: {combinations}")
    print(f"  Test weights: {len(test_weights)}")
    print(f"  Total configurations: {total_configs}")
    print(f"  Queries with raw scores: {len(optimizer.raw_scores)}")
    
    results = []
    
    # Test all configurations
    for norm in normalizations:
        for comb in combinations:
            for neural_weight in test_weights:
                neural_weight = round(neural_weight, 2)
                lexical_weight = round(1.0 - neural_weight, 2)
                
                # Evaluate across all queries
                ndcg_totals = {k: 0.0 for k in k_values}
                query_count = 0
                
                for query_id in optimizer.raw_scores:
                    if query_id not in query_ratings:
                        continue
                    
                    # Compute hybrid scores OFFLINE
                    ranked_result = optimizer.compute_offline_hybrid(
                        query_id, neural_weight, norm, comb
                    )
                    
                    # Evaluate NDCG
                    relevance = query_ratings[query_id]
                    for k in k_values:
                        ndcg = optimizer.compute_ndcg_at_k(
                            ranked_result, relevance, k, binary_relevance
                        )
                        ndcg_totals[k] += ndcg
                    
                    query_count += 1
                
                if query_count > 0:
                    result = {
                        'normalization': norm,
                        'combination': comb,
                        'neural_weight': neural_weight,
                        'lexical_weight': lexical_weight,
                        'query_count': query_count
                    }
                    for k in k_values:
                        result[f'ndcg@{k}'] = ndcg_totals[k] / query_count
                    results.append(result)
    
    # Convert to DataFrame
    results_df = pd.DataFrame(results)
    
    # Find best configuration for each normalization/combination
    print(f"\n{'='*70}")
    print("BEST CONFIGURATIONS")
    print(f"{'='*70}")
    
    for norm in normalizations:
        for comb in combinations:
            subset = results_df[
                (results_df['normalization'] == norm) & 
                (results_df['combination'] == comb)
            ]
            if subset.empty:
                continue
            
            best_idx = subset['ndcg@10'].idxmax()
            best_row = subset.loc[best_idx]
            
            print(f"\n{norm} + {comb}:")
            print(f"  Best neural weight: {best_row['neural_weight']:.2f}")
            print(f"  Best lexical weight: {best_row['lexical_weight']:.2f}")
            for k in k_values:
                print(f"  NDCG@{k}: {best_row[f'ndcg@{k}']:.4f}")
    
    return results_df


def main():
    parser = argparse.ArgumentParser(
        description='POC: Offline Hybrid Search Optimization'
    )
    
    # OpenSearch configuration
    parser.add_argument('--opensearch-host', type=str, required=True,
                        help='OpenSearch host')
    parser.add_argument('--opensearch-port', type=int, default=80,
                        help='OpenSearch port')
    parser.add_argument('--index-name', type=str, required=True,
                        help='Index name')
    parser.add_argument('--model-id', type=str, required=True,
                        help='Neural model ID')
    parser.add_argument('--neural-field', type=str, default='passage_embedding',
                        help='Neural field name')
    parser.add_argument('--lexical-fields', type=str, nargs='+', 
                        default=['title_key^2', 'text_key'],
                        help='Lexical fields')
    
    # Dataset configuration
    parser.add_argument('--dataset-path', type=str, required=True,
                        help='Path to BEIR dataset')
    parser.add_argument('--binary-relevance', action='store_true',
                        help='Use binary relevance')
    parser.add_argument('--sample-size', type=int, default=None,
                        help='Number of queries to sample')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed')
    
    # Optimization options
    parser.add_argument('--normalizations', type=str, nargs='+', 
                        default=['l2', 'min_max'],
                        choices=['l2', 'min_max'],
                        help='Normalization techniques to test')
    parser.add_argument('--combinations', type=str, nargs='+',
                        default=['arithmetic_mean'],
                        choices=['arithmetic_mean', 'geometric_mean', 'harmonic_mean'],
                        help='Combination techniques to test')
    parser.add_argument('--test-weights', type=float, nargs='+',
                        default=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                        help='Neural weights to test (default: 0.0 to 1.0 step 0.1)')
    
    # Validation options
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip parity validation against OpenSearch')
    parser.add_argument('--validation-sample', type=int, default=10,
                        help='Number of queries for parity validation')
    parser.add_argument('--collection-size', type=int, default=100,
                        help='Number of documents to collect per query (neural + lexical)')
    
    # Output options
    parser.add_argument('--output-file', type=str, default=None,
                        help='Output CSV file for results')
    
    args = parser.parse_args()
    
    print("="*70)
    print("OFFLINE HYBRID SEARCH OPTIMIZATION - Proof of Concept")
    print("="*70)
    print("\nThis POC demonstrates:")
    print("  1. Run neural + lexical queries ONCE (2 queries per query)")
    print("  2. Test ALL weight/norm/combination configs OFFLINE (pure math)")
    print("  3. Validate results match OpenSearch's hybrid query")
    
    # Initialize optimizer
    optimizer = OfflineHybridOptimizer(
        opensearch_host=args.opensearch_host,
        opensearch_port=args.opensearch_port,
        index_name=args.index_name,
        model_id=args.model_id,
        neural_field=args.neural_field,
        lexical_fields=args.lexical_fields
    )
    
    # Check if dataset exists, auto-download if BEIR dataset
    dataset_path = args.dataset_path
    if not dataset_exists(dataset_path):
        dataset_name = os.path.basename(dataset_path)
        parent_dir = os.path.dirname(dataset_path) or 'datasets'
        
        if dataset_name in BEIR_DATASETS:
            print(f"\nDataset not found at {dataset_path}")
            dataset_path = download_beir_dataset(dataset_name, parent_dir)
        else:
            raise FileNotFoundError(f"Dataset not found: {dataset_path}")
    
    # Load queries and ratings
    print("\nLoading dataset...")
    test_queries = load_test_queries(dataset_path, requires_split=False)
    ratings_data = load_ratings(dataset_path)
    
    print(f"  Queries: {len(test_queries)}")
    print(f"  Ratings: {len(ratings_data)}")
    
    # Sample queries if requested
    if args.sample_size and args.sample_size < len(test_queries):
        print(f"\nSampling {args.sample_size} queries (seed={args.seed})...")
        random.seed(args.seed)
        # Sort keys to ensure deterministic ordering across runs
        sorted_keys = sorted(test_queries.keys())
        sampled_ids = random.sample(sorted_keys, args.sample_size)
        test_queries = {qid: test_queries[qid] for qid in sampled_ids}
    
    # STEP 1: Collect Raw Scores (runs 2 queries per query)
    collection_start = time.time()
    optimizer.collect_raw_scores(test_queries, size=args.collection_size)
    collection_time = time.time() - collection_start
    
    # STEP 2: Parity Validation (optional but recommended)
    # Uses same weights as offline/live experiments
    if not args.skip_validation:
        match_rate, mismatches = run_parity_validation(
            optimizer=optimizer,
            queries=test_queries,
            sample_size=args.validation_sample,
            normalizations=args.normalizations,
            combinations=args.combinations,
            weights=args.test_weights
        )
        
        if match_rate < 90:
            print(f"\n⚠️  WARNING: Low parity match rate ({match_rate:.1f}%)")
            print("   Offline results may not match OpenSearch hybrid query.")
    
    # STEP 3: Offline Weight Optimization
    optimization_start = time.time()
    
    results_df = run_offline_optimization(
        optimizer=optimizer,
        ratings_data=ratings_data,
        normalizations=args.normalizations,
        combinations=args.combinations,
        test_weights=args.test_weights,
        k_values=[1, 10, 25],
        binary_relevance=args.binary_relevance
    )
    
    optimization_time = time.time() - optimization_start
    
    # STEP 4: Validate optimal weights against OpenSearch
    validation_time = 0
    if not args.skip_validation:
        validation_start = time.time()
        print(f"\n{'='*70}")
        print("OPTIMAL WEIGHT VALIDATION")
        print(f"{'='*70}")
        print("Testing if offline optimization selects same optimal weight as OpenSearch")
        
        # Prepare relevance data for validation
        query_ratings = defaultdict(dict)
        for rating in ratings_data:
            qid = rating['query_id']
            did = rating['doc_id']
            query_ratings[qid][did] = 1 if args.binary_relevance else rating['rating']
        
        # Use ALL sampled queries for fair comparison (same as offline optimization)
        validation_queries = test_queries
        print(f"  Using {len(validation_queries)} queries (same as offline optimization)")
        
        for norm in args.normalizations:  # Test ALL normalizations
            for comb in args.combinations:  # Test ALL combinations
                print(f"\nTesting {norm} + {comb}:")
                
                # Get offline optimal weight
                subset = results_df[
                    (results_df['normalization'] == norm) & 
                    (results_df['combination'] == comb)
                ]
                if subset.empty:
                    continue
                
                best_idx = subset['ndcg@10'].idxmax()
                offline_best_weight = subset.loc[best_idx, 'neural_weight']
                offline_best_ndcg = subset.loc[best_idx, 'ndcg@10']
                
                # Test OpenSearch at ALL weight steps (same as offline optimization)
                print(f"  Running OpenSearch at weights {args.test_weights}...")
                os_results = {}
                
                # Also get offline NDCG at these same weights for comparison
                offline_results_at_weights = {}
                for w in args.test_weights:
                    subset_w = results_df[
                        (results_df['normalization'] == norm) & 
                        (results_df['combination'] == comb) &
                        (results_df['neural_weight'] == w)
                    ]
                    if not subset_w.empty:
                        offline_results_at_weights[w] = subset_w.iloc[0]['ndcg@10']
                    else:
                        # Need to compute if not in results_df
                        offline_results_at_weights[w] = None
                
                # Compute OpenSearch NDCG at all K values
                k_values = [1, 10, 25]
                os_results_all_k = {k: {} for k in k_values}  # {k: {weight: ndcg}}
                
                for neural_weight in args.test_weights:
                    os_ndcg_totals = {k: 0.0 for k in k_values}
                    query_count = 0
                    for query_id, query_text in validation_queries.items():
                        if query_id not in query_ratings:
                            continue
                        
                        os_result = optimizer.execute_opensearch_hybrid(
                            query_text, neural_weight, norm, comb
                        )
                        for k in k_values:
                            os_ndcg = optimizer.compute_ndcg_at_k(
                                os_result, query_ratings[query_id], k, args.binary_relevance
                            )
                            os_ndcg_totals[k] += os_ndcg
                        query_count += 1
                    
                    for k in k_values:
                        os_results_all_k[k][neural_weight] = os_ndcg_totals[k] / query_count if query_count > 0 else 0
                
                # For backward compatibility
                os_results = os_results_all_k[10]
                
                # Find OpenSearch optimal weight
                os_best_weight = max(os_results, key=os_results.get)
                os_best_ndcg = os_results[os_best_weight]
                
                # Get offline results for all K values
                offline_results_all_k = {k: {} for k in k_values}
                for w in args.test_weights:
                    w_rounded = round(w, 2)
                    # Use np.isclose for float comparison to handle precision issues
                    subset_w = results_df[
                        (results_df['normalization'] == norm) & 
                        (results_df['combination'] == comb) &
                        (np.isclose(results_df['neural_weight'], w_rounded, atol=0.001))
                    ]
                    if not subset_w.empty:
                        for k in k_values:
                            offline_results_all_k[k][w] = subset_w.iloc[0][f'ndcg@{k}']
                    else:
                        # Debug: weight not found in results
                        print(f"    [DEBUG] Weight {w_rounded} not found in offline results for {norm}+{comb}")
                
                # Print comparison for all K values
                for k in k_values:
                    print(f"\n  NDCG@{k} COMPARISON (Offline vs OpenSearch at same weights):")
                    print(f"    {'Weight':<8} {'Offline':<12} {'OpenSearch':<12} {'Delta':<12} {'Match?':<8}")
                    print(f"    {'-'*52}")
                    row_count = 0
                    for w in sorted(args.test_weights):
                        offline_ndcg = offline_results_all_k[k].get(w)
                        os_ndcg = os_results_all_k[k].get(w)
                        if offline_ndcg is not None and os_ndcg is not None:
                            delta = offline_ndcg - os_ndcg
                            match = "✅" if abs(delta) < 0.001 else "⚠️"
                            print(f"    {w:<8.1f} {offline_ndcg:<12.4f} {os_ndcg:<12.4f} {delta:+.4f}      {match}")
                            row_count += 1
                        else:
                            # Always print row even if values are missing
                            offline_str = f"{offline_ndcg:.4f}" if offline_ndcg is not None else "MISSING"
                            os_str = f"{os_ndcg:.4f}" if os_ndcg is not None else "MISSING"
                            print(f"    {w:<8.1f} {offline_str:<12} {os_str:<12} {'N/A':<12} ⚠️ DATA")
                            row_count += 1
                    if row_count == 0:
                        print(f"    [ERROR] No data found for NDCG@{k}")
                
                print(f"\n  OFFLINE OPTIMIZATION:")
                print(f"    Best weight: {offline_best_weight:.2f}")
                print(f"    Best NDCG@10: {offline_best_ndcg:.4f}")
                
                print(f"\n  OPENSEARCH OPTIMIZATION (ground truth):")
                for w, ndcg in sorted(os_results.items()):
                    marker = " <-- best" if w == os_best_weight else ""
                    print(f"    weight={w:.1f}: NDCG@10={ndcg:.4f}{marker}")
                print(f"    Best weight: {os_best_weight:.2f}")
                
                # Find best weights for each K for both offline and OpenSearch
                print(f"\n  {'='*60}")
                print(f"  BEST CONFIGURATION SUMMARY: {norm} + {comb}")
                print(f"  {'='*60}")
                print(f"  {'Mode':<12} {'NDCG@1':<20} {'NDCG@10':<20} {'NDCG@25':<20}")
                print(f"  {'-'*72}")
                
                # Find best for each K - Offline
                offline_best_per_k = {}
                for k in k_values:
                    best_w = max(offline_results_all_k[k], key=offline_results_all_k[k].get)
                    best_ndcg = offline_results_all_k[k][best_w]
                    offline_best_per_k[k] = (best_w, best_ndcg)
                
                # Find best for each K - OpenSearch
                os_best_per_k = {}
                for k in k_values:
                    best_w = max(os_results_all_k[k], key=os_results_all_k[k].get)
                    best_ndcg = os_results_all_k[k][best_w]
                    os_best_per_k[k] = (best_w, best_ndcg)
                
                # Print Offline row
                offline_cells = []
                for k in k_values:
                    w, ndcg = offline_best_per_k[k]
                    offline_cells.append(f"w={w:.1f} ({ndcg:.4f})")
                print(f"  {'Offline':<12} {offline_cells[0]:<20} {offline_cells[1]:<20} {offline_cells[2]:<20}")
                
                # Print Live row
                live_cells = []
                for k in k_values:
                    w, ndcg = os_best_per_k[k]
                    live_cells.append(f"w={w:.1f} ({ndcg:.4f})")
                print(f"  {'Live':<12} {live_cells[0]:<20} {live_cells[1]:<20} {live_cells[2]:<20}")
                
                # Print match status
                print(f"\n  Weight Match by K:")
                for k in k_values:
                    off_w, off_ndcg = offline_best_per_k[k]
                    live_w, live_ndcg = os_best_per_k[k]
                    match = "✅" if off_w == live_w else "⚠️"
                    gap = live_ndcg - off_ndcg
                    print(f"    NDCG@{k}: Offline w={off_w:.1f}, Live w={live_w:.1f} {match} | NDCG gap: {gap:+.4f}")
        
        validation_time = time.time() - validation_start
    
    # Summary statistics
    print(f"\n{'='*70}")
    print("PERFORMANCE SUMMARY")
    print(f"{'='*70}")
    print(f"\nScore collection time: {collection_time:.2f} seconds ({len(test_queries) * 2} queries)")
    print(f"Offline optimization time: {optimization_time:.2f} seconds ({len(results_df)} configs)")
    if validation_time > 0:
        num_validation_queries = len(test_queries) * 11  # 11 weights
        print(f"Validation time: {validation_time:.2f} seconds ({num_validation_queries} OpenSearch hybrid queries)")
    print(f"\nQuery reduction:")
    queries_if_online = len(test_queries) * len(results_df)
    print(f"  Online queries needed: {queries_if_online}")
    print(f"  Offline queries used: {len(test_queries) * 2}")
    print(f"  Reduction: {queries_if_online / (len(test_queries) * 2):.1f}x")
    
    # Save results
    if args.output_file:
        output_path = args.output_file
    else:
        dataset_name = os.path.basename(dataset_path)
        output_path = f'dynamic_hybrid/offline_optimization_results_{dataset_name}.csv'
    
    results_df.to_csv(output_path, index=False)
    print(f"\nResults saved to {output_path}")
    
    print(f"\n{'='*70}")
    print("POC COMPLETE")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
