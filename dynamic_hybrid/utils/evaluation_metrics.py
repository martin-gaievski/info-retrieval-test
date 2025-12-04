"""
Evaluation metrics utilities for hybrid search.
Provides functions to compute NDCG and other metrics.
"""

import numpy as np
from typing import List, Dict, Optional


def compute_dcg_at_k(relevances: List[float], k: int) -> float:
    """
    Compute Discounted Cumulative Gain at k.
    
    Args:
        relevances: List of relevance scores in rank order
        k: Cutoff position
        
    Returns:
        DCG@k score
    """
    relevances = np.array(relevances[:k])
    if len(relevances) == 0:
        return 0.0
    
    # Using the standard DCG formula: rel_i / log2(i + 1)
    discounts = np.log2(np.arange(1, len(relevances) + 1) + 1)
    return np.sum(relevances / discounts)


def compute_ndcg_at_k(
    ranked_docs: List[str], 
    relevance_dict, 
    k: int,
    binary: bool = False
) -> float:
    """
    Compute Normalized Discounted Cumulative Gain at k.
    
    Args:
        ranked_docs: List of document IDs in rank order
        relevance_dict: Dictionary mapping doc_id to relevance score, or set of relevant doc_ids (for binary)
        k: Cutoff position
        binary: If True, treat all relevant docs as equally relevant (score=1)
        
    Returns:
        NDCG@k score
    """
    # Handle both set (binary) and dict (graded) inputs
    if isinstance(relevance_dict, set):
        # Convert set to dict for uniform processing
        relevance_dict_normalized = {doc_id: 1 for doc_id in relevance_dict}
        binary = True  # Force binary mode when set is passed
    else:
        relevance_dict_normalized = relevance_dict
    
    # Get relevance scores for ranked documents
    relevances = []
    for doc_id in ranked_docs[:k]:
        score = relevance_dict_normalized.get(doc_id, 0)
        if binary and score > 0:
            score = 1
        relevances.append(score)
    
    # Compute DCG
    dcg = compute_dcg_at_k(relevances, k)
    
    # Compute ideal DCG
    ideal_relevances = sorted(relevance_dict_normalized.values(), reverse=True)
    if binary:
        ideal_relevances = [1 if r > 0 else 0 for r in ideal_relevances]
    idcg = compute_dcg_at_k(ideal_relevances, k)
    
    # Compute NDCG
    if idcg == 0:
        return 0.0
    
    return dcg / idcg


def compute_metrics_at_k(
    ranked_docs: List[str], 
    relevance_dict, 
    k: int,
    binary: bool = False
) -> Dict[str, float]:
    """
    Compute multiple metrics at k.
    
    Args:
        ranked_docs: List of document IDs in rank order
        relevance_dict: Dictionary mapping doc_id to relevance score, or set of relevant doc_ids (for binary)
        k: Cutoff position
        binary: If True, treat all relevant docs as equally relevant
        
    Returns:
        Dictionary of metrics
    """
    # Handle both set (binary) and dict (graded) inputs
    if isinstance(relevance_dict, set):
        # Convert set to dict for uniform processing
        relevance_dict_normalized = {doc_id: 1 for doc_id in relevance_dict}
        binary = True  # Force binary mode when set is passed
    else:
        relevance_dict_normalized = relevance_dict
    
    metrics = {}
    
    # NDCG@k
    metrics[f'ndcg@{k}'] = compute_ndcg_at_k(ranked_docs, relevance_dict, k, binary)
    
    # Precision@k
    relevant_at_k = sum(1 for doc_id in ranked_docs[:k] if doc_id in relevance_dict_normalized and relevance_dict_normalized[doc_id] > 0)
    metrics[f'precision@{k}'] = relevant_at_k / k if k > 0 else 0.0
    
    # Recall@k
    total_relevant = sum(1 for score in relevance_dict_normalized.values() if score > 0)
    metrics[f'recall@{k}'] = relevant_at_k / total_relevant if total_relevant > 0 else 0.0
    
    # MRR (Mean Reciprocal Rank) - position of first relevant document
    for i, doc_id in enumerate(ranked_docs[:k], 1):
        if doc_id in relevance_dict_normalized and relevance_dict_normalized[doc_id] > 0:
            metrics['mrr'] = 1.0 / i
            break
    else:
        metrics['mrr'] = 0.0
    
    return metrics


def compute_average_metrics(
    results: List[Dict[str, any]], 
    k_values: Optional[List[int]] = None
) -> Dict[str, float]:
    """
    Compute average metrics across multiple queries.
    
    Args:
        results: List of result dictionaries, each containing 'metrics' field
        k_values: List of k values to compute averages for
        
    Returns:
        Dictionary of average metrics
    """
    if not results:
        return {}
    
    # Collect all metric names
    all_metrics = set()
    for result in results:
        if 'metrics' in result:
            all_metrics.update(result['metrics'].keys())
    
    # Compute averages
    avg_metrics = {}
    for metric_name in all_metrics:
        values = [r['metrics'].get(metric_name, 0.0) for r in results if 'metrics' in r]
        if values:
            avg_metrics[f'avg_{metric_name}'] = np.mean(values)
            avg_metrics[f'std_{metric_name}'] = np.std(values)
    
    return avg_metrics


def format_metrics_table(metrics: Dict[str, float], precision: int = 4) -> str:
    """
    Format metrics as a readable table.
    
    Args:
        metrics: Dictionary of metric names to values
        precision: Number of decimal places
        
    Returns:
        Formatted string table
    """
    if not metrics:
        return "No metrics available"
    
    lines = []
    lines.append("=" * 40)
    lines.append(f"{'Metric':<20} {'Value':>15}")
    lines.append("-" * 40)
    
    for metric, value in sorted(metrics.items()):
        if isinstance(value, float):
            value_str = f"{value:.{precision}f}"
        else:
            value_str = str(value)
        lines.append(f"{metric:<20} {value_str:>15}")
    
    lines.append("=" * 40)
    
    return "\n".join(lines)


def compare_models(
    baseline_metrics: Dict[str, float],
    model_metrics: Dict[str, float],
    precision: int = 4
) -> str:
    """
    Compare metrics between baseline and model.
    
    Args:
        baseline_metrics: Baseline metric values
        model_metrics: Model metric values
        precision: Number of decimal places
        
    Returns:
        Formatted comparison string
    """
    lines = []
    lines.append("=" * 60)
    lines.append(f"{'Metric':<20} {'Baseline':>12} {'Model':>12} {'Diff':>12}")
    lines.append("-" * 60)
    
    # Get all metrics
    all_metrics = set(baseline_metrics.keys()) | set(model_metrics.keys())
    
    for metric in sorted(all_metrics):
        baseline_val = baseline_metrics.get(metric, 0.0)
        model_val = model_metrics.get(metric, 0.0)
        diff = model_val - baseline_val
        
        # Format values
        baseline_str = f"{baseline_val:.{precision}f}"
        model_str = f"{model_val:.{precision}f}"
        
        # Format diff with sign
        if diff >= 0:
            diff_str = f"+{diff:.{precision}f}"
        else:
            diff_str = f"{diff:.{precision}f}"
        
        lines.append(f"{metric:<20} {baseline_str:>12} {model_str:>12} {diff_str:>12}")
    
    lines.append("=" * 60)
    
    return "\n".join(lines)
