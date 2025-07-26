"""
Dynamic Hybrid Search BEIR Evaluation POC

This package provides tools for evaluating dynamic hybrid search
optimization on BEIR datasets with OpenSearch.
"""

__version__ = "0.1.0"

from .feature_extractor import (
    Domain,
    QueryFeatureExtractor,
    DomainAwareFeatureExtractor,
    get_domain_for_dataset
)

from .weight_predictor import (
    WeightPredictor,
    HeuristicWeightPredictor,
    DomainAwareWeightPredictor,
    MLWeightPredictor,
    get_predictor_for_dataset
)

__all__ = [
    'Domain',
    'QueryFeatureExtractor',
    'DomainAwareFeatureExtractor',
    'get_domain_for_dataset',
    'WeightPredictor',
    'HeuristicWeightPredictor',
    'DomainAwareWeightPredictor',
    'MLWeightPredictor',
    'get_predictor_for_dataset'
]
