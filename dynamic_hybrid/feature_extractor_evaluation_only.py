#!/usr/bin/env python3
"""
Evaluation-Only Feature Extractor for O19S Dynamic Weight Prediction

This extractor avoids termvectors API calls during evaluation by using:
1. Basic query features only (no corpus API calls)
2. Default values for corpus features that require API calls
3. Compatible with models trained on full corpus-aware features

Author: Dynamic Hybrid Search Team
Version: 1.0.0
"""

import re
import logging
import numpy as np
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class EvaluationOnlyFeatureExtractor:
    """
    Lightweight feature extractor for evaluation that avoids API calls.
    
    Uses basic query features and defaults for corpus features.
    Compatible with models trained on corpus-aware features.
    """
    
    def __init__(self, feature_set: str = "o19s"):
        """
        Initialize evaluation-only feature extractor
        
        Args:
            feature_set: Feature set to use ("o19s" for 17 features)
        """
        self.feature_set = feature_set
        self.stopwords = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 
            'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 
            'will', 'would', 'could', 'should', 'may', 'might', 'must'
        }
        
        logger.info(f"Initialized evaluation-only extractor with feature_set='{feature_set}'")
    
    def extract_features(self, query_string: str) -> Dict[str, float]:
        """
        Extract features for evaluation without API calls
        
        Args:
            query_string: Query to extract features from
            
        Returns:
            Dictionary of feature values compatible with trained models
        """
        
        if self.feature_set == "o19s":
            return self._extract_o19s_features_evaluation_only(query_string)
        else:
            return self._extract_basic_features(query_string)
    
    def _extract_o19s_features_evaluation_only(self, query_string: str) -> Dict[str, float]:
        """
        Extract O19S-compatible features for evaluation without API calls.
        
        Uses query features + default values for corpus features.
        Must match the 17 features used in training.
        """
        
        # Extract basic query features (no API calls needed)
        query_features = self._extract_query_features(query_string)
        
        # Default values for corpus features that would require API calls
        # These are set to neutral/average values to minimize impact
        corpus_defaults = {
            'avg_doc_freq': 1000.0,  # Neutral document frequency
            'max_doc_freq': 5000.0,  # Conservative maximum
            'min_doc_freq': 100.0,   # Conservative minimum
            'total_doc_freq': 3000.0, # Sum estimate
            'doc_freq_variance': 500.0, # Moderate variance
            'rare_terms_count': 1.0,    # Assume some rare terms
            'common_terms_count': 2.0,  # Assume some common terms
            'doc_freq_entropy': 2.5,    # Neutral entropy
            'normalized_doc_freq_sum': 0.3, # Normalized average
            'max_normalized_doc_freq': 0.5, # Conservative max
            'doc_freq_range': 1000.0,   # Conservative range
            'doc_freq_coefficient_variation': 0.8 # Moderate variation
        }
        
        # Combine query features with corpus defaults
        features = {**query_features, **corpus_defaults}
        
        logger.debug(f"Extracted {len(features)} O19S evaluation features for query: '{query_string[:50]}...'")
        
        return features
    
    def _extract_query_features(self, query_string: str) -> Dict[str, float]:
        """Extract basic query-level features (no API calls)"""
        
        # Tokenize query
        words = self._tokenize_query(query_string)
        
        if not words:
            return {
                'query_length': 0.0,
                'avg_word_length': 0.0,
                'max_word_length': 0.0,
                'unique_terms_ratio': 0.0,
                'stopwords_ratio': 0.0
            }
        
        # Calculate query statistics
        word_lengths = [len(word) for word in words]
        unique_words = set(words)
        stopword_count = sum(1 for word in words if word.lower() in self.stopwords)
        
        features = {
            'query_length': float(len(words)),
            'avg_word_length': float(np.mean(word_lengths)),
            'max_word_length': float(max(word_lengths)),
            'unique_terms_ratio': float(len(unique_words) / len(words)),
            'stopwords_ratio': float(stopword_count / len(words))
        }
        
        return features
    
    def _extract_basic_features(self, query_string: str) -> Dict[str, float]:
        """Extract basic features (fallback for non-O19S feature sets)"""
        
        words = query_string.split()
        
        if not words:
            return {
                'query_length': 0,
                'avg_word_length': 0,
                'max_word_length': 0,
                'num_stopwords': 0,
                'has_numbers': 0,
                'has_quotes': 0,
                'has_special_chars': 0,
                'query_specificity': 0
            }
        
        return {
            'query_length': len(words),
            'avg_word_length': np.mean([len(w) for w in words]),
            'max_word_length': max([len(w) for w in words]),
            'num_stopwords': sum(1 for w in words if w.lower() in self.stopwords),
            'has_numbers': float(any(c.isdigit() for c in query_string)),
            'has_quotes': float('"' in query_string or "'" in query_string),
            'has_special_chars': float(any(c in query_string for c in ['!', '?', '$', '%', '&'])),
            'query_specificity': len(set(words)) / len(words)
        }
    
    def _tokenize_query(self, query_string: str) -> List[str]:
        """Tokenize query into words"""
        
        # Simple tokenization - split on whitespace and punctuation
        words = re.findall(r'\b\w+\b', query_string.lower())
        return [word for word in words if len(word) > 1]  # Filter very short words


# Convenience function for backward compatibility
def create_evaluation_extractor(feature_set: str = "o19s") -> EvaluationOnlyFeatureExtractor:
    """Create evaluation-only feature extractor"""
    return EvaluationOnlyFeatureExtractor(feature_set=feature_set)
