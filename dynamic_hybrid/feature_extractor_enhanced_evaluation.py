#!/usr/bin/env python3
"""
Enhanced Evaluation Feature Extractor for O19S Dynamic Weight Prediction

This extractor provides query-only features that match the exact feature names
used during enhanced training (25 features) but without executing searches.

Matches training features:
- 20 corpus-aware features (query-only approximations)
- 5 O19S result-based features (set to 0 during evaluation)

Author: Dynamic Hybrid Search Team
Version: 1.0.0
"""

import re
import logging
import numpy as np
from typing import Dict, List

logger = logging.getLogger(__name__)


class EnhancedEvaluationFeatureExtractor:
    """
    Feature extractor for evaluation that matches enhanced training features.
    
    Provides query-only approximations for corpus features and zeros for result-based features.
    Exactly matches the 25 feature names used during enhanced training.
    """
    
    def __init__(self):
        """Initialize enhanced evaluation feature extractor"""
        
        self.stopwords = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with', 'this', 'but', 'they', 'have',
            'had', 'what', 'when', 'where', 'who', 'which', 'why', 'how'
        }
        
        logger.info("Initialized enhanced evaluation feature extractor (25 features)")
    
    def extract_features(self, query: str) -> Dict[str, float]:
        """
        Extract all 25 features that match enhanced training.
        
        Args:
            query: The search query
            
        Returns:
            Dictionary with exact feature names from enhanced training
        """
        features = {}
        
        # 1. Extract query-based approximations for corpus features (20 features)
        corpus_features = self._extract_corpus_approximations(query)
        features.update(corpus_features)
        
        # 2. Set result-based features to 0 (5 features)
        result_features = self._get_zero_result_features()
        features.update(result_features)
        
        return features
    
    def _extract_corpus_approximations(self, query: str) -> Dict[str, float]:
        """
        Extract query-only approximations for corpus-aware features.
        
        These approximate the corpus features without API calls.
        """
        features = {}
        
        # Basic query features
        features['query_length'] = len(query)
        
        tokens = query.lower().split()
        features['num_terms'] = len(tokens)
        features['has_numbers'] = float(bool(any(c.isdigit() for c in query)))
        features['has_special_chars'] = float(bool(any(c for c in query if not c.isalnum() and not c.isspace())))
        
        # Unique terms ratio
        unique_terms = set(tokens)
        features['unique_terms_ratio'] = len(unique_terms) / len(tokens) if tokens else 0
        
        # Stopword ratio
        stopword_count = sum(1 for token in tokens if token in self.stopwords)
        features['stopword_ratio'] = stopword_count / len(tokens) if tokens else 0
        
        # Capitalization ratio
        letter_count = sum(1 for char in query if char.isalpha())
        capital_count = sum(1 for char in query if char.isupper())
        features['capitalization_ratio'] = capital_count / letter_count if letter_count > 0 else 0
        
        # Has punctuation
        features['has_punctuation'] = float(any(c for c in query if c in '.,!?;:'))
        
        # Approximate corpus features using query-only heuristics
        if tokens:
            # Estimate document frequencies based on word characteristics
            estimated_dfs = []
            for token in tokens:
                # Heuristic: shorter, common words have higher DF
                if token in self.stopwords:
                    df = 50000  # High DF for stopwords
                elif len(token) <= 3:
                    df = 10000  # High DF for short words
                elif len(token) <= 6:
                    df = 5000   # Medium DF for medium words
                else:
                    df = 1000   # Lower DF for long words
                estimated_dfs.append(df)
            
            # Document frequency statistics
            features['max_document_frequency'] = max(estimated_dfs)
            features['min_document_frequency'] = min(estimated_dfs)
            features['total_document_frequency'] = sum(estimated_dfs)
            features['average_document_frequency'] = np.mean(estimated_dfs)
            features['variance_document_frequency'] = np.var(estimated_dfs)
            features['std_dev_document_frequency'] = np.std(estimated_dfs)
            
            # Estimate IDF values (assuming 100k total docs)
            total_docs = 100000
            idf_values = [np.log(total_docs / df) for df in estimated_dfs]
            
            # IDF statistics
            features['max_inverse_document_frequency'] = max(idf_values)
            features['min_inverse_document_frequency'] = min(idf_values)
            features['total_inverse_document_frequency'] = sum(idf_values)
            features['average_inverse_document_frequency'] = np.mean(idf_values)
            features['variance_inverse_document_frequency'] = np.var(idf_values)
            features['std_dev_inverse_document_frequency'] = np.std(idf_values)
        else:
            # Zero features for empty query
            zero_features = [
                'max_document_frequency', 'min_document_frequency', 'total_document_frequency',
                'average_document_frequency', 'variance_document_frequency', 'std_dev_document_frequency',
                'max_inverse_document_frequency', 'min_inverse_document_frequency', 'total_inverse_document_frequency',
                'average_inverse_document_frequency', 'variance_inverse_document_frequency', 'std_dev_inverse_document_frequency'
            ]
            for feature in zero_features:
                features[feature] = 0.0
        
        return features
    
    def _get_zero_result_features(self) -> Dict[str, float]:
        """
        Return zero values for O19S result-based features.
        
        These features are not available during evaluation (no search execution).
        """
        return {
            'num_results_kw_search': 0.0,
            'max_doc_score': 0.0,
            'sum_of_doc_scores': 0.0,
            'max_semantic_score': 0.0,
            'avg_semantic_score': 0.0
        }
    
    def get_feature_names(self) -> List[str]:
        """Get list of all 25 feature names that match enhanced training"""
        return [
            # Query and corpus features (20)
            'query_length', 'num_terms', 'has_numbers', 'has_special_chars',
            'unique_terms_ratio', 'stopword_ratio', 'capitalization_ratio', 'has_punctuation',
            'max_document_frequency', 'min_document_frequency', 'total_document_frequency',
            'average_document_frequency', 'variance_document_frequency', 'std_dev_document_frequency',
            'max_inverse_document_frequency', 'min_inverse_document_frequency', 'total_inverse_document_frequency',
            'average_inverse_document_frequency', 'variance_inverse_document_frequency', 'std_dev_inverse_document_frequency',
            
            # O19S result-based features (5) - set to 0 during evaluation
            'num_results_kw_search', 'max_doc_score', 'sum_of_doc_scores',
            'max_semantic_score', 'avg_semantic_score'
        ]
    
    def get_feature_count(self) -> int:
        """Get total number of features (25)"""
        return 25
