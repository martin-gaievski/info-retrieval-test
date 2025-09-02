"""
Feature extractor with corpus-aware features for dynamic hybrid search.
Implements query-only features and corpus-based features without requiring search execution.
"""

import re
import string
import numpy as np
from typing import Dict, List, Set, Optional
from collections import Counter
import logging
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)


class CorpusAwareFeatureExtractor:
    """
    Extract features from queries including:
    1. Query string features (8 features)
    2. Corpus-based features using term statistics (12 features)
    """
    
    def __init__(self, 
                 client: OpenSearch,
                 index_name: str,
                 field_name: str = "product_title",
                 cache_term_stats: bool = True):
        """
        Initialize the corpus-aware feature extractor.
        
        Args:
            client: OpenSearch client
            index_name: Name of the index
            field_name: Field to get term statistics from
            cache_term_stats: Whether to cache term statistics
        """
        self.client = client
        self.index_name = index_name
        self.field_name = field_name
        self.cache_term_stats = cache_term_stats
        self.term_stats_cache = {}
        
        # Get total document count once
        try:
            count_response = self.client.count(index=self.index_name)
            self.total_docs = count_response['count']
            logger.info(f"Total documents in index: {self.total_docs}")
        except Exception as e:
            logger.error(f"Failed to get document count: {e}")
            self.total_docs = 1  # Fallback to avoid division by zero
        
        # Common English stopwords
        self.stopwords = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with', 'the', 'this', 'but', 'they', 'have',
            'had', 'what', 'when', 'where', 'who', 'which', 'why', 'how'
        }
        
    def extract_features(self, query: str) -> Dict[str, float]:
        """
        Extract all features from the query.
        
        Args:
            query: The search query
            
        Returns:
            Dictionary of feature names to values
        """
        features = {}
        
        # 1. Extract query string features
        query_features = self._extract_query_features(query)
        features.update(query_features)
        
        # 2. Extract corpus-based features
        corpus_features = self._extract_corpus_features(query)
        features.update(corpus_features)
        
        return features
    
    def _extract_query_features(self, query: str) -> Dict[str, float]:
        """Extract query-only string features"""
        features = {}
        
        # Basic features
        features['query_length'] = len(query)
        
        # Tokenize query
        tokens = query.lower().split()
        features['num_terms'] = len(tokens)
        
        # Check for numbers
        features['has_numbers'] = float(bool(re.search(r'\d', query)))
        
        # Check for special characters
        features['has_special_chars'] = float(bool(re.search(r'[^a-zA-Z0-9\s]', query)))
        
        # Additional features
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
        
        # Has punctuation at the end
        features['has_punctuation'] = float(query.rstrip() and query.rstrip()[-1] in string.punctuation)
        
        return features
    
    def _extract_corpus_features(self, query: str) -> Dict[str, float]:
        """Extract corpus-based features using term statistics"""
        features = {}
        
        # Tokenize query
        tokens = query.lower().split()
        
        if not tokens:
            # Return zero features for empty query
            return {
                'max_document_frequency': 0,
                'min_document_frequency': 0,
                'total_document_frequency': 0,
                'average_document_frequency': 0,
                'variance_document_frequency': 0,
                'std_dev_document_frequency': 0,
                'max_inverse_document_frequency': 0,
                'min_inverse_document_frequency': 0,
                'total_inverse_document_frequency': 0,
                'average_inverse_document_frequency': 0,
                'variance_inverse_document_frequency': 0,
                'std_dev_inverse_document_frequency': 0
            }
        
        # Get document frequencies for each term
        doc_frequencies = []
        for term in tokens:
            df = self._get_term_document_frequency(term)
            doc_frequencies.append(df)
        
        # Calculate DF statistics
        features['max_document_frequency'] = max(doc_frequencies) if doc_frequencies else 0
        features['min_document_frequency'] = min(doc_frequencies) if doc_frequencies else 0
        features['total_document_frequency'] = sum(doc_frequencies)
        features['average_document_frequency'] = np.mean(doc_frequencies) if doc_frequencies else 0
        features['variance_document_frequency'] = np.var(doc_frequencies) if doc_frequencies else 0
        features['std_dev_document_frequency'] = np.std(doc_frequencies) if doc_frequencies else 0
        
        # Calculate IDF values
        idf_values = []
        for df in doc_frequencies:
            if df > 0:
                idf = np.log(self.total_docs / df)
            else:
                # For terms not in corpus, use max IDF
                idf = np.log(self.total_docs)
            idf_values.append(idf)
        
        # Calculate IDF statistics
        features['max_inverse_document_frequency'] = max(idf_values) if idf_values else 0
        features['min_inverse_document_frequency'] = min(idf_values) if idf_values else 0
        features['total_inverse_document_frequency'] = sum(idf_values)
        features['average_inverse_document_frequency'] = np.mean(idf_values) if idf_values else 0
        features['variance_inverse_document_frequency'] = np.var(idf_values) if idf_values else 0
        features['std_dev_inverse_document_frequency'] = np.std(idf_values) if idf_values else 0
        
        return features
    
    def _get_term_document_frequency(self, term: str) -> int:
        """Get document frequency for a term"""
        # Check cache first
        if self.cache_term_stats and term in self.term_stats_cache:
            return self.term_stats_cache[term]
        
        try:
            # Use termvectors API to get term statistics
            response = self.client.termvectors(
                index=self.index_name,
                body={
                    "doc": {self.field_name: term},
                    "fields": [self.field_name],
                    "field_statistics": True,
                    "term_statistics": True,
                    "offsets": False,
                    "positions": False,
                    "payloads": False
                }
            )
            
            # Extract document frequency
            term_vectors = response.get('term_vectors', {})
            field_stats = term_vectors.get(self.field_name, {})
            terms = field_stats.get('terms', {})
            
            # The term might be analyzed differently, so we check all terms
            doc_freq = 0
            for analyzed_term, stats in terms.items():
                if analyzed_term.lower() == term.lower():
                    doc_freq = stats.get('doc_freq', 0)
                    break
            
            # If no exact match, take the first term (in case of analysis)
            if doc_freq == 0 and terms:
                first_term_stats = next(iter(terms.values()))
                doc_freq = first_term_stats.get('doc_freq', 0)
            
            # Cache the result
            if self.cache_term_stats:
                self.term_stats_cache[term] = doc_freq
            
            return doc_freq
            
        except Exception as e:
            logger.warning(f"Failed to get term statistics for '{term}': {e}")
            return 0
    
    def clear_cache(self):
        """Clear the term statistics cache"""
        self.term_stats_cache.clear()
        logger.info("Cleared term statistics cache")


class ESCICorpusAwareFeatureExtractor(CorpusAwareFeatureExtractor):
    """
    ESCI-specific corpus-aware feature extractor.
    Uses product_title field by default and includes ESCI-specific patterns.
    """
    
    def __init__(self, client: OpenSearch, index_name: str = "esci_products", cache_term_stats: bool = True):
        super().__init__(client, index_name, field_name="product_title", cache_term_stats=cache_term_stats)
        
        # ESCI-specific patterns
        self.size_patterns = re.compile(r'\b(\d+(?:\.\d+)?)\s*(gb|mb|tb|kg|g|mg|ml|l|oz|lb|inch|in|cm|mm|m)\b', re.IGNORECASE)
        self.model_number_pattern = re.compile(r'\b[A-Z0-9]{3,}[-]?[A-Z0-9]+\b')
    
    def _extract_query_features(self, query: str) -> Dict[str, float]:
        """Extract query features with ESCI-specific additions"""
        features = super()._extract_query_features(query)
        
        # Add ESCI-specific features
        features['has_size_specification'] = float(bool(self.size_patterns.search(query)))
        features['has_model_number'] = float(bool(self.model_number_pattern.search(query)))
        
        return features
