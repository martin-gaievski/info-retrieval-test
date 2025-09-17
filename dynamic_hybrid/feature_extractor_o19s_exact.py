"""
O19S Exact Feature Extractor - Uses full query string with termvectors API.
Matches the exact O19S methodology for corpus feature extraction.
"""

import math
import re
import string
import numpy as np
from typing import Dict, List, Optional
import logging
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)


class O19SExactFeatureExtractor:
    """
    Extract features using O19S exact methodology:
    - Query features (5): query_length, has_special_char, has_punctuation_at_end, 
                         capital_letters_ratio, stopwords_ratio
    - Corpus features (12): Statistics calculated from termvectors API using FULL query string
    """
    
    def __init__(self, 
                 client: OpenSearch,
                 index_name: str,
                 field_name: str = "product_title"):
        """
        Initialize the O19S exact feature extractor.
        
        Args:
            client: OpenSearch client
            index_name: Name of the index
            field_name: Field to get term statistics from
        """
        self.client = client
        self.index_name = index_name
        self.field_name = field_name
        
        # Get total document count once
        try:
            count_response = self.client.count(index=self.index_name)
            self.total_docs = count_response['count']
            logger.info(f"Total documents in index: {self.total_docs}")
        except Exception as e:
            logger.error(f"Failed to get document count: {e}")
            self.total_docs = 1  # Fallback to avoid division by zero
        
        # Common English stopwords (O19S set)
        self.stopwords = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with', 'the', 'this', 'but', 'they', 'have',
            'had', 'what', 'when', 'where', 'who', 'which', 'why', 'how'
        }
    
    def extract_features(self, query: str) -> Dict[str, float]:
        """
        Extract all features from the query using O19S exact methodology.
        
        Args:
            query: The search query
            
        Returns:
            Dictionary of feature names to values (17 features total)
        """
        features = {}
        
        # 1. Extract query string features (5 features)
        query_features = self._extract_query_features(query)
        features.update(query_features)
        
        # 2. Extract corpus-based features using FULL query string (12 features)
        corpus_features = self._extract_corpus_features_o19s(query)
        features.update(corpus_features)
        
        return features
    
    def _extract_query_features(self, query: str) -> Dict[str, float]:
        """Extract O19S query-only string features (5 features)"""
        features = {}
        
        # 1. Query length
        features['query_length'] = len(query)
        
        # 2. Has special characters
        features['has_special_chars'] = float(bool(re.search(r'[^a-zA-Z0-9\s]', query)))
        
        # 3. Has punctuation at the end
        features['has_punctuation'] = float(query.rstrip() and query.rstrip()[-1] in string.punctuation)
        
        # 4. Capitalization ratio
        letter_count = sum(1 for char in query if char.isalpha())
        capital_count = sum(1 for char in query if char.isupper())
        features['capitalization_ratio'] = capital_count / letter_count if letter_count > 0 else 0
        
        # 5. Stopword ratio
        tokens = query.lower().split()
        stopword_count = sum(1 for token in tokens if token in self.stopwords)
        features['stopword_ratio'] = stopword_count / len(tokens) if tokens else 0
        
        return features
    
    def _extract_corpus_features_o19s(self, query_string: str) -> Dict[str, float]:
        """
        Extract corpus-based features using O19S exact methodology.
        CRITICAL: Uses FULL query string with termvectors API, not individual terms!
        
        Args:
            query_string: The full query string
            
        Returns:
            Dictionary with 12 corpus features
        """
        try:
            # Construct the body for the _termvectors API using FULL query string
            body = {
                "doc": {
                    self.field_name: query_string  # FULL QUERY STRING, not individual terms!
                },
                "fields": [self.field_name],
                "term_statistics": True,   # Crucial to get doc_freq
                "field_statistics": True,  # Gives us total doc counts
            }
            
            # Make the API call to get term vectors
            term_vectors_response = self.client.termvectors(
                index=self.index_name, 
                body=body
            )
            
            # Process the response to extract term statistics
            doc_freqs = []
            idfs = []
            
            if 'term_vectors' in term_vectors_response and self.field_name in term_vectors_response['term_vectors']:
                terms = term_vectors_response['term_vectors'][self.field_name]['terms']
                
                for term, stats in terms.items():
                    doc_freq = stats.get('doc_freq', 0)
                    
                    # Calculate IDF using natural log (O19S method)
                    if doc_freq > 0:
                        idf = math.log(self.total_docs / doc_freq)
                    else:
                        idf = math.log(self.total_docs)  # Max IDF for unseen terms
                    
                    doc_freqs.append(doc_freq)
                    idfs.append(idf)
            
            # Calculate summary statistics for both DF and IDF
            if doc_freqs:
                # Document Frequency (DF) Stats
                max_df = max(doc_freqs)
                min_df = min(doc_freqs)
                sum_df = sum(doc_freqs)
                avg_df = sum_df / len(doc_freqs)
                variance_df = sum([(x - avg_df) ** 2 for x in doc_freqs]) / len(doc_freqs)
                std_dev_df = math.sqrt(variance_df)
                
                # Inverse Document Frequency (IDF) Stats
                max_idf = max(idfs)
                min_idf = min(idfs)
                sum_idf = sum(idfs)
                avg_idf = sum_idf / len(idfs)
                variance_idf = sum([(x - avg_idf) ** 2 for x in idfs]) / len(idfs)
                std_dev_idf = math.sqrt(variance_idf)
                
                return {
                    'max_document_frequency': max_df,
                    'min_document_frequency': min_df,
                    'total_document_frequency': sum_df,
                    'average_document_frequency': avg_df,
                    'variance_document_frequency': variance_df,
                    'std_dev_document_frequency': std_dev_df,
                    'max_inverse_document_frequency': max_idf,
                    'min_inverse_document_frequency': min_idf,
                    'total_inverse_document_frequency': sum_idf,
                    'average_inverse_document_frequency': avg_idf,
                    'variance_inverse_document_frequency': variance_idf,
                    'std_dev_inverse_document_frequency': std_dev_idf
                }
            else:
                # Return zeros if no terms found
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
                
        except Exception as e:
            logger.error(f"Failed to get term statistics for query '{query_string}': {e}")
            # Return zeros on error
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
    
    def get_feature_names(self) -> List[str]:
        """Get ordered list of feature names for O19S model."""
        return [
            # Query features (5)
            'query_length',
            'has_special_chars',
            'has_punctuation',
            'capitalization_ratio',
            'stopword_ratio',
            # Corpus features (12)
            'max_document_frequency',
            'min_document_frequency',
            'total_document_frequency',
            'average_document_frequency',
            'variance_document_frequency',
            'std_dev_document_frequency',
            'max_inverse_document_frequency',
            'min_inverse_document_frequency',
            'total_inverse_document_frequency',
            'average_inverse_document_frequency',
            'variance_inverse_document_frequency',
            'std_dev_inverse_document_frequency'
        ]
