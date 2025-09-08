"""
O19S Enhanced Feature Extractor - Exact Implementation Match

This implementation exactly matches the O19S Jupyter notebook implementation
for result-based features, adding them to existing corpus-aware features.

Key differences from my previous implementation:
1. Uses SearchArray for BM25 score calculation on retrieved titles (not OpenSearch scores)
2. Calculates scores only on retrieved subset, not whole index
3. Uses whitespace tokenizer exactly as O19S does
4. Semantic search uses k=100 but returns top 10 scores

Author: Dynamic Hybrid Search Team
Version: 2.0.0 - O19S Exact Match
"""

import json
import logging
import numpy as np
import pandas as pd
import requests
from typing import Dict, List, Optional
from opensearchpy import OpenSearch

# Import SearchArray for O19S-compatible BM25 scoring
try:
    from searcharray import SearchArray
except ImportError:
    SearchArray = None
    logging.warning("SearchArray not available - O19S result features will be disabled")

logger = logging.getLogger(__name__)


class O19SEnhancedFeatureExtractor:
    """
    Enhanced feature extractor that combines corpus-aware features with 
    O19S result-based features using exact O19S implementation.
    """
    
    def __init__(self, 
                 client: OpenSearch,
                 host: str = "localhost",
                 port: int = 9200,
                 index_name: str = "esci-products",
                 model_id: str = None,
                 cache_term_stats: bool = True):
        """
        Initialize the O19S enhanced feature extractor.
        
        Args:
            client: OpenSearch client
            host: OpenSearch host
            port: OpenSearch port
            index_name: Name of the index
            model_id: Neural model ID for semantic search
            cache_term_stats: Whether to cache term statistics
        """
        self.client = client
        self.host = host
        self.port = port
        self.index_name = index_name
        self.model_id = model_id
        self.cache_term_stats = cache_term_stats
        self.term_stats_cache = {}
        
        # Get total document count
        try:
            count_response = self.client.count(index=self.index_name)
            self.total_docs = count_response['count']
            logger.info(f"Total documents in index: {self.total_docs}")
        except Exception as e:
            logger.error(f"Failed to get document count: {e}")
            self.total_docs = 1
        
        # Common English stopwords
        self.stopwords = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with', 'this', 'but', 'they', 'have',
            'had', 'what', 'when', 'where', 'who', 'which', 'why', 'how'
        }
        
        # Check if we can use O19S result features
        self.can_use_result_features = (SearchArray is not None and model_id is not None)
        if not self.can_use_result_features:
            logger.warning("O19S result features disabled - missing SearchArray or model_id")
        
        logger.info(f"Initialized O19S enhanced feature extractor for {host}:{port}/{index_name}")
    
    def extract_features(self, query: str) -> Dict[str, float]:
        """
        Extract all features: corpus-aware + O19S result-based features.
        
        Args:
            query: The search query
            
        Returns:
            Dictionary of feature names to values
        """
        features = {}
        
        # 1. Extract corpus-aware features (existing implementation)
        corpus_features = self._extract_corpus_aware_features(query)
        features.update(corpus_features)
        
        # 2. Extract O19S result-based features (exact O19S implementation)
        if self.can_use_result_features:
            result_features = self._extract_o19s_result_features(query)
            features.update(result_features)
        else:
            # Add zero features if O19S features not available
            features.update({
                'num_results_kw_search': 0,
                'max_doc_score': 0,
                'sum_of_doc_scores': 0,
                'max_semantic_score': 0,
                'avg_semantic_score': 0
            })
        
        return features
    
    def _extract_corpus_aware_features(self, query: str) -> Dict[str, float]:
        """Extract corpus-aware features (existing implementation)"""
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
        
        # Corpus-based features using term statistics
        if tokens:
            doc_frequencies = []
            for term in tokens:
                df = self._get_term_document_frequency(term)
                doc_frequencies.append(df)
            
            # DF statistics
            features['max_document_frequency'] = max(doc_frequencies) if doc_frequencies else 0
            features['min_document_frequency'] = min(doc_frequencies) if doc_frequencies else 0
            features['total_document_frequency'] = sum(doc_frequencies)
            features['average_document_frequency'] = np.mean(doc_frequencies) if doc_frequencies else 0
            features['variance_document_frequency'] = np.var(doc_frequencies) if doc_frequencies else 0
            features['std_dev_document_frequency'] = np.std(doc_frequencies) if doc_frequencies else 0
            
            # IDF values
            idf_values = []
            for df in doc_frequencies:
                if df > 0:
                    idf = np.log(self.total_docs / df)
                else:
                    idf = np.log(self.total_docs)
                idf_values.append(idf)
            
            # IDF statistics
            features['max_inverse_document_frequency'] = max(idf_values) if idf_values else 0
            features['min_inverse_document_frequency'] = min(idf_values) if idf_values else 0
            features['total_inverse_document_frequency'] = sum(idf_values)
            features['average_inverse_document_frequency'] = np.mean(idf_values) if idf_values else 0
            features['variance_inverse_document_frequency'] = np.var(idf_values) if idf_values else 0
            features['std_dev_inverse_document_frequency'] = np.std(idf_values) if idf_values else 0
        else:
            # Zero features for empty query
            zero_features = [
                'max_document_frequency', 'min_document_frequency', 'total_document_frequency',
                'average_document_frequency', 'variance_document_frequency', 'std_dev_document_frequency',
                'max_inverse_document_frequency', 'min_inverse_document_frequency', 'total_inverse_document_frequency',
                'average_inverse_document_frequency', 'variance_inverse_document_frequency', 'std_dev_inverse_document_frequency'
            ]
            for feature in zero_features:
                features[feature] = 0
        
        return features
    
    def _extract_o19s_result_features(self, query: str) -> Dict[str, float]:
        """
        Extract O19S result-based features using exact O19S implementation.
        
        This matches the O19S Jupyter notebook implementation exactly:
        1. Get number of keyword search results
        2. Get titles for keyword search
        3. Calculate BM25 scores using SearchArray on retrieved titles
        4. Get semantic scores using neural search
        """
        features = {
            'num_results_kw_search': 0,
            'max_doc_score': 0,
            'sum_of_doc_scores': 0,
            'max_semantic_score': 0,
            'avg_semantic_score': 0
        }
        
        try:
            # 1. Get number of results for keyword search (exact O19S implementation)
            features['num_results_kw_search'] = self._num_results_kw_search(query)
            
            # 2. Get titles for BM25 score calculation (exact O19S implementation)
            titles_df = self._get_titles_for_query(query)
            
            # 3. Calculate BM25 scores using SearchArray (exact O19S implementation)
            if not titles_df.empty:
                features['max_doc_score'] = self._max_doc_score(titles_df, query)
                features['sum_of_doc_scores'] = self._sum_of_doc_scores(titles_df, query)
            
            # 4. Get semantic scores (exact O19S implementation)
            semantic_scores = self._get_semantic_scores_for_query(query)
            if semantic_scores:
                features['max_semantic_score'] = max(semantic_scores)
                features['avg_semantic_score'] = np.mean(semantic_scores)
            
        except Exception as e:
            logger.warning(f"Failed to extract O19S result features for query '{query[:50]}...': {e}")
        
        return features
    
    def _num_results_kw_search(self, query_string: str) -> int:
        """
        Get number of results for keyword search - exact O19S implementation.
        """
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            "_source": {
                "excludes": ["title_embedding"]
            },
            "query": {
                "multi_match": {
                    "type": "best_fields",
                    "fields": [
                        "product_id^100",
                        "product_bullet_point^3",
                        "product_color^2",
                        "product_brand^5",
                        "product_description",
                        "product_title^10"
                    ],
                    "operator": "and",
                    "query": query_string
                }
            },
            "track_total_hits": "true"
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            result = response.json()
            return result['hits']['total']['value']
        except Exception as e:
            logger.error(f"Failed to get keyword search results count: {e}")
            return 0
    
    def _get_titles_for_query(self, query_string: str) -> pd.DataFrame:
        """
        Get titles for query - exact O19S implementation.
        """
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            "_source": {
                "excludes": ["title_embedding"],
                "includes": "product_title"
            },
            "query": {
                "multi_match": {
                    "type": "best_fields",
                    "fields": [
                        "product_id^100",
                        "product_bullet_point^3",
                        "product_color^2",
                        "product_brand^5",
                        "product_description",
                        "product_title^10"
                    ],
                    "operator": "and",
                    "query": query_string
                }
            },
            "track_total_hits": "true"
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            result = response.json()
            
            titles = []
            num_results = result['hits']['total']['value']
            if num_results > 0:
                titles = [item['_source']['product_title'] for item in result['hits']['hits']]
            
            return pd.DataFrame(titles, columns=['title'])
            
        except Exception as e:
            logger.error(f"Failed to get titles for query: {e}")
            return pd.DataFrame()
    
    def _whitespace_to_lower(self, text: str) -> List[str]:
        """
        O19S tokenizer - exact implementation.
        """
        split = text.lower().split()
        return [token for token in split]
    
    def _max_doc_score(self, df: pd.DataFrame, query: str) -> float:
        """
        Calculate max BM25 score using SearchArray - exact O19S implementation.
        """
        if df.shape[0] == 0:
            return 0
        else:
            # Index the titles with the defined tokenizer
            df['title_indexed'] = SearchArray.index(df['title'], tokenizer=self._whitespace_to_lower)
            # Tokenize the query with the same tokenizer
            tokenized_query = df['title_indexed'].array.tokenizer(query)
            # Calculate the score for each query term
            scores = np.asarray([df['title_indexed'].array.score(query_term)
                               for query_term in tokenized_query])
            # Calculate the sum of all query terms per title
            sums = [sum(group) for group in zip(*scores)]
            # Return the maximum
            return max(sums)
    
    def _sum_of_doc_scores(self, df: pd.DataFrame, query: str) -> float:
        """
        Calculate sum of BM25 scores using SearchArray - exact O19S implementation.
        """
        if df.shape[0] == 0:
            return 0
        else:
            # Index the titles with the defined tokenizer
            df['title_indexed'] = SearchArray.index(df['title'], tokenizer=self._whitespace_to_lower)
            # Tokenize the query with the same tokenizer
            tokenized_query = df['title_indexed'].array.tokenizer(query)
            # Calculate the score for each query term
            scores = np.asarray([df['title_indexed'].array.score(query_term)
                               for query_term in tokenized_query])
            # Calculate the sum of all query terms per title
            sums = [sum(group) for group in zip(*scores)]
            # Return the sum of all title scores
            return sum(sums)
    
    def _get_semantic_scores_for_query(self, query_string: str) -> List[float]:
        """
        Get semantic scores - exact O19S implementation.
        """
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        payload = {
            "_source": {
                "excludes": ["title_embedding"],
                "includes": "product_title"
            },
            "query": {
                "neural": {
                    "title_embedding": {
                        "query_text": query_string,
                        "k": 100,  # O19S uses k=100
                        "model_id": self.model_id
                    }
                }
            },
            "size": 10  # But returns top 10
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            result = response.json()
            
            scores = []
            num_results = result['hits']['total']['value']
            if num_results > 0:
                scores = [item['_score'] for item in result['hits']['hits']]
            
            return scores
            
        except Exception as e:
            logger.error(f"Failed to get semantic scores: {e}")
            return []
    
    def _get_term_document_frequency(self, term: str) -> int:
        """Get document frequency for a term using termvectors API"""
        if self.cache_term_stats and term in self.term_stats_cache:
            return self.term_stats_cache[term]
        
        try:
            response = self.client.termvectors(
                index=self.index_name,
                body={
                    "doc": {"product_title": term},
                    "fields": ["product_title"],
                    "field_statistics": True,
                    "term_statistics": True,
                    "offsets": False,
                    "positions": False,
                    "payloads": False
                }
            )
            
            term_vectors = response.get('term_vectors', {})
            field_stats = term_vectors.get('product_title', {})
            terms = field_stats.get('terms', {})
            
            doc_freq = 0
            for analyzed_term, stats in terms.items():
                if analyzed_term.lower() == term.lower():
                    doc_freq = stats.get('doc_freq', 0)
                    break
            
            if doc_freq == 0 and terms:
                first_term_stats = next(iter(terms.values()))
                doc_freq = first_term_stats.get('doc_freq', 0)
            
            if self.cache_term_stats:
                self.term_stats_cache[term] = doc_freq
            
            return doc_freq
            
        except Exception as e:
            logger.warning(f"Failed to get term statistics for '{term}': {e}")
            return 0
    
    def get_feature_names(self) -> List[str]:
        """Get list of all feature names"""
        corpus_features = [
            'query_length', 'num_terms', 'has_numbers', 'has_special_chars',
            'unique_terms_ratio', 'stopword_ratio', 'capitalization_ratio', 'has_punctuation',
            'max_document_frequency', 'min_document_frequency', 'total_document_frequency',
            'average_document_frequency', 'variance_document_frequency', 'std_dev_document_frequency',
            'max_inverse_document_frequency', 'min_inverse_document_frequency', 'total_inverse_document_frequency',
            'average_inverse_document_frequency', 'variance_inverse_document_frequency', 'std_dev_inverse_document_frequency'
        ]
        
        o19s_features = [
            'num_results_kw_search', 'max_doc_score', 'sum_of_doc_scores',
            'max_semantic_score', 'avg_semantic_score'
        ]
        
        return corpus_features + o19s_features
    
    def get_feature_count(self) -> int:
        """Get total number of features"""
        return len(self.get_feature_names())
    
    def clear_cache(self):
        """Clear the term statistics cache"""
        self.term_stats_cache.clear()
        logger.info("Cleared term statistics cache")
