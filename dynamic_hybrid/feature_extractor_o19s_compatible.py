#!/usr/bin/env python3
"""
O19S-Compatible Feature Extractor for Corpus-Aware Dynamic Hybrid Search

This implementation extracts features WITHOUT using the termvectors API,
instead relying on search results similar to O19S approach.

Key differences from termvectors approach:
- Uses actual search queries to estimate document frequencies
- Relies on search result statistics rather than index metadata
- More similar to O19S methodology for direct comparison

Usage:
    extractor = O19SCompatibleFeatureExtractor(client, index_name, model_id)
    features = extractor.extract_features(query)
"""

import logging
import numpy as np
import re
from opensearchpy import OpenSearch
from typing import Dict, List, Tuple
import time

logger = logging.getLogger(__name__)


class O19SCompatibleFeatureExtractor:
    """Feature extractor that mimics O19S methodology without termvectors API"""
    
    def __init__(self, client: OpenSearch, index_name: str, model_id: str):
        self.client = client
        self.index_name = index_name
        self.model_id = model_id
        
        # Cache for document frequency estimates
        self._df_cache = {}
        
        # Get total document count once
        self.total_docs = self._get_total_document_count()
        
        # Feature names attribute (required by validation framework)
        self.feature_names = [
            # Query string features (8)
            "query_length",
            "query_terms", 
            "long_terms",
            "digit_count",
            "alpha_count",
            "special_chars",
            "unique_terms",
            "has_capitals",
            
            # Corpus features - search-based estimates (12)
            "mean_doc_freq",
            "max_doc_freq",
            "min_doc_freq", 
            "std_doc_freq",
            "mean_idf",
            "max_idf",
            "min_idf",
            "std_idf",
            "rarest_term_ratio",
            "most_common_ratio", 
            "rare_terms_count",
            "common_terms_count"
        ]
        
    def _get_total_document_count(self) -> int:
        """Get total document count using count API"""
        try:
            response = self.client.count(index=self.index_name)
            return response['count']
        except Exception as e:
            logger.error(f"Failed to get document count: {e}")
            return 1000000  # fallback estimate
    
    def _estimate_document_frequency_by_search(self, term: str, field: str = "product_title") -> int:
        """
        Estimate document frequency by running a term query (O19S-style approach)
        This mimics how O19S would get term statistics through search rather than termvectors
        """
        cache_key = f"{field}:{term}"
        if cache_key in self._df_cache:
            return self._df_cache[cache_key]
            
        try:
            # Use term query to find documents containing the exact term
            body = {
                "size": 0,  # We only want the count
                "query": {
                    "term": {
                        field: term.lower()  # Term queries are case sensitive
                    }
                },
                "track_total_hits": True
            }
            
            response = self.client.search(
                index=self.index_name,
                body=body
            )
            
            doc_freq = response['hits']['total']['value']
            self._df_cache[cache_key] = doc_freq
            return doc_freq
            
        except Exception as e:
            logger.warning(f"Failed to get document frequency for term '{term}': {e}")
            # Fallback: estimate based on term length and commonality
            if len(term) <= 3:
                return max(1, int(self.total_docs * 0.1))  # Common short terms
            else:
                return max(1, int(self.total_docs * 0.01))  # Less common longer terms
    
    def _calculate_idf(self, doc_freq: int) -> float:
        """Calculate IDF similar to how BM25 would calculate it"""
        if doc_freq == 0:
            return 0.0
        return np.log((self.total_docs + 1) / (doc_freq + 1))
    
    def _extract_query_string_features(self, query: str) -> List[float]:
        """Extract basic query string features (same as termvectors approach)"""
        features = []
        
        # Query length features
        features.append(len(query))
        features.append(len(query.split()))
        features.append(len([w for w in query.split() if len(w) > 3]))
        
        # Character type features
        features.append(sum(1 for c in query if c.isdigit()))
        features.append(sum(1 for c in query if c.isalpha()))
        features.append(sum(1 for c in query if not c.isalnum() and not c.isspace()))
        
        # Query complexity
        features.append(len(set(query.lower().split())))  # unique terms
        features.append(1 if any(c.isupper() for c in query) else 0)  # has capitals
        
        return features
    
    def _extract_corpus_features_search_based(self, query: str) -> List[float]:
        """
        Extract corpus features using search-based approach (O19S-style)
        This avoids termvectors API and uses search queries to estimate statistics
        """
        features = []
        terms = query.lower().split()
        
        if not terms:
            return [0.0] * 12  # Return zeros if no terms
        
        # Document frequency features for query terms
        doc_frequencies = []
        idf_values = []
        
        for term in terms:
            # Clean term (remove special characters)
            clean_term = re.sub(r'[^\w]', '', term)
            if clean_term:
                df = self._estimate_document_frequency_by_search(clean_term, "product_title")
                idf = self._calculate_idf(df)
                doc_frequencies.append(df)
                idf_values.append(idf)
        
        # Aggregate statistics
        if doc_frequencies:
            features.extend([
                np.mean(doc_frequencies),
                np.max(doc_frequencies), 
                np.min(doc_frequencies),
                np.std(doc_frequencies) if len(doc_frequencies) > 1 else 0.0
            ])
            
            features.extend([
                np.mean(idf_values),
                np.max(idf_values),
                np.min(idf_values), 
                np.std(idf_values) if len(idf_values) > 1 else 0.0
            ])
        else:
            features.extend([0.0] * 8)
        
        # Collection-level features (estimated)
        rarest_term_df = min(doc_frequencies) if doc_frequencies else 1
        most_common_df = max(doc_frequencies) if doc_frequencies else 1
        
        features.extend([
            rarest_term_df / self.total_docs,  # rarest term ratio
            most_common_df / self.total_docs,  # most common term ratio
            len([df for df in doc_frequencies if df < 100]),  # rare terms count
            len([df for df in doc_frequencies if df > self.total_docs * 0.1])  # common terms count
        ])
        
        return features
    
    def extract_features(self, query: str) -> Dict[str, float]:
        """
        Extract features for a query using O19S-compatible approach
        Returns dictionary of features (compatible with validation framework)
        """
        try:
            # Extract query string features (8 features)
            query_features = self._extract_query_string_features(query)
            
            # Extract corpus features using search-based approach (12 features)
            corpus_features = self._extract_corpus_features_search_based(query)
            
            # Combine all features
            all_features = query_features + corpus_features
            
            # Convert to dictionary with feature names
            features_dict = {}
            for i, feature_name in enumerate(self.feature_names):
                features_dict[feature_name] = float(all_features[i])
            
            return features_dict
            
        except Exception as e:
            logger.error(f"Error extracting features for query '{query}': {e}")
            # Return zero features dictionary on error
            return {name: 0.0 for name in self.feature_names}
    
    def extract_batch_features(self, queries: List[str], show_progress: bool = True) -> np.ndarray:
        """Extract features for a batch of queries"""
        features_list = []
        
        logger.info(f"Extracting O19S-compatible features for {len(queries)} queries...")
        
        for i, query in enumerate(queries):
            features = self.extract_features(query)
            features_list.append(features)
            
            if show_progress and i % 100 == 0:
                logger.info(f"Processed {i}/{len(queries)} queries")
        
        return np.array(features_list)
    
    def get_feature_names(self) -> List[str]:
        """Get names of all features for interpretability"""
        names = [
            # Query string features (8)
            "query_length",
            "query_terms", 
            "long_terms",
            "digit_count",
            "alpha_count",
            "special_chars",
            "unique_terms",
            "has_capitals",
            
            # Corpus features - search-based estimates (12)
            "mean_doc_freq",
            "max_doc_freq",
            "min_doc_freq", 
            "std_doc_freq",
            "mean_idf",
            "max_idf",
            "min_idf",
            "std_idf",
            "rarest_term_ratio",
            "most_common_ratio", 
            "rare_terms_count",
            "common_terms_count"
        ]
        return names
    
    def clear_cache(self):
        """Clear the document frequency cache"""
        self._df_cache.clear()
        logger.info("Cleared document frequency cache")


def main():
    """Test the O19S-compatible feature extractor"""
    from opensearchpy import OpenSearch, RequestsHttpConnection
    
    # Example usage
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        use_ssl=False,
        verify_certs=False,
        connection_class=RequestsHttpConnection
    )
    
    extractor = O19SCompatibleFeatureExtractor(
        client=client,
        index_name="esci-products",
        model_id="your-model-id"
    )
    
    # Test single query
    query = "wireless bluetooth headphones"
    features = extractor.extract_features(query)
    print(f"Query: {query}")
    print(f"Features shape: {features.shape}")
    print(f"Features: {features}")
    
    # Test feature names
    feature_names = extractor.get_feature_names()
    print(f"\nFeature names:")
    for i, name in enumerate(feature_names):
        print(f"{i+1:2d}. {name}: {features[i]:.4f}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
