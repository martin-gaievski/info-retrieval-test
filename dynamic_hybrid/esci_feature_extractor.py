"""
Simple feature extractor for ESCI dynamic weight prediction evaluation.
Extracts the 20 features used in the trained model.
"""

import numpy as np
from typing import Dict
from opensearchpy import OpenSearch


class ESCIFeatureExtractor:
    """Feature extractor for ESCI evaluation"""
    
    def __init__(self, client: OpenSearch, index_name: str, corpus_field: str = 'product_title'):
        """Initialize feature extractor"""
        self.client = client
        self.index_name = index_name
        self.corpus_field = corpus_field
    
    def extract_features(self, query: str, weight: float) -> Dict[str, float]:
        """
        Extract features for a query with a given neural weight.
        
        Returns dictionary with 20 features matching the training model:
        - f_0_neuralness: The neural weight itself
        - f_2_query_length: Character count
        - f_3_has_numbers: Binary feature for numbers
        - f_4_has_special_char: Binary feature for special characters
        - f_5_has_punctuation_at_end: Binary feature for ending punctuation
        - f_6_unique_terms_ratio: Ratio of unique terms
        - f_7_capital_letters_ratio: Ratio of capital letters
        - f_8_stopwords_ratio: Ratio of stopwords
        - f_14 to f_25: Document frequency statistics
        """
        
        features = {}
        
        # Feature 0: Neural weight
        features['f_0_neuralness'] = weight
        
        # Basic query features (2-8)
        features['f_2_query_length'] = len(query)
        features['f_3_has_numbers'] = 1.0 if any(c.isdigit() for c in query) else 0.0
        features['f_4_has_special_char'] = 1.0 if any(not c.isalnum() and not c.isspace() for c in query) else 0.0
        features['f_5_has_punctuation_at_end'] = 1.0 if query and query[-1] in '!?.,:;' else 0.0
        
        # Token-based features
        tokens = query.lower().split()
        unique_tokens = set(tokens)
        features['f_6_unique_terms_ratio'] = len(unique_tokens) / len(tokens) if tokens else 0.0
        
        # Capital letters ratio
        capital_count = sum(1 for c in query if c.isupper())
        alpha_count = sum(1 for c in query if c.isalpha())
        features['f_7_capital_letters_ratio'] = capital_count / alpha_count if alpha_count > 0 else 0.0
        
        # Stopwords ratio
        stopwords = {'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
                    'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
                    'to', 'was', 'will', 'with', 'the', 'this', 'what', 'when', 'where',
                    'who', 'why', 'how', 'which', 'can', 'should', 'would', 'could'}
        stopword_count = sum(1 for token in tokens if token in stopwords)
        features['f_8_stopwords_ratio'] = stopword_count / len(tokens) if tokens else 0.0
        
        # Document frequency features (14-25)
        # Get term statistics from OpenSearch
        doc_frequencies = []
        inv_doc_frequencies = []
        
        for token in unique_tokens:
            try:
                # Get document frequency for the term
                response = self.client.search(
                    index=self.index_name,
                    body={
                        "size": 0,
                        "query": {
                            "term": {
                                self.corpus_field: token
                            }
                        }
                    }
                )
                
                total_docs = response['hits']['total']['value']
                doc_freq = total_docs if total_docs > 0 else 0
                doc_frequencies.append(doc_freq)
                
                # Calculate inverse document frequency
                # Using standard IDF formula with smoothing
                total_corpus_size = 10000  # Approximate corpus size
                idf = np.log((total_corpus_size + 1) / (doc_freq + 1))
                inv_doc_frequencies.append(idf)
                
            except Exception:
                # If we can't get stats, use defaults
                doc_frequencies.append(0)
                inv_doc_frequencies.append(0)
        
        # If no tokens, use zeros
        if not doc_frequencies:
            doc_frequencies = [0]
            inv_doc_frequencies = [0]
        
        # Document frequency statistics
        features['f_14_max_document_frequency'] = max(doc_frequencies)
        features['f_15_min_document_frequency'] = min(doc_frequencies)
        features['f_16_total_document_frequency'] = sum(doc_frequencies)
        features['f_17_average_document_frequency'] = np.mean(doc_frequencies)
        features['f_18_variance_document_frequency'] = np.var(doc_frequencies)
        features['f_19_std_dev_document_frequency'] = np.std(doc_frequencies)
        
        # Inverse document frequency statistics
        features['f_20_max_inverse_document_frequency'] = max(inv_doc_frequencies)
        features['f_21_min_inverse_document_frequency'] = min(inv_doc_frequencies)
        features['f_22_total_inverse_document_frequency'] = sum(inv_doc_frequencies)
        features['f_23_average_inverse_document_frequency'] = np.mean(inv_doc_frequencies)
        features['f_24_variance_inverse_document_frequency'] = np.var(inv_doc_frequencies)
        features['f_25_std_dev_inverse_document_frequency'] = np.std(inv_doc_frequencies)
        
        return features
