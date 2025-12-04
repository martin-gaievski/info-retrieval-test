"""
OpenSearch client utilities for hybrid search.
Provides functions to connect and perform searches with OpenSearch.
"""

import requests
import json
from typing import Dict, List, Optional, Tuple
import numpy as np


class OpenSearchClient:
    """OpenSearch client for hybrid search operations."""
    
    def __init__(self, host: str, port: int = 80, use_ssl: bool = False):
        """
        Initialize OpenSearch client.
        
        Args:
            host: OpenSearch host
            port: OpenSearch port (default 80)
            use_ssl: Whether to use HTTPS
        """
        protocol = "https" if use_ssl else "http"
        self.base_url = f"{protocol}://{host}:{port}"
        self.session = requests.Session()
        
    def test_connection(self) -> bool:
        """Test connection to OpenSearch."""
        try:
            response = self.session.get(self.base_url)
            return response.status_code == 200
        except:
            return False
    
    def get_index_info(self, index_name: str) -> Optional[Dict]:
        """Get information about an index."""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}")
            if response.status_code == 200:
                return response.json()
        except:
            pass
        return None
    
    def lexical_search(
        self, 
        index: str, 
        query: str, 
        fields: List[str], 
        size: int = 100
    ) -> List[str]:
        """
        Perform lexical (BM25) search.
        
        Args:
            index: Index name
            query: Query text
            fields: List of fields to search
            size: Number of results to return
            
        Returns:
            List of document IDs in rank order
        """
        query_body = {
            "size": size,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": fields,
                    "type": "best_fields"
                }
            }
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/{index}/_search",
                json=query_body
            )
            
            if response.status_code == 200:
                results = response.json()
                return [hit['_id'] for hit in results.get('hits', {}).get('hits', [])]
        except Exception as e:
            print(f"Lexical search error: {str(e)}")
        
        return []
    
    def neural_search(
        self, 
        index: str, 
        query: str, 
        model_id: str,
        neural_field: str,
        size: int = 100
    ) -> List[str]:
        """
        Perform neural (semantic) search.
        
        Args:
            index: Index name
            query: Query text
            model_id: Neural model ID
            neural_field: Field containing embeddings
            size: Number of results to return
            
        Returns:
            List of document IDs in rank order
        """
        query_body = {
            "size": size,
            "query": {
                "neural": {
                    neural_field: {
                        "query_text": query,
                        "model_id": model_id,
                        "k": size
                    }
                }
            }
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/{index}/_search",
                json=query_body
            )
            
            if response.status_code == 200:
                results = response.json()
                return [hit['_id'] for hit in results.get('hits', {}).get('hits', [])]
        except Exception as e:
            print(f"Neural search error: {str(e)}")
        
        return []
    
    def hybrid_search(
        self,
        index: str,
        query: str,
        model_id: str,
        neural_field: str,
        lexical_fields: List[str],
        neural_weight: float,
        lexical_weight: float,
        normalization: str = "min_max",
        combination: str = "arithmetic_mean",
        size: int = 100
    ) -> List[str]:
        """
        Perform hybrid search combining neural and lexical.
        
        Args:
            index: Index name
            query: Query text
            model_id: Neural model ID
            neural_field: Field containing embeddings
            lexical_fields: List of fields for lexical search
            neural_weight: Weight for neural search
            lexical_weight: Weight for lexical search
            normalization: Normalization technique
            combination: Combination technique
            size: Number of results to return
            
        Returns:
            List of document IDs in rank order
        """
        query_body = {
            "size": size,
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": lexical_fields,
                                "type": "best_fields"
                            }
                        },
                        {
                            "neural": {
                                neural_field: {
                                    "query_text": query,
                                    "model_id": model_id,
                                    "k": size
                                }
                            }
                        }
                    ]
                }
            },
            "_source": False,
            "search_pipeline": {
                "request_processors": [],
                "response_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {
                                "technique": normalization
                            },
                            "combination": {
                                "technique": combination,
                                "parameters": {
                                    "weights": [lexical_weight, neural_weight]
                                }
                            }
                        }
                    }
                ]
            }
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/{index}/_search",
                json=query_body
            )
            
            if response.status_code == 200:
                results = response.json()
                return [hit['_id'] for hit in results.get('hits', {}).get('hits', [])]
        except Exception as e:
            print(f"Hybrid search error: {str(e)}")
        
        return []
    
    def find_optimal_weight(
        self,
        index: str,
        query: str,
        relevance_dict: Dict[str, int],
        model_id: str,
        neural_field: str,
        lexical_fields: List[str],
        normalization: str = "min_max",
        combination: str = "arithmetic_mean",
        binary_relevance: bool = False,
        k: int = 10,
        weight_increment: float = 0.1
    ) -> Tuple[float, float]:
        """
        Find optimal neural/lexical weights for a query.
        
        Args:
            index: Index name
            query: Query text
            relevance_dict: Dictionary mapping doc_id to relevance score
            model_id: Neural model ID
            neural_field: Field containing embeddings
            lexical_fields: List of fields for lexical search
            normalization: Normalization technique
            combination: Combination technique
            binary_relevance: If True, treat all relevant docs as equally relevant
            k: k value for NDCG calculation
            weight_increment: Increment for weight search
            
        Returns:
            Tuple of (optimal_weight, best_ndcg)
        """
        from .evaluation_metrics import compute_ndcg_at_k
        
        best_weight = 0.0
        best_ndcg = 0.0
        
        # Try different weight combinations
        weights = np.arange(0, 1.0 + weight_increment, weight_increment)
        
        for neural_weight in weights:
            lexical_weight = 1.0 - neural_weight
            
            # Perform hybrid search with these weights
            ranked_docs = self.hybrid_search(
                index=index,
                query=query,
                model_id=model_id,
                neural_field=neural_field,
                lexical_fields=lexical_fields,
                neural_weight=neural_weight,
                lexical_weight=lexical_weight,
                normalization=normalization,
                combination=combination,
                size=100
            )
            
            # Calculate NDCG
            ndcg = compute_ndcg_at_k(
                ranked_docs, 
                relevance_dict, 
                k,
                binary=binary_relevance
            )
            
            # Update best if better
            if ndcg >= best_ndcg:  # Use >= to prefer higher neural weight when equal
                best_ndcg = ndcg
                best_weight = neural_weight
        
        return best_weight, best_ndcg
