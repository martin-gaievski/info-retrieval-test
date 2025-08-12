"""
Basic feature extractor for ESCI queries.
Extracts only fundamental query characteristics.
"""

import re
from typing import Dict


class ESCIBasicFeatureExtractor:
    """Basic feature extractor that only extracts simple features"""
    
    def extract_features(self, query: str) -> Dict[str, float]:
        """Extract only basic features from query"""
        return {
            'query_length': len(query),
            'token_count': len(query.split()),
            'has_numbers': float(any(c.isdigit() for c in query)),
            'has_special_chars': float(bool(set(query) & set('!@#$%^&*()_+-=[]{};:"\',.<>?/\\|`~')))
        }


if __name__ == "__main__":
    # Test the basic feature extractor
    extractor = ESCIBasicFeatureExtractor()
    
    test_queries = [
        "iPhone 14 Pro Max",
        "best smartphone for photography",
        "laptop under $1000",
        "gift ideas for mom",
        "Samsung Galaxy S23 vs iPhone 14",
        "running shoes",
        "wireless headphones review",
        "kitchen appliances"
    ]
    
    for query in test_queries:
        features = extractor.extract_features(query)
        print(f"Query: {query}")
        print(f"Features: {features}")
        print("-" * 50)
