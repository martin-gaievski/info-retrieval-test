"""
Optimized feature extractor for ESCI based on 5k query analysis.
Key improvements:
1. Removed noisy basic features (avg_word_length, query_length)
2. Added stronger intent signals
3. Added pattern-based features that directly correlate with weights
"""

import re
from typing import Dict, List, Set
from feature_extractor import DomainAwareFeatureExtractor, Domain


class ESCIOptimizedFeatureExtractor(DomainAwareFeatureExtractor):
    """Optimized feature extractor based on 5k query analysis"""
    
    # Strong product patterns (should predict 0.8-0.9 lexical)
    EXACT_PRODUCT_PATTERNS = [
        r'^(iphone|samsung galaxy|ipad|macbook|surface)\s+\d+',  # iPhone 14, Galaxy S23
        r'^[A-Z]{2,}\d{2,}',  # GT3080, RTX4090
        r'^\w+\s+\d+gb',  # Sandisk 64GB
        r'^[A-Z0-9]{4,}$',  # SKU patterns
    ]
    
    # Strong advice patterns (should predict 0.1-0.2 lexical)
    ADVICE_PATTERNS = [
        r'^(how to|how do|how can)',
        r'^(what is the best|which is better)',
        r'^(tips for|guide to|tutorial)',
        r'^(help me|i need help)',
        r'(recommendations?|suggest|advice)',
    ]
    
    # Comparison patterns (should predict 0.2-0.3 lexical)
    COMPARISON_PATTERNS = [
        r'\bvs\b|\bversus\b',
        r'\bcompare\b|\bcomparison\b',
        r'\bbetter than\b|\bworse than\b',
        r'\bdifference between\b',
    ]
    
    # Category/browsing patterns (should predict 0.4-0.6 lexical)
    BROWSE_PATTERNS = [
        r'^(best|top|good)\s+\w+$',  # "best laptops"
        r'under\s*\$?\d+',  # "under $1000"
        r'for\s+(gaming|work|school|travel)',
        r'^(cheap|affordable|budget)\s+\w+',
    ]
    
    def __init__(self):
        super().__init__(Domain.ECOMMERCE)
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for efficiency"""
        self.exact_product_regex = re.compile('|'.join(self.EXACT_PRODUCT_PATTERNS), re.IGNORECASE)
        self.advice_regex = re.compile('|'.join(self.ADVICE_PATTERNS), re.IGNORECASE)
        self.comparison_regex = re.compile('|'.join(self.COMPARISON_PATTERNS), re.IGNORECASE)
        self.browse_regex = re.compile('|'.join(self.BROWSE_PATTERNS), re.IGNORECASE)
    
    def extract_features(self, query_text: str) -> Dict[str, float]:
        """Extract optimized features for ESCI queries"""
        features = {}
        query_lower = query_text.lower()
        
        # Strong intent signals (primary features)
        features['is_exact_product'] = 1.0 if self.exact_product_regex.search(query_text) else 0.0
        features['is_advice_query'] = 1.0 if self.advice_regex.search(query_text) else 0.0
        features['is_comparison_query'] = 1.0 if self.comparison_regex.search(query_text) else 0.0
        features['is_browse_query'] = 1.0 if self.browse_regex.search(query_text) else 0.0
        
        # Specific pattern features
        features['starts_with_how'] = 1.0 if query_lower.startswith(('how to', 'how do', 'how can')) else 0.0
        features['starts_with_what'] = 1.0 if query_lower.startswith(('what is', 'what are', 'which')) else 0.0
        features['has_model_number'] = 1.0 if re.search(r'\b\d{2,}\b', query_text) else 0.0
        features['has_price_limit'] = 1.0 if re.search(r'under\s*\$?\d+|below\s*\$?\d+|\$\d+', query_text) else 0.0
        
        # E-commerce specific features
        features['has_brand_name'] = 1.0 if self._has_brand_name(query_text) else 0.0
        features['has_product_specs'] = 1.0 if re.search(r'\b\d+\s*(gb|tb|inch|mp|mhz|ghz)\b', query_lower) else 0.0
        features['gift_intent'] = 1.0 if any(word in query_lower for word in ['gift', 'present', 'birthday']) else 0.0
        
        # Query structure features (reduced importance)
        features['token_count'] = len(query_text.split())
        features['has_numbers'] = 1.0 if bool(re.search(r'\d', query_text)) else 0.0
        
        # Composite features
        features['product_lookup_score'] = self._calculate_product_lookup_score(features)
        features['advice_seeking_score'] = self._calculate_advice_seeking_score(features)
        
        # Query type classification (one-hot encoding)
        query_type = self._classify_query_type(features)
        features['type_exact_product'] = 1.0 if query_type == 'exact_product' else 0.0
        features['type_advice'] = 1.0 if query_type == 'advice' else 0.0
        features['type_comparison'] = 1.0 if query_type == 'comparison' else 0.0
        features['type_browse'] = 1.0 if query_type == 'browse' else 0.0
        features['type_general'] = 1.0 if query_type == 'general' else 0.0
        
        return features
    
    def _has_brand_name(self, query: str) -> bool:
        """Check if query contains known brand names"""
        brands = [
            'apple', 'samsung', 'google', 'microsoft', 'amazon', 'sony',
            'nike', 'adidas', 'dell', 'hp', 'lenovo', 'asus', 'intel', 'amd',
            'iphone', 'galaxy', 'pixel', 'surface', 'macbook', 'ipad'
        ]
        query_lower = query.lower()
        return any(brand in query_lower for brand in brands)
    
    def _calculate_product_lookup_score(self, features: Dict[str, float]) -> float:
        """Calculate composite score for product lookup intent"""
        score = 0.0
        score += features.get('is_exact_product', 0) * 0.4
        score += features.get('has_brand_name', 0) * 0.3
        score += features.get('has_model_number', 0) * 0.2
        score += features.get('has_product_specs', 0) * 0.1
        return min(score, 1.0)
    
    def _calculate_advice_seeking_score(self, features: Dict[str, float]) -> float:
        """Calculate composite score for advice seeking intent"""
        score = 0.0
        score += features.get('is_advice_query', 0) * 0.3
        score += features.get('starts_with_how', 0) * 0.3
        score += features.get('starts_with_what', 0) * 0.2
        score += features.get('is_comparison_query', 0) * 0.1
        score += features.get('gift_intent', 0) * 0.1
        return min(score, 1.0)
    
    def _classify_query_type(self, features: Dict[str, float]) -> str:
        """Classify query into distinct types"""
        # Priority order matters
        if features.get('is_exact_product', 0) > 0:
            return 'exact_product'
        elif features.get('is_advice_query', 0) > 0:
            return 'advice'
        elif features.get('is_comparison_query', 0) > 0:
            return 'comparison'
        elif features.get('is_browse_query', 0) > 0:
            return 'browse'
        else:
            return 'general'
    
    def predict_optimal_weights(self, query: str) -> tuple:
        """Rule-based weight prediction for baseline"""
        features = self.extract_features(query)
        query_type = self._classify_query_type(features)
        
        # Strong rules based on query type
        if query_type == 'exact_product':
            return (0.9, 0.1)  # Heavy lexical
        elif query_type == 'advice':
            return (0.1, 0.9)  # Heavy neural
        elif query_type == 'comparison':
            return (0.2, 0.8)  # Neural-leaning
        elif query_type == 'browse':
            return (0.5, 0.5)  # Balanced
        else:
            # Default based on scores
            product_score = features.get('product_lookup_score', 0)
            advice_score = features.get('advice_seeking_score', 0)
            
            if product_score > 0.7:
                return (0.8, 0.2)
            elif advice_score > 0.7:
                return (0.2, 0.8)
            else:
                return (0.6, 0.4)  # Slight lexical bias for e-commerce


if __name__ == "__main__":
    # Test the optimized feature extractor
    extractor = ESCIOptimizedFeatureExtractor()
    
    test_queries = [
        "iPhone 14 Pro Max",
        "Samsung Galaxy S23",
        "how to choose a laptop",
        "best smartphone for photography",
        "laptop under $1000",
        "gift ideas for mom",
        "Samsung Galaxy S23 vs iPhone 14",
        "running shoes",
        "wireless headphones review",
        "GTX3080",
        "what is the best coffee maker",
        "tips for buying a car"
    ]
    
    print("Testing Optimized Feature Extractor\n" + "="*50)
    
    for query in test_queries:
        features = extractor.extract_features(query)
        predicted_weights = extractor.predict_optimal_weights(query)
        
        # Show key features only
        key_features = {
            k: v for k, v in features.items() 
            if v > 0 and k in [
                'is_exact_product', 'is_advice_query', 'is_comparison_query',
                'is_browse_query', 'product_lookup_score', 'advice_seeking_score',
                'type_exact_product', 'type_advice', 'type_comparison', 'type_browse'
            ]
        }
        
        print(f"Query: {query}")
        print(f"Predicted weights: {predicted_weights}")
        print(f"Key features: {key_features}")
        print("-" * 50)
