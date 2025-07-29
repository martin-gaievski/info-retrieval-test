"""
Enhanced feature extractor for ESCI dynamic optimization.
Focuses on search intent and e-commerce specific patterns.
"""

import re
from typing import Dict, List, Set
from feature_extractor import DomainAwareFeatureExtractor, Domain


class ESCIEnhancedFeatureExtractor(DomainAwareFeatureExtractor):
    """Enhanced feature extractor specifically designed for ESCI e-commerce queries"""
    
    # Enhanced patterns for e-commerce
    BRAND_PATTERNS = [
        r'\b(apple|samsung|sony|nike|adidas|amazon|microsoft|google|dell|hp|lenovo|asus|intel|amd)\b',
        r'\biphone\b',
        r'\bgalaxy\b',
        r'\bmacbook\b',
        r'\bpixel\b',
        r'\bsurface\b',
    ]
    
    MODEL_NUMBER_PATTERNS = [
        r'\biphone\s*\d+\b',                 # iPhone 14, iPhone14
        r'\bgalaxy\s*[s]\d+\b',              # Galaxy S23
        r'\b[A-Z]+\d+[A-Z]*\b',             # GTX3080, RTX4090
        r'\b\d+[A-Z]{2,}[0-9]*\b',          # 64GB, 256GB
        r'\b[A-Z]{2,}\s*\d+[A-Z]*\b',       # Pro Max, etc.
    ]
    
    ADVICE_PATTERNS = [
        r'\b(best|good|better|recommend|suggest|should|which|what)\b',
        r'\b(vs|versus|compare|comparison|difference)\b',
        r'\b(review|opinion|worth|quality|rating)\b',
        r'\b(help|advice|guide|tips|how)\b',
    ]
    
    PURCHASE_INTENT_PATTERNS = [
        r'\b(buy|purchase|order|shop|shopping|get|need)\b',
        r'\b(price|cost|cheap|expensive|budget|affordable)\b',
        r'\b(sale|discount|deal|offer|promo)\b',
    ]
    
    SPECIFICATION_PATTERNS = [
        r'\b\d+(\.\d+)?\s*(inch|gb|tb|mb|ghz|mhz|mp|mm|cm)\b',
        r'\b(color|colour|size|weight|dimension|spec|feature)\b',
        r'\b(black|white|red|blue|green|silver|gold)\b',
    ]
    
    CATEGORY_KEYWORDS = {
        'electronics': ['laptop', 'computer', 'phone', 'tablet', 'camera', 'headphone', 'speaker'],
        'clothing': ['shirt', 'pants', 'dress', 'shoe', 'jacket', 'coat', 'hat'],
        'home': ['furniture', 'kitchen', 'bedroom', 'living', 'decor', 'appliance'],
        'books': ['book', 'novel', 'textbook', 'manual', 'guide', 'magazine'],
        'sports': ['fitness', 'exercise', 'sports', 'outdoor', 'gym', 'running'],
        'beauty': ['cosmetic', 'makeup', 'skincare', 'perfume', 'beauty', 'hair']
    }
    
    def __init__(self):
        super().__init__(Domain.ECOMMERCE)
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for efficiency"""
        self.brand_regex = re.compile('|'.join(self.BRAND_PATTERNS), re.IGNORECASE)
        self.model_regex = re.compile('|'.join(self.MODEL_NUMBER_PATTERNS), re.IGNORECASE)
        self.advice_regex = re.compile('|'.join(self.ADVICE_PATTERNS), re.IGNORECASE)
        self.purchase_regex = re.compile('|'.join(self.PURCHASE_INTENT_PATTERNS), re.IGNORECASE)
        self.spec_regex = re.compile('|'.join(self.SPECIFICATION_PATTERNS), re.IGNORECASE)
    
    def extract_features(self, query_text: str) -> Dict[str, float]:
        """Extract enhanced features for ESCI queries"""
        # Start with basic features (manually extract to avoid parent conflict)
        features = {}
        
        # Basic features
        features['query_length'] = len(query_text)
        features['token_count'] = len(query_text.split())
        features['has_numbers'] = 1.0 if bool(re.search(r'\d', query_text)) else 0.0
        features['has_special_chars'] = 1.0 if bool(re.search(r'[^a-zA-Z0-9\s]', query_text)) else 0.0
        
        # Add intent-based features
        search_features = self._extract_search_intent_features(query_text)
        features.update(search_features)
        
        # Add e-commerce specific features (pass search features for dependencies)
        features.update(self._extract_ecommerce_features(query_text, search_features))
        
        # Add linguistic complexity features
        features.update(self._extract_complexity_features(query_text))
        
        return features
    
    def _extract_search_intent_features(self, query: str) -> Dict[str, float]:
        """Extract features related to search intent"""
        features = {}
        query_lower = query.lower()
        
        # Product lookup intent (should favor lexical search)
        features['has_brand_mention'] = 1.0 if self.brand_regex.search(query) else 0.0
        features['has_model_number'] = 1.0 if self.model_regex.search(query) else 0.0
        features['has_specifications'] = 1.0 if self.spec_regex.search(query) else 0.0
        
        # Advice/comparison intent (should favor neural search)
        features['seeks_advice'] = 1.0 if self.advice_regex.search(query) else 0.0
        features['is_comparison'] = 1.0 if any(word in query_lower for word in ['vs', 'versus', 'compare', 'better']) else 0.0
        features['asks_opinion'] = 1.0 if any(word in query_lower for word in ['review', 'opinion', 'worth', 'good', 'bad']) else 0.0
        
        # Question patterns
        features['is_question'] = 1.0 if query.strip().endswith('?') else 0.0
        features['starts_with_question'] = 1.0 if query.lower().startswith(('what', 'how', 'why', 'when', 'where', 'which', 'who')) else 0.0
        
        # Purchase intent
        features['has_purchase_intent'] = 1.0 if self.purchase_regex.search(query) else 0.0
        features['price_conscious'] = 1.0 if any(word in query_lower for word in ['cheap', 'budget', 'affordable', 'price']) else 0.0
        
        return features
    
    def _extract_ecommerce_features(self, query: str, search_features: Dict[str, float]) -> Dict[str, float]:
        """Extract e-commerce domain specific features"""
        features = {}
        query_lower = query.lower()
        
        # Category detection
        category_score = 0.0
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            if any(keyword in query_lower for keyword in keywords):
                category_score += 1.0
        features['category_mentions'] = min(category_score, 3.0) / 3.0  # Normalize
        
        # Specificity indicators (now using passed search_features)
        features['is_specific_product'] = 1.0 if (search_features.get('has_brand_mention', 0) and 
                                                search_features.get('has_model_number', 0)) else 0.0
        
        features['is_general_browse'] = 1.0 if (features.get('category_mentions', 0) > 0 and 
                                               not features.get('is_specific_product', 0)) else 0.0
        
        # Shopping context
        features['gift_search'] = 1.0 if any(word in query_lower for word in ['gift', 'present', 'birthday', 'christmas']) else 0.0
        features['urgent_need'] = 1.0 if any(word in query_lower for word in ['urgent', 'asap', 'quick', 'fast']) else 0.0
        
        return features
    
    def _extract_complexity_features(self, query: str) -> Dict[str, float]:
        """Extract linguistic complexity features"""
        features = {}
        
        # Ambiguity indicators
        features['has_ambiguous_terms'] = 1.0 if self._has_ambiguous_terms(query) else 0.0
        features['metaphor_score'] = self._calculate_metaphor_score(query)
        
        # Precision indicators
        features['has_exact_terms'] = 1.0 if self._has_exact_terms(query) else 0.0
        features['specificity_score'] = self._calculate_specificity_score(query)
        
        # Complexity metrics
        features['avg_word_length'] = sum(len(word) for word in query.split()) / max(len(query.split()), 1)
        features['unique_word_ratio'] = len(set(query.lower().split())) / max(len(query.split()), 1)
        
        return features
    
    def _has_ambiguous_terms(self, query: str) -> bool:
        """Check if query contains ambiguous terms"""
        ambiguous_terms = ['thing', 'stuff', 'item', 'product', 'something', 'anything']
        return any(term in query.lower() for term in ambiguous_terms)
    
    def _calculate_metaphor_score(self, query: str) -> float:
        """Calculate metaphor/figurative language score"""
        metaphor_indicators = ['like', 'as', 'similar', 'kind of', 'sort of']
        score = sum(1 for indicator in metaphor_indicators if indicator in query.lower())
        return min(score / 3.0, 1.0)  # Normalize
    
    def _has_exact_terms(self, query: str) -> bool:
        """Check if query contains exact/precise terms"""
        exact_indicators = ['exact', 'exactly', 'specific', 'precisely', 'particular']
        return any(term in query.lower() for term in exact_indicators)
    
    def _calculate_specificity_score(self, query: str) -> float:
        """Calculate how specific the query is"""
        specificity_indicators = [
            self.model_regex.search(query) is not None,
            self.spec_regex.search(query) is not None,
            self.brand_regex.search(query) is not None,
            len(query.split()) <= 3,  # Short queries tend to be specific
            any(char.isdigit() for char in query),  # Numbers indicate specificity
        ]
        return sum(specificity_indicators) / len(specificity_indicators)
    
    def predict_optimal_weights(self, query: str) -> tuple:
        """Rule-based weight prediction based on enhanced features"""
        features = self.extract_features(query)
        
        # PRIORITY 1: High neural scenarios (intent-based)
        if features.get('asks_opinion', 0) > 0 or features.get('gift_search', 0) > 0:
            return (0.1, 0.9)  # Opinion/gift queries
        
        if features.get('is_comparison', 0) > 0:
            return (0.2, 0.8)  # Comparison queries (prioritize over specific products)
        
        if features.get('seeks_advice', 0) > 0 and not features.get('is_specific_product', 0):
            return (0.2, 0.8)  # Advice queries (but not for specific products)
        
        # PRIORITY 2: High lexical scenarios (specific products)
        if features.get('is_specific_product', 0) > 0:
            return (0.9, 0.1)  # Exact product lookup
        
        if features.get('has_model_number', 0) > 0 and features.get('specificity_score', 0) > 0.6:
            return (0.8, 0.2)  # Specific product search
        
        # PRIORITY 3: Medium scenarios
        if features.get('has_purchase_intent', 0) > 0:
            return (0.6, 0.4)  # Purchase intent
        
        if features.get('is_general_browse', 0) > 0:
            return (0.4, 0.6)  # Category browsing
        
        # Default for e-commerce
        return (0.7, 0.3)


def create_enhanced_training_data(queries: List[str], evaluator) -> List[Dict]:
    """Create enhanced training data with new features"""
    extractor = ESCIEnhancedFeatureExtractor()
    training_data = []
    
    for query in queries:
        features = extractor.extract_features(query)
        
        # Test different weight combinations
        weight_combinations = [
            (0.1, 0.9), (0.2, 0.8), (0.3, 0.7), (0.4, 0.6), (0.5, 0.5),
            (0.6, 0.4), (0.7, 0.3), (0.8, 0.2), (0.9, 0.1)
        ]
        
        best_score = 0.0
        best_weights = (0.7, 0.3)
        
        for lex_weight, neural_weight in weight_combinations:
            score = evaluator.evaluate_query(query, lex_weight, neural_weight)
            if score > best_score:
                best_score = score
                best_weights = (lex_weight, neural_weight)
        
        # Create training example
        example = features.copy()
        example['best_lexical_weight'] = best_weights[0]
        example['best_neural_weight'] = best_weights[1]
        example['best_score'] = best_score
        example['query_text'] = query
        
        training_data.append(example)
    
    return training_data


if __name__ == "__main__":
    # Test the enhanced feature extractor
    extractor = ESCIEnhancedFeatureExtractor()
    
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
        predicted_weights = extractor.predict_optimal_weights(query)
        
        print(f"Query: {query}")
        print(f"Predicted weights: {predicted_weights}")
        print(f"Key features: {[(k, v) for k, v in features.items() if v > 0]}")
        print("-" * 50)
