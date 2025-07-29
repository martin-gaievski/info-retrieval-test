"""
Data-driven feature extractor for ESCI that learns from actual data patterns.
This extractor uses patterns discovered from analyzing the ESCI dataset
rather than hardcoded rules.
"""

import re
import pickle
import os
from typing import Dict, List, Set
from feature_extractor import DomainAwareFeatureExtractor, Domain
import numpy as np


class ESCIDataDrivenFeatureExtractor(DomainAwareFeatureExtractor):
    """Feature extractor that uses learned patterns from ESCI data analysis"""
    
    def __init__(self, analysis_path: str = "dynamic_hybrid/esci_data_analysis.pkl"):
        super().__init__(Domain.ECOMMERCE)
        self.analysis_data = None
        self.patterns = None
        self.feature_rules = None
        self.brand_vocabulary = set()
        self.product_terms = set()
        
        # Load analysis data if available
        if os.path.exists(analysis_path):
            self._load_analysis(analysis_path)
        else:
            print(f"Warning: Analysis data not found at {analysis_path}")
            print("Using basic features. Run analyze_esci_patterns.py first for better results.")
    
    def _load_analysis(self, analysis_path: str):
        """Load learned patterns from analysis"""
        with open(analysis_path, 'rb') as f:
            self.analysis_data = pickle.load(f)
        
        self.patterns = self.analysis_data.get('patterns', {})
        self.feature_rules = self.analysis_data.get('feature_rules', {})
        self.brand_vocabulary = set(self.analysis_data.get('brand_vocabulary', []))
        self.product_terms = set(self.analysis_data.get('product_terms', []))
        
        print(f"Loaded analysis with {len(self.brand_vocabulary)} brands and {len(self.product_terms)} product terms")
    
    def extract_features(self, query_text: str) -> Dict[str, float]:
        """Extract features based on learned patterns"""
        features = {}
        query_lower = query_text.lower()
        query_words = query_lower.split()
        
        # Basic features (kept minimal)
        features['token_count'] = len(query_words)
        features['has_numbers'] = 1.0 if bool(re.search(r'\d', query_text)) else 0.0
        
        # Data-driven features
        if self.patterns:
            # Match against learned patterns for each weight class
            features.update(self._extract_pattern_features(query_text))
            
            # Vocabulary-based features
            features.update(self._extract_vocabulary_features(query_text))
            
            # Template matching features
            features.update(self._extract_template_features(query_text))
        
        # Always include these even without analysis data
        features.update(self._extract_structural_features(query_text))
        
        return features
    
    def _extract_pattern_features(self, query: str) -> Dict[str, float]:
        """Extract features based on learned patterns"""
        features = {}
        query_lower = query.lower()
        
        # Check similarity to each weight class
        for weight_class in ['lexical_heavy', 'lexical_moderate', 'balanced', 'neural_moderate', 'neural_heavy']:
            if weight_class in self.patterns:
                pattern = self.patterns[weight_class]
                
                # Term matching score
                term_score = 0.0
                if 'top_terms' in pattern:
                    for term in pattern['top_terms']:
                        if term in query_lower:
                            term_score += 1.0
                    term_score = term_score / max(len(pattern['top_terms']), 1)
                
                features[f'{weight_class}_term_score'] = term_score
                
                # Start pattern matching
                start_match = 0.0
                if 'common_start_words' in pattern:
                    for start_pattern, _ in pattern['common_start_words'][:3]:
                        if query_lower.startswith(start_pattern):
                            start_match = 1.0
                            break
                
                features[f'{weight_class}_start_match'] = start_match
                
                # Length similarity
                if 'avg_length' in pattern:
                    query_length = len(query.split())
                    length_diff = abs(query_length - pattern['avg_length'])
                    length_similarity = 1.0 / (1.0 + length_diff)
                    features[f'{weight_class}_length_sim'] = length_similarity
        
        return features
    
    def _extract_vocabulary_features(self, query: str) -> Dict[str, float]:
        """Extract features based on learned vocabulary"""
        features = {}
        query_lower = query.lower()
        query_words = set(query_lower.split())
        
        # Brand presence
        brand_found = 0.0
        for brand in self.brand_vocabulary:
            if brand.lower() in query_lower:
                brand_found = 1.0
                break
        features['has_known_brand'] = brand_found
        
        # Product term overlap
        if self.product_terms:
            term_overlap = len(query_words.intersection(self.product_terms))
            features['product_term_overlap'] = min(term_overlap / max(len(query_words), 1), 1.0)
        
        return features
    
    def _extract_template_features(self, query: str) -> Dict[str, float]:
        """Extract features based on query templates"""
        features = {}
        
        # Convert query to template
        query_template = re.sub(r'\b\d+\b', '<NUM>', query)
        query_template = re.sub(r'\b[A-Z][A-Za-z]+\b', '<BRAND>', query_template)
        query_template = re.sub(r'\$\d+', '<PRICE>', query_template)
        query_template = query_template.lower()
        
        # Check against learned templates
        if self.feature_rules:
            if 'lexical_indicators' in self.feature_rules:
                lex_templates = self.feature_rules['lexical_indicators'].get('templates', [])
                features['matches_lexical_template'] = 1.0 if query_template in lex_templates else 0.0
            
            if 'neural_indicators' in self.feature_rules:
                neural_templates = self.feature_rules['neural_indicators'].get('templates', [])
                features['matches_neural_template'] = 1.0 if query_template in neural_templates else 0.0
        
        return features
    
    def _extract_structural_features(self, query: str) -> Dict[str, float]:
        """Extract structural features that don't depend on learned patterns"""
        features = {}
        query_lower = query.lower()
        
        # Query starts
        features['starts_with_question'] = 1.0 if query_lower.startswith(('what', 'how', 'why', 'when', 'where', 'which')) else 0.0
        features['starts_with_brand_pattern'] = 1.0 if re.match(r'^[A-Z][a-z]+\s+[A-Z0-9]', query) else 0.0
        
        # Query contains
        features['has_comparison_word'] = 1.0 if any(word in query_lower for word in ['vs', 'versus', 'compare', 'better']) else 0.0
        features['has_price_mention'] = 1.0 if re.search(r'\$\d+|under \d+|below \d+|cheap|expensive', query_lower) else 0.0
        
        return features
    
    def predict_optimal_weights(self, query: str) -> tuple:
        """Predict weights based on similarity to learned patterns"""
        features = self.extract_features(query)
        
        # If we have pattern data, use similarity scores
        if self.patterns:
            # Calculate overall similarity to each weight class
            similarities = {}
            
            for weight_class in ['lexical_heavy', 'lexical_moderate', 'balanced', 'neural_moderate', 'neural_heavy']:
                # Combine different similarity scores
                term_score = features.get(f'{weight_class}_term_score', 0)
                start_match = features.get(f'{weight_class}_start_match', 0)
                length_sim = features.get(f'{weight_class}_length_sim', 0)
                
                # Weighted combination
                similarity = (term_score * 0.5) + (start_match * 0.3) + (length_sim * 0.2)
                similarities[weight_class] = similarity
            
            # Find best matching class
            best_class = max(similarities.items(), key=lambda x: x[1])[0]
            
            # Map to weights
            weight_map = {
                'lexical_heavy': (0.9, 0.1),
                'lexical_moderate': (0.7, 0.3),
                'balanced': (0.5, 0.5),
                'neural_moderate': (0.3, 0.7),
                'neural_heavy': (0.1, 0.9)
            }
            
            return weight_map.get(best_class, (0.6, 0.4))
        
        # Fallback to simple rules if no pattern data
        if features.get('starts_with_question', 0) > 0:
            return (0.2, 0.8)
        elif features.get('has_known_brand', 0) > 0:
            return (0.8, 0.2)
        else:
            return (0.6, 0.4)


if __name__ == "__main__":
    # Test the data-driven feature extractor
    
    # First, check if analysis exists
    analysis_path = "dynamic_hybrid/esci_data_analysis.pkl"
    if not os.path.exists(analysis_path):
        print("Analysis data not found. Please run:")
        print("python dynamic_hybrid/analyze_esci_patterns.py")
        print("\nUsing basic features for now...")
    
    extractor = ESCIDataDrivenFeatureExtractor()
    
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
    ]
    
    print("Testing Data-Driven Feature Extractor\n" + "="*50)
    
    for query in test_queries:
        features = extractor.extract_features(query)
        predicted_weights = extractor.predict_optimal_weights(query)
        
        # Show key features
        print(f"Query: {query}")
        print(f"Predicted weights: {predicted_weights}")
        
        # Show pattern matching scores if available
        pattern_features = {k: v for k, v in features.items() 
                          if ('_score' in k or '_match' in k or '_sim' in k) and v > 0}
        if pattern_features:
            print(f"Pattern scores: {pattern_features}")
        
        print("-" * 50)
