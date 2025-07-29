"""
Weight prediction for dynamic hybrid search optimization - TUNED VERSION.
Includes more aggressive neural preferences and FiQA-specific optimizations.
"""

import pickle
from typing import Dict, Tuple, Optional
import numpy as np
from sklearn.linear_model import LinearRegression
from feature_extractor import Domain, DomainAwareFeatureExtractor


class WeightPredictor:
    """Base class for weight prediction"""
    
    def predict_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """
        Predict lexical and neural weights based on features.
        Returns: (lexical_weight, neural_weight) tuple that sums to 1.0
        """
        raise NotImplementedError


class HeuristicWeightPredictor(WeightPredictor):
    """Simple heuristic-based weight prediction"""
    
    def predict_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """Predict weights based on simple heuristics"""
        # Simple length-based heuristic
        query_length = features.get('query_length', 0)
        
        # Short queries tend to be more keyword-based
        if query_length < 20:
            return (0.7, 0.3)
        # Long queries benefit more from semantic understanding
        elif query_length > 50:
            return (0.3, 0.7)
        else:
            return (0.5, 0.5)


class DomainAwareWeightPredictor(WeightPredictor):
    """Domain-specific weight prediction with tuned heuristics"""
    
    DOMAIN_DEFAULTS = {
        Domain.ECOMMERCE: (0.7, 0.3),  # UPDATED: Based on ESCI training data - lexical-heavy
        Domain.QA_CONVERSATIONAL: (0.3, 0.7),
        Domain.MEDICAL_SCIENTIFIC: (0.6, 0.4),
        Domain.LEGAL: (0.7, 0.3),
        Domain.TECHNICAL: (0.5, 0.5),
        Domain.GENERAL: (0.5, 0.5),
        Domain.FINANCIAL: (0.2, 0.8),  # NEW: FiQA default - more neural
    }
    
    def __init__(self, domain: Domain = Domain.GENERAL):
        self.domain = domain
    
    def predict_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """Predict weights based on domain-specific heuristics"""
        # Use domain-specific prediction
        if self.domain == Domain.ECOMMERCE:
            return self._predict_ecommerce_weights(features)
        elif self.domain == Domain.QA_CONVERSATIONAL:
            return self._predict_qa_weights(features)
        elif self.domain == Domain.MEDICAL_SCIENTIFIC:
            return self._predict_medical_weights(features)
        elif self.domain == Domain.LEGAL:
            return self._predict_legal_weights(features)
        elif self.domain == Domain.TECHNICAL:
            return self._predict_technical_weights(features)
        elif self.domain == Domain.FINANCIAL:
            return self._predict_financial_weights(features)
        else:
            return self._predict_general_weights(features)
    
    def _predict_ecommerce_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """E-commerce specific weight prediction - UPDATED based on ESCI training data"""
        # ESCI training showed 40%+ queries need lexical-heavy weights
        
        # Product codes/SKUs need exact matching
        if features.get('has_sku_pattern', 0) > 0:
            return (0.9, 0.1)
        
        # Queries with numbers (model numbers, sizes) favor lexical
        elif features.get('has_numbers', 0) > 0:
            # Very short queries with numbers are likely model lookups
            if features.get('query_length', 0) < 15:
                return (0.8, 0.2)
            else:
                return (0.7, 0.3)
        
        # Currency or size terms indicate specific product search
        elif features.get('has_currency', 0) > 0 or features.get('has_size_terms', 0) > 0:
            return (0.7, 0.3)
        
        # Short queries in e-commerce are often brand/product names
        elif features.get('query_length', 0) < 20:
            return (0.7, 0.3)
        
        # Product category searches
        elif features.get('is_product_search', 0) > 0:
            return (0.6, 0.4)
        
        # Longer queries might be looking for advice/reviews
        elif features.get('query_length', 0) > 50:
            return (0.5, 0.5)
        
        else:
            # Default for ESCI: lexical-heavy (based on training data)
            return (0.7, 0.3)
    
    def _predict_qa_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """Q&A specific weight prediction"""
        # Questions generally benefit from semantic understanding
        if features.get('is_question', 0) > 0:
            # Complex questions need more neural
            if features.get('sentence_count', 1) > 1 or features.get('conversational_score', 0) > 0.5:
                return (0.2, 0.8)
            else:
                return (0.3, 0.7)
        else:
            # Statements might be looking for exact matches
            return (0.5, 0.5)
    
    def _predict_medical_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """Medical/Scientific specific weight prediction"""
        # Technical medical queries with specific terms
        if features.get('medical_acronym_count', 0) > 2:
            return (0.7, 0.3)
        elif features.get('has_dosage', 0) > 0:
            return (0.8, 0.2)
        elif features.get('clinical_term_density', 0) > 0.1:
            return (0.6, 0.4)
        else:
            # General health queries benefit from semantic search
            return (0.4, 0.6)
    
    def _predict_legal_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """Legal specific weight prediction"""
        # Legal citations need exact matching
        if features.get('has_citation', 0) > 0 or features.get('has_section_ref', 0) > 0:
            return (0.9, 0.1)
        elif features.get('legal_entity_count', 0) > 0:
            return (0.7, 0.3)
        elif features.get('formality_score', 0.5) > 0.7:
            return (0.6, 0.4)
        else:
            return (0.5, 0.5)
    
    def _predict_technical_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """Technical specific weight prediction"""
        # Code patterns and version numbers need exact matching
        if features.get('has_code_pattern', 0) > 0:
            return (0.7, 0.3)
        elif features.get('has_version_number', 0) > 0:
            return (0.8, 0.2)
        elif features.get('has_error_pattern', 0) > 0:
            return (0.6, 0.4)
        elif features.get('technical_term_ratio', 0) > 0.2:
            return (0.5, 0.5)
        else:
            # Conceptual technical questions
            return (0.3, 0.7)
    
    def _predict_general_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """General heuristic for non-domain-specific queries - TUNED VERSION"""
        query_length = features.get('query_length', 0)
        has_numbers = features.get('has_numbers', 0.0)
        
        # TUNED: More aggressive neural preference
        # Start at 0.7 neural for short queries, up to 0.9 for long
        neural_weight = min(0.9, 0.7 + (query_length / 200.0))
        
        # Adjust for presence of numbers (favor lexical)
        if has_numbers > 0:
            # TUNED: Less aggressive reduction (was 0.5)
            neural_weight *= 0.8
        
        # TUNED: Allow more extreme neural weights (was max 0.2)
        neural_weight = max(0.1, neural_weight)
        lexical_weight = 1.0 - neural_weight
        
        return (lexical_weight, neural_weight)
    
    def _predict_financial_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """FiQA-specific financial weight prediction - NEW"""
        # Start with strong neural preference (like best static)
        base_neural_weight = 0.9
        
        # Adjust based on financial features
        if features.get('is_specific_lookup', 0.0) > 0:
            # Short queries with tickers are likely ticker lookups
            base_neural_weight = 0.3  # Favor lexical
        elif features.get('has_ticker', 0.0) > 0 and features.get('ticker_count', 0) <= 2:
            # Single ticker mentions need more lexical
            base_neural_weight -= 0.4
        elif features.get('has_financial_numbers', 0.0) > 0:
            # Specific numbers need some lexical
            base_neural_weight -= 0.2
        
        if features.get('seeks_advice', 0.0) > 0:
            # Advice questions need semantic understanding
            base_neural_weight = max(base_neural_weight, 0.8)
        
        # More keywords = more conceptual = more neural
        keyword_count = features.get('financial_keyword_count', 0)
        if keyword_count >= 3:
            base_neural_weight = max(base_neural_weight, 0.8)
        
        neural_weight = max(0.1, min(0.9, base_neural_weight))
        return (1.0 - neural_weight, neural_weight)


class MLWeightPredictor(WeightPredictor):
    """Machine learning based weight prediction with tuned defaults"""
    
    def __init__(self, model_path: Optional[str] = None, feature_names: Optional[list] = None, domain: Domain = Domain.GENERAL):
        self.model = None
        self.feature_names = feature_names
        self.domain = domain
        self.fallback_predictor = DomainAwareWeightPredictor(domain)
        
        if model_path:
            try:
                with open(model_path, 'rb') as f:
                    model_dict = pickle.load(f)
                    self.model = model_dict['model']
                    self.feature_names = model_dict.get('feature_names', feature_names)
                    self.train_r2 = model_dict.get('train_r2', None)
                    self.test_r2 = model_dict.get('test_r2', None)
                    print(f"Loaded model with {len(self.feature_names)} features")
                    if self.train_r2 is not None:
                        print(f"Model R²: train={self.train_r2:.3f}, test={self.test_r2:.3f}")
            except Exception as e:
                print(f"Failed to load model: {e}")
    
    def predict_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """Predict weights using ML model with fallback to heuristics"""
        if self.model is None or self.feature_names is None:
            # No model available, use fallback
            return self.fallback_predictor.predict_weights(features)
        
        try:
            # Prepare feature vector
            feature_vector = []
            for fname in self.feature_names:
                feature_vector.append(features.get(fname, 0.0))
            
            # Predict lexical weight
            X = np.array(feature_vector).reshape(1, -1)
            lexical_weight = self.model.predict(X)[0]
            
            # TUNED: More aggressive clamping for poor models
            # If model has poor R², push predictions toward optimal range
            if self.test_r2 is not None and self.test_r2 < 0:
                # Bad model - use more aggressive fallback
                print(f"Poor model R²={self.test_r2:.3f}, using tuned fallback")
                return self.fallback_predictor.predict_weights(features)
            
            # Ensure weights are in valid range
            lexical_weight = max(0.1, min(0.9, lexical_weight))
            neural_weight = 1.0 - lexical_weight
            
            # Round to nearest 0.1
            lexical_weight = round(lexical_weight * 10) / 10
            neural_weight = round(neural_weight * 10) / 10
            
            # Ensure they sum to 1.0
            if lexical_weight + neural_weight != 1.0:
                neural_weight = 1.0 - lexical_weight
            
            return (lexical_weight, neural_weight)
            
        except Exception as e:
            print(f"ML prediction failed: {e}, using fallback")
            return self.fallback_predictor.predict_weights(features)


def get_predictor_for_dataset(dataset_name: str, use_ml: bool = False, model_path: Optional[str] = None) -> WeightPredictor:
    """
    Get appropriate weight predictor for a dataset.
    
    Args:
        dataset_name: Name of the dataset
        use_ml: Whether to use ML predictor
        model_path: Path to trained model file (for ML predictor)
        
    Returns:
        WeightPredictor instance
    """
    from feature_extractor import get_domain_for_dataset
    import os
    
    domain = get_domain_for_dataset(dataset_name)
    
    if use_ml:
        # Check if model_path was provided via command line
        if model_path and os.path.exists(model_path):
            print(f"Using ML model from: {model_path}")
            return MLWeightPredictor(model_path=model_path, domain=domain)
        else:
            # Try default paths
            default_paths = [
                f"{dataset_name}_weight_predictor_model_enhanced.pkl",
                f"{dataset_name}_weight_predictor_model.pkl",
                f"{dataset_name.lower()}_weight_predictor_model_enhanced.pkl",
                f"{dataset_name.lower()}_weight_predictor_model.pkl"
            ]
            for path in default_paths:
                if os.path.exists(path):
                    print(f"Found ML model at: {path}")
                    return MLWeightPredictor(model_path=path, domain=domain)
            
            print(f"No ML model found, falling back to heuristics")
            return DomainAwareWeightPredictor(domain=domain)
    else:
        return DomainAwareWeightPredictor(domain=domain)
