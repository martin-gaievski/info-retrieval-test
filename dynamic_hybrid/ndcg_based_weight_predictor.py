"""
NDCG-based weight predictor that predicts NDCG for multiple weight combinations
and selects the best one
"""

import pickle
import numpy as np
from typing import Dict, Tuple, List, Optional
from sklearn.preprocessing import StandardScaler

from weight_predictor import WeightPredictor, DomainAwareWeightPredictor
from feature_extractor import Domain


class NDCGBasedWeightPredictor(WeightPredictor):
    """
    Predicts weights by:
    1. Predicting NDCG for each possible weight combination
    2. Selecting the combination with highest predicted NDCG
    """
    
    def __init__(self, 
                 model_path: str,
                 weight_values: Optional[List[float]] = None,
                 include_result_features: bool = False,
                 domain: Domain = Domain.GENERAL):
        """
        Initialize NDCG-based predictor.
        
        Args:
            model_path: Path to trained NDCG prediction model
            weight_values: Neural weight values to test (default: 0.0 to 1.0 by 0.1)
            include_result_features: Whether model expects result features
            domain: Domain for fallback predictor
        """
        self.model = None
        self.scaler = None
        self.feature_columns = None
        self.include_result_features = include_result_features
        self.fallback_predictor = DomainAwareWeightPredictor(domain)
        
        # Default weight values to test
        if weight_values is None:
            self.weight_values = [round(w/10, 1) for w in range(11)]  # 0.0, 0.1, ..., 1.0
        else:
            self.weight_values = weight_values
        
        # Load model
        try:
            with open(model_path, 'rb') as f:
                model_dict = pickle.load(f)
                self.model = model_dict['model']
                self.scaler = model_dict.get('scaler')
                self.feature_columns = model_dict.get('feature_columns', [])
                self.model_type = model_dict.get('model_type', 'unknown')
                
                # Print model info
                print(f"Loaded NDCG prediction model ({self.model_type})")
                print(f"Features: {len(self.feature_columns)}")
                if 'metrics' in model_dict:
                    metrics = model_dict['metrics']
                    print(f"Model R²: train={metrics.get('train_r2', 'N/A'):.3f}, "
                          f"test={metrics.get('test_r2', 'N/A'):.3f}")
        except Exception as e:
            print(f"Failed to load NDCG model: {e}")
    
    def predict_weights(self, features: Dict[str, float]) -> Tuple[float, float]:
        """
        Predict optimal weights by finding the combination with highest predicted NDCG.
        
        Args:
            features: Query features
            
        Returns:
            (lexical_weight, neural_weight) tuple
        """
        if self.model is None:
            return self.fallback_predictor.predict_weights(features)
        
        try:
            best_neural_weight = None
            best_ndcg = -1
            predictions = {}
            
            # Test each weight combination
            for neural_weight in self.weight_values:
                # Prepare features for this weight combination
                combined_features = self._prepare_features(features, neural_weight)
                
                # Predict NDCG
                feature_vector = []
                for fname in self.feature_columns:
                    feature_vector.append(combined_features.get(fname, 0.0))
                
                X = np.array(feature_vector).reshape(1, -1)
                
                # Scale if scaler available
                if self.scaler is not None:
                    X = self.scaler.transform(X)
                
                # Predict
                predicted_ndcg = self.model.predict(X)[0]
                predictions[neural_weight] = predicted_ndcg
                
                # Track best
                if predicted_ndcg > best_ndcg:
                    best_ndcg = predicted_ndcg
                    best_neural_weight = neural_weight
            
            # Debug output (optional)
            if len(predictions) > 0:
                sorted_preds = sorted(predictions.items(), key=lambda x: x[1], reverse=True)
                print(f"NDCG predictions (top 3): {sorted_preds[:3]}")
            
            if best_neural_weight is not None:
                lexical_weight = round(1.0 - best_neural_weight, 1)
                return (lexical_weight, best_neural_weight)
            else:
                return self.fallback_predictor.predict_weights(features)
                
        except Exception as e:
            print(f"NDCG prediction failed: {e}, using fallback")
            return self.fallback_predictor.predict_weights(features)
    
    def _prepare_features(self, 
                         query_features: Dict[str, float], 
                         neural_weight: float) -> Dict[str, float]:
        """
        Prepare features for NDCG prediction.
        
        Args:
            query_features: Features extracted from query
            neural_weight: The neural weight to test
            
        Returns:
            Combined features including weight as a feature
        """
        # Start with query features
        combined = query_features.copy()
        
        # Add weight features
        combined['neural_weight'] = neural_weight
        combined['lexical_weight'] = 1.0 - neural_weight
        
        # If model expects result features but we don't have them,
        # add dummy values (this happens during inference)
        if self.include_result_features:
            result_feature_names = [
                'num_results', 'max_score', 'min_score', 
                'avg_score', 'score_std', 'score_range'
            ]
            for feat in result_feature_names:
                if feat not in combined:
                    combined[feat] = 0.0  # Default value
        
        return combined
    
    def predict_ndcg_for_weights(self, 
                                features: Dict[str, float],
                                neural_weight: float) -> float:
        """
        Predict NDCG for a specific weight combination.
        
        Args:
            features: Query features
            neural_weight: Neural weight to test
            
        Returns:
            Predicted NDCG score
        """
        if self.model is None:
            return 0.0
        
        try:
            combined_features = self._prepare_features(features, neural_weight)
            
            feature_vector = []
            for fname in self.feature_columns:
                feature_vector.append(combined_features.get(fname, 0.0))
            
            X = np.array(feature_vector).reshape(1, -1)
            
            if self.scaler is not None:
                X = self.scaler.transform(X)
            
            return self.model.predict(X)[0]
            
        except Exception as e:
            print(f"Failed to predict NDCG: {e}")
            return 0.0


def get_ndcg_predictor_for_dataset(dataset_name: str, 
                                  model_path: Optional[str] = None,
                                  include_result_features: bool = False) -> WeightPredictor:
    """
    Get NDCG-based predictor for a dataset.
    
    Args:
        dataset_name: Name of the dataset
        model_path: Path to trained NDCG model
        include_result_features: Whether model expects result features
        
    Returns:
        WeightPredictor instance
    """
    from feature_extractor import get_domain_for_dataset
    import os
    
    domain = get_domain_for_dataset(dataset_name)
    
    # Try to find model if not specified
    if model_path is None:
        possible_paths = [
            f"{dataset_name}_ndcg_predictor_model.pkl",
            f"{dataset_name.lower()}_ndcg_predictor_model.pkl",
            "ndcg_predictor_model.pkl"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                model_path = path
                print(f"Found NDCG model at: {path}")
                break
    
    if model_path and os.path.exists(model_path):
        return NDCGBasedWeightPredictor(
            model_path=model_path,
            include_result_features=include_result_features,
            domain=domain
        )
    else:
        print(f"No NDCG model found, falling back to domain heuristics")
        return DomainAwareWeightPredictor(domain=domain)
