#!/usr/bin/env python3
"""
Train O19S model with AMPLIFIED weight feature to prevent collapse.
This addresses the root cause: weight coefficient being 10x smaller than other features.
"""

import numpy as np
import pandas as pd
import pickle
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from sklearn.linear_model import Ridge
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from tqdm import tqdm
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dynamic_hybrid.feature_extractor_o19s_query_string import O19SQueryStringFeatureExtractor
from dynamic_hybrid.utils.opensearch_client import OpenSearchClient
from dynamic_hybrid.utils.metrics import calculate_ndcg


class O19SAmplifiedTrainer:
    """Train O19S model with amplified weight feature."""
    
    def __init__(self, opensearch_client, model_id: str, 
                 corpus_field: str = "product_title",
                 amplification_factor: float = 10.0):
        """
        Initialize trainer with weight amplification.
        
        Args:
            opensearch_client: OpenSearch client
            model_id: Neural model ID
            corpus_field: Field for corpus statistics
            amplification_factor: How much to amplify weight feature
        """
        self.client = opensearch_client
        self.model_id = model_id
        self.corpus_field = corpus_field
        self.amplification_factor = amplification_factor
        self.feature_extractor = O19SQueryStringFeatureExtractor(
            opensearch_client=opensearch_client,
            corpus_field=corpus_field
        )
        
        # Fixed normalization and combination
        self.normalization = 'min_max'
        self.combination = 'arithmetic_mean'
        
        # Feature scaler
        self.scaler = MinMaxScaler()
        
        print(f"Initialized O19S amplified trainer for {opensearch_client.host}:{opensearch_client.port}/{opensearch_client.index_name}")
        print(f"Weight amplification factor: {amplification_factor}x")
        print(f"Using fixed: {self.normalization} + {self.combination}")
        
    def collect_training_data(self, queries_df: pd.DataFrame, 
                             weights: List[float],
                             sample_size: Optional[int] = None) -> Tuple[np.ndarray, np.ndarray]:
        """
        Collect training data with amplified weight feature.
        
        Args:
            queries_df: DataFrame with queries and ratings
            weights: List of weight values to test
            sample_size: Number of queries to use (None for all)
            
        Returns:
            X: Feature matrix with AMPLIFIED weight feature
            y: Target NDCG values
        """
        if sample_size:
            queries_df = queries_df.head(sample_size)
        
        print(f"\nCollecting training data with {len(weights)} weights per query")
        print(f"Amplification factor: {self.amplification_factor}x")
        
        X = []
        y = []
        
        # Progress bar
        total = len(queries_df) * len(weights)
        pbar = tqdm(total=total, desc="Collecting amplified training data")
        
        for _, row in queries_df.iterrows():
            query = row['query']
            query_id = row['query_id']
            
            # Get ratings for this query
            query_ratings = row.get('ratings', {})
            if not query_ratings:
                pbar.update(len(weights))
                continue
            
            # Extract features once per query
            features = self.feature_extractor.extract_features(query)
            
            # Test each weight
            for weight in weights:
                # Add weight as first feature
                feature_vector = [weight] + features
                
                # Perform hybrid search
                results = self.client.hybrid_search(
                    query=query,
                    model_id=self.model_id,
                    neural_weight=weight,
                    normalization_technique=self.normalization,
                    combination_technique=self.combination,
                    size=10
                )
                
                # Calculate NDCG
                ndcg = calculate_ndcg(results, query_ratings, k=10)
                
                X.append(feature_vector)
                y.append(ndcg)
                pbar.update(1)
        
        pbar.close()
        
        X = np.array(X)
        y = np.array(y)
        
        # Normalize features
        X = self.scaler.fit_transform(X)
        
        # AMPLIFY WEIGHT FEATURE
        print(f"\n🔥 Amplifying weight feature by {self.amplification_factor}x")
        X[:, 0] *= self.amplification_factor
        
        print(f"Collected {len(X)} training samples")
        print(f"Weight feature stats after amplification:")
        print(f"  Mean: {np.mean(X[:, 0]):.4f}")
        print(f"  Std: {np.std(X[:, 0]):.4f}")
        print(f"  Min: {np.min(X[:, 0]):.4f}")
        print(f"  Max: {np.max(X[:, 0]):.4f}")
        
        return X, y
    
    def train_model(self, X: np.ndarray, y: np.ndarray, 
                   alpha: float = 10.0, test_size: float = 0.2) -> Ridge:
        """
        Train Ridge regression model with amplified features.
        
        Args:
            X: Feature matrix (with amplified weight feature)
            y: Target values
            alpha: Regularization strength
            test_size: Fraction for test set
            
        Returns:
            Trained Ridge model
        """
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        print(f"\nTraining Ridge regression with amplified weight feature...")
        print(f"  Training samples: {len(X_train)}")
        print(f"  Test samples: {len(X_test)}")
        print(f"  Features: {X_train.shape[1]}")
        print(f"  Alpha: {alpha}")
        print(f"  Amplification: {self.amplification_factor}x")
        
        # Train model
        model = Ridge(alpha=alpha, random_state=42)
        model.fit(X_train, y_train)
        
        # Evaluate
        train_score = model.score(X_train, y_train)
        test_score = model.score(X_test, y_test)
        
        train_pred = model.predict(X_train)
        test_pred = model.predict(X_test)
        
        train_rmse = np.sqrt(np.mean((y_train - train_pred) ** 2))
        test_rmse = np.sqrt(np.mean((y_test - test_pred) ** 2))
        
        print(f"\nModel Performance:")
        print(f"  Train RMSE: {train_rmse:.4f}, R²: {train_score:.4f}")
        print(f"  Test RMSE: {test_rmse:.4f}, R²: {test_score:.4f}")
        
        # Analyze coefficients
        coefficients = model.coef_
        intercept = model.intercept_
        
        print(f"\nFeature Coefficients (top 10):")
        coef_indices = np.argsort(np.abs(coefficients))[::-1][:10]
        
        feature_names = self.get_feature_names()
        for idx in coef_indices:
            print(f"  {feature_names[idx]}: {coefficients[idx]:.6f}")
        
        # Check weight coefficient
        weight_coef = coefficients[0]
        print(f"\n🎯 Weight coefficient (f_0_neuralness): {weight_coef:.6f}")
        
        # Check if model is sensitive to weight
        weight_coef_abs = abs(weight_coef)
        other_coefs_abs = [abs(c) for c in coefficients[1:]]
        median_other = np.median(other_coefs_abs)
        
        if weight_coef_abs >= median_other * 0.5:
            print("✅ Model is NOW sensitive to weight changes!")
        else:
            print("⚠️ Weight coefficient still too small - try higher amplification")
        
        # Predict with varying weights to check sensitivity
        print("\n📊 Testing weight sensitivity (first 3 training samples):")
        for i in range(min(3, len(X_train))):
            sample = X_train[i].copy()
            predictions = []
            weights_test = np.linspace(0, self.amplification_factor, 11)
            
            print(f"\nSample {i+1}:")
            for w in weights_test:
                sample[0] = w  # Set amplified weight
                pred = model.predict(sample.reshape(1, -1))[0]
                predictions.append(pred)
                print(f"  Weight={w/self.amplification_factor:.1f} → NDCG={pred:.4f}")
            
            pred_range = max(predictions) - min(predictions)
            print(f"  Prediction range: {pred_range:.4f}")
            
            if pred_range < 0.01:
                print("  ⚠️ Low variation - model may still collapse")
            else:
                print("  ✅ Good variation - model responds to weight changes")
        
        return model
    
    def get_feature_names(self) -> List[str]:
        """Get feature names for 18-feature model."""
        return [
            'f_0_neuralness',  # Weight feature (amplified)
            'f_2_query_length',
            'f_4_has_special_char',
            'f_5_has_punctuation_at_end',
            'f_7_capital_letters_ratio',
            'f_8_stopwords_ratio',
            'f_14_max_document_frequency',
            'f_15_min_document_frequency', 
            'f_16_total_document_frequency',
            'f_17_average_document_frequency',
            'f_18_variance_document_frequency',
            'f_19_std_dev_document_frequency',
            'f_20_max_inverse_document_frequency',
            'f_21_min_inverse_document_frequency',
            'f_22_total_inverse_document_frequency',
            'f_23_average_inverse_document_frequency',
            'f_24_variance_inverse_document_frequency',
            'f_25_std_dev_inverse_document_frequency'
        ]
    
    def save_model(self, model: Ridge, output_path: str = "o19s_amplified_model.pkl"):
        """Save trained model with amplification metadata."""
        model_data = {
            'model': model,
            'scaler': self.scaler,
            'amplification_factor': self.amplification_factor,
            'normalization': self.normalization,
            'combination': self.combination,
            'feature_names': self.get_feature_names(),
            'timestamp': datetime.now().isoformat()
        }
        
        with open(output_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"\nModel saved to {output_path}")
        
        # Save metadata
        metadata = {
            'amplification_factor': self.amplification_factor,
            'normalization': self.normalization,
            'combination': self.combination,
            'coefficients': model.coef_.tolist(),
            'intercept': float(model.intercept_),
            'weight_coefficient': float(model.coef_[0]),
            'timestamp': datetime.now().isoformat()
        }
        
        metadata_path = output_path.replace('.pkl', '_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Metadata saved to {metadata_path}")


def main():
    parser = argparse.ArgumentParser(description='Train O19S model with amplified weight feature')
    parser.add_argument('--host', default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--index-name', default='esci-products', help='Index name')
    parser.add_argument('--model-id', required=True, help='Neural model ID')
    parser.add_argument('--sample-size', type=int, default=100, help='Number of queries to use')
    parser.add_argument('--alpha', type=float, default=10.0, help='Ridge regularization strength')
    parser.add_argument('--amplification', type=float, default=10.0, help='Weight feature amplification factor')
    parser.add_argument('--output-model', default='o19s_amplified_model.pkl', help='Output model path')
    
    args = parser.parse_args()
    
    print("="*80)
    print("O19S AMPLIFIED MODEL TRAINING")
    print("="*80)
    print("Configuration:")
    print(f"  • Amplification factor: {args.amplification}x")
    print(f"  • Alpha regularization: {args.alpha}")
    print(f"  • Sample size: {args.sample_size}")
    print("="*80)
    
    # Initialize OpenSearch client
    client = OpenSearchClient(
        host=args.host,
        port=args.port,
        index_name=args.index_name
    )
    
    # Initialize trainer with amplification
    trainer = O19SAmplifiedTrainer(
        opensearch_client=client,
        model_id=args.model_id,
        amplification_factor=args.amplification
    )
    
    # Load queries with ratings
    data_path = Path("dynamic_hybrid/data/query_train.csv")
    if not data_path.exists():
        print(f"Error: {data_path} not found!")
        return
    
    queries_df = pd.read_csv(data_path)
    
    # Collect training data
    weights = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    X, y = trainer.collect_training_data(
        queries_df, 
        weights,
        sample_size=args.sample_size
    )
    
    # Print NDCG statistics
    print(f"\nNDCG Statistics:")
    print(f"  Mean NDCG: {np.mean(y):.4f}")
    print(f"  Std NDCG: {np.std(y):.4f}")
    print(f"  Min NDCG: {np.min(y):.4f}")
    print(f"  Max NDCG: {np.max(y):.4f}")
    
    # Train model
    model = trainer.train_model(X, y, alpha=args.alpha)
    
    # Save model
    trainer.save_model(model, args.output_model)
    
    print("\n" + "="*80)
    print("TRAINING COMPLETE!")
    print("="*80)
    print(f"Model saved to: {args.output_model}")
    print("Use with evaluation script to test performance")
    print("="*80)


if __name__ == "__main__":
    main()
