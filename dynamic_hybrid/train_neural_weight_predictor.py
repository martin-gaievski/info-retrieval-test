#!/usr/bin/env python3
"""
Neural network training script for dynamic weight predictors.
Uses a 3-layer MLP with ReLU activations and sigmoid output to predict lexical weights.
"""

import json
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import argparse
import random
import os
import requests  # For OpenSearch client
from collections import defaultdict
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Import utilities from the refactored modules
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_loader import (
    check_dataset_exists,
    download_and_extract_dataset,
    load_dataset_with_split,
    load_dataset_with_separate_files
)
from utils.feature_extractor import (
    extract_query_features,
    get_feature_columns
)
from utils.evaluation_metrics import compute_ndcg_at_k
from utils.opensearch_client import OpenSearchClient
from utils.output_formatter import (
    create_metadata,
    format_training_summary
)

from opensearchpy import OpenSearch  # For legacy OpenSearch client

# Set random seeds for reproducibility
def set_seed(seed=42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)

# Common English stopwords
STOPWORDS = {
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 
    'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself',
    'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them',
    'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this',
    'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
    'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing',
    'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until',
    'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between',
    'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to',
    'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again',
    'further', 'then', 'once'
}


class WeightPredictorNetwork(nn.Module):
    """3-layer MLP for predicting lexical weight from query features."""
    
    def __init__(self, input_dim):
        super(WeightPredictorNetwork, self).__init__()
        self.layer1 = nn.Linear(input_dim, 150)
        self.layer2 = nn.Linear(150, 100)
        self.layer3 = nn.Linear(100, 50)
        self.output = nn.Linear(50, 1)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()
        
        # Initialize weights
        self._initialize_weights()
    
    def _initialize_weights(self):
        """Initialize network weights using Xavier initialization."""
        for module in self.modules():
            if isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                if module.bias is not None:
                    nn.init.zeros_(module.bias)
    
    def forward(self, x):
        x = self.relu(self.layer1(x))
        x = self.relu(self.layer2(x))
        x = self.relu(self.layer3(x))
        x = self.sigmoid(self.output(x))
        return x


class GenericOpenSearchClient:
    """Generic OpenSearch client for hybrid search"""
    
    def __init__(self, host, port, index_name, model_id, neural_field='passage_embedding',
                 lexical_fields=None, normalization='l2', combination='arithmetic_mean'):
        self.host = host
        self.port = port
        self.index_name = index_name
        self.model_id = model_id
        self.neural_field = neural_field
        self.lexical_fields = lexical_fields or ["title_key^2", "text_key"]
        self.normalization = normalization
        self.combination = combination
        
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        
        # Verify connection
        if not self.client.ping():
            raise Exception(f"Cannot connect to OpenSearch at {host}:{port}")
        
        print(f"Connected to OpenSearch at {host}:{port}")
        print(f"Using index: {index_name}")
        print(f"Using model: {model_id}")
        print(f"Neural field: {neural_field}")
        print(f"Lexical fields: {lexical_fields}")
        print(f"Normalization: {normalization}, Combination: {combination}")
    
    def execute_hybrid_search(self, query, neural_weight, size=100):
        """Execute hybrid search with given neural/lexical weights"""
        lexical_weight = round(1.0 - neural_weight, 2)
        
        url = f"http://{self.host}:{self.port}/{self.index_name}/_search"
        headers = {'Content-Type': 'application/json'}
        
        # Use lexical-only search when neural weight is 0
        if neural_weight == 0.0:
            payload = {
                "_source": ["_id"],
                "query": {
                    "multi_match": {
                        "query": query,
                        "type": "best_fields",
                        "operator": "or",
                        "fields": self.lexical_fields
                    }
                },
                "size": size
            }
        # Use neural-only search when lexical weight is 0
        elif lexical_weight == 0.0:
            payload = {
                "_source": ["_id"],
                "query": {
                    "neural": {
                        self.neural_field: {
                            "query_text": query,
                            "model_id": self.model_id,
                            "k": size
                        }
                    }
                },
                "size": size
            }
        # Use hybrid search for mixed weights
        else:
            payload = {
                "_source": {"excludes": [self.neural_field]},
                "query": {
                    "hybrid": {
                        "queries": [
                            {
                                "neural": {
                                    self.neural_field: {
                                        "query_text": query,
                                        "model_id": self.model_id,
                                        "k": 100
                                    }
                                }
                            },
                            {
                                "multi_match": {
                                    "query": query,
                                    "type": "best_fields",
                                    "operator": "or",
                                    "fields": self.lexical_fields
                                }
                            }
                        ]
                    }
                },
                "search_pipeline": {
                    "description": f"{self.index_name} hybrid search",
                    "phase_results_processors": [
                        {
                            "normalization-processor": {
                                "normalization": {"technique": self.normalization},
                                "combination": {
                                    "technique": self.combination,
                                    "parameters": {"weights": [neural_weight, lexical_weight]}
                                }
                            }
                        }
                    ]
                },
                "size": size
            }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()
            result = response.json()
            doc_ids = [hit['_id'] for hit in result.get('hits', {}).get('hits', [])]
            return doc_ids
        except Exception as e:
            # Return empty list on error
            return []




def find_optimal_weight(query_text, query_ratings, opensearch_client, binary_relevance=False):
    """Find the optimal weight for a query that maximizes NDCG@10"""
    if binary_relevance:
        relevant_docs = set(r['doc_id'] for r in query_ratings)
    else:
        relevant_docs = {r['doc_id']: r['rating'] for r in query_ratings}
    
    best_weight = 0.0
    best_ndcg = 0.0
    
    for neural_weight in np.arange(0.0, 1.1, 0.1):
        neural_weight = round(neural_weight, 1)
        
        # Execute search
        ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
        
        # Compute NDCG
        ndcg = compute_ndcg_at_k(ranked_docs, relevant_docs, k=10, binary=binary_relevance)
        
        # Track best weight
        lexical_weight = round(1.0 - neural_weight, 1)
        if ndcg >= best_ndcg:
            best_ndcg = ndcg
            best_weight = lexical_weight
    
    return best_weight, best_ndcg




def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Train neural network dynamic weight predictor')
    parser.add_argument('--dataset-path', type=str, required=True,
                        help='Path to dataset folder (e.g., datasets/fiqa)')
    parser.add_argument('--dataset-url', type=str, default=None,
                        help='URL to download dataset from if missing (default: BEIR repository URL)')
    parser.add_argument('--opensearch-host', type=str, required=True,
                        help='OpenSearch host')
    parser.add_argument('--opensearch-port', type=int, default=80,
                        help='OpenSearch port (default: 80)')
    parser.add_argument('--index-name', type=str, required=True,
                        help='OpenSearch index name')
    parser.add_argument('--model-id', type=str, required=True,
                        help='Neural model ID')
    parser.add_argument('--neural-field', type=str, default='passage_embedding',
                        help='Neural field name (default: passage_embedding)')
    parser.add_argument('--lexical-fields', type=str, nargs='+', 
                        default=['title_key^2', 'text_key'],
                        help='Lexical field names with optional boost')
    parser.add_argument('--requires-split', action='store_true',
                        help='Dataset requires train/test split')
    parser.add_argument('--split-ratio', type=float, default=0.8,
                        help='Train split ratio if requires-split is true (default: 0.8)')
    parser.add_argument('--binary-relevance', action='store_true',
                        help='Use binary relevance')
    parser.add_argument('--sample-size', type=int, default=None,
                        help='Number of queries to sample for training')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility (default: 42)')
    parser.add_argument('--model-name', type=str, default=None,
                        help='Name for the model files (default: {dataset}_neural_model)')
    parser.add_argument('--normalization', type=str, default='l2',
                        choices=['l2', 'min_max'],
                        help='Normalization technique for hybrid search (default: l2)')
    parser.add_argument('--combination', type=str, default='arithmetic_mean',
                        choices=['arithmetic_mean', 'geometric_mean', 'harmonic_mean'],
                        help='Combination technique for hybrid search (default: arithmetic_mean)')
    parser.add_argument('--epochs', type=int, default=100,
                        help='Number of training epochs (default: 100)')
    parser.add_argument('--batch-size', type=int, default=32,
                        help='Batch size for training (default: 32)')
    parser.add_argument('--learning-rate', type=float, default=0.001,
                        help='Learning rate for optimizer (default: 0.001)')
    parser.add_argument('--validation-split', type=float, default=0.2,
                        help='Validation split from training data (default: 0.2)')
    parser.add_argument('--early-stopping-patience', type=int, default=10,
                        help='Early stopping patience (default: 10)')
    args = parser.parse_args()
    
    # Set random seed
    set_seed(args.seed)
    
    # Set default model name based on dataset
    if args.model_name is None:
        dataset_name = os.path.basename(args.dataset_path.rstrip('/'))
        args.model_name = f"{dataset_name}_neural_model"
    
    print("="*70)
    print(f"Neural Network Dynamic Weight Predictor Training")
    print(f"Dataset: {args.dataset_path}")
    print("="*70)
    
    # Check if dataset exists, download if necessary
    if not check_dataset_exists(args.dataset_path):
        if args.dataset_url is None:
            dataset_name = os.path.basename(args.dataset_path.rstrip('/'))
            args.dataset_url = f"https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{dataset_name}.zip"
        
        download_and_extract_dataset(args.dataset_path, args.dataset_url)
        
        if not check_dataset_exists(args.dataset_path):
            raise FileNotFoundError(f"Dataset download/extraction failed.")
    else:
        print(f"Dataset found at {args.dataset_path}")
    
    # Load dataset based on type
    if args.requires_split:
        print("\nLoading dataset with train/test split...")
        train_queries, test_queries, ratings_data = load_dataset_with_split(
            args.dataset_path, args.split_ratio, args.seed
        )
    else:
        print("\nLoading dataset with separate train/test files...")
        train_queries, test_queries, ratings_data = load_dataset_with_separate_files(
            args.dataset_path
        )
    
    # Sample training queries if requested
    if args.sample_size and args.sample_size < len(train_queries):
        print(f"\nSampling {args.sample_size} queries from {len(train_queries)} total...")
        random.seed(args.seed)
        sampled_ids = random.sample(list(train_queries.keys()), args.sample_size)
        train_queries = {qid: train_queries[qid] for qid in sampled_ids}
        print(f"Using {len(train_queries)} sampled queries for training")
    
    # Initialize OpenSearch client
    opensearch_client = GenericOpenSearchClient(
        host=args.opensearch_host,
        port=args.opensearch_port,
        index_name=args.index_name,
        model_id=args.model_id,
        neural_field=args.neural_field,
        lexical_fields=args.lexical_fields,
        normalization=args.normalization,
        combination=args.combination
    )
    
    # Group ratings by query
    query_ratings = defaultdict(list)
    for rating in ratings_data:
        query_ratings[rating['query_id']].append(rating)
    
    # Find optimal weights for training queries
    print("\n" + "="*70)
    print("FINDING OPTIMAL WEIGHTS FOR TRAINING QUERIES")
    print("="*70)
    
    training_data = []
    
    for query_id, query_text in tqdm(train_queries.items(), desc="Processing queries"):
        if query_id not in query_ratings:
            continue
        
        # Find optimal weight
        optimal_weight, optimal_ndcg = find_optimal_weight(
            query_text, query_ratings[query_id], opensearch_client, args.binary_relevance
        )
        
        # Extract features
        features = extract_query_features(query_text)
        
        # Store training sample
        training_data.append({
            'query_id': query_id,
            'query_text': query_text,
            **features,
            'optimal_weight': optimal_weight,
            'optimal_ndcg': optimal_ndcg
        })
    
    # Convert to DataFrame
    training_df = pd.DataFrame(training_data)
    
    # Display distribution of optimal weights
    print("\n" + "="*70)
    print("OPTIMAL WEIGHT DISTRIBUTION")
    print("="*70)
    weight_distribution = training_df['optimal_weight'].value_counts()
    print(weight_distribution.sort_index())
    
    # Check for diversity
    unique_weights = len(weight_distribution)
    most_common_weight = weight_distribution.iloc[0]
    most_common_percentage = (most_common_weight / len(training_df)) * 100
    
    print(f"\nUnique weight values: {unique_weights}")
    print(f"Most common weight frequency: {most_common_percentage:.1f}%")
    
    if most_common_percentage > 80:
        print("\n⚠️  WARNING: Weight distribution is highly skewed!")
        print(f"   {most_common_percentage:.1f}% of queries have the same optimal weight.")
        print("   Neural network may struggle to learn meaningful patterns.")
    
    # Prepare features and targets
    feature_columns = [
        'query_length', 'has_numbers', 'has_special_chars', 'num_terms',
        'unique_terms_ratio', 'stopword_ratio', 'capitalization_ratio', 'has_punctuation'
    ]
    
    X = training_df[feature_columns].values
    y = training_df['optimal_weight'].values.reshape(-1, 1)
    
    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Split into train and validation
    X_train, X_val, y_train, y_val = train_test_split(
        X_scaled, y, test_size=args.validation_split, random_state=args.seed
    )
    
    # Convert to PyTorch tensors
    X_train_tensor = torch.FloatTensor(X_train)
    y_train_tensor = torch.FloatTensor(y_train)
    X_val_tensor = torch.FloatTensor(X_val)
    y_val_tensor = torch.FloatTensor(y_val)
    
    # Create DataLoaders
    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    
    val_dataset = TensorDataset(X_val_tensor, y_val_tensor)
    val_loader = DataLoader(val_dataset, batch_size=args.batch_size, shuffle=False)
    
    # Initialize model
    print("\n" + "="*70)
    print("TRAINING NEURAL NETWORK")
    print("="*70)
    
    input_dim = len(feature_columns)
    model = WeightPredictorNetwork(input_dim)
    
    # Loss function and optimizer
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=args.learning_rate)
    
    # Training loop with early stopping
    best_val_loss = float('inf')
    patience_counter = 0
    training_history = {
        'train_loss': [],
        'val_loss': [],
        'train_mae': [],
        'val_mae': []
    }
    
    print(f"\nTraining for up to {args.epochs} epochs")
    print(f"Batch size: {args.batch_size}")
    print(f"Learning rate: {args.learning_rate}")
    print(f"Early stopping patience: {args.early_stopping_patience}")
    print(f"Training samples: {len(X_train)}")
    print(f"Validation samples: {len(X_val)}")
    
    for epoch in range(args.epochs):
        # Training phase
        model.train()
        train_loss = 0.0
        train_mae = 0.0
        
        for batch_features, batch_targets in train_loader:
            optimizer.zero_grad()
            predictions = model(batch_features)
            loss = criterion(predictions, batch_targets)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item() * batch_features.size(0)
            train_mae += torch.mean(torch.abs(predictions - batch_targets)).item() * batch_features.size(0)
        
        train_loss /= len(train_loader.dataset)
        train_mae /= len(train_loader.dataset)
        
        # Validation phase
        model.eval()
        val_loss = 0.0
        val_mae = 0.0
        
        with torch.no_grad():
            for batch_features, batch_targets in val_loader:
                predictions = model(batch_features)
                loss = criterion(predictions, batch_targets)
                
                val_loss += loss.item() * batch_features.size(0)
                val_mae += torch.mean(torch.abs(predictions - batch_targets)).item() * batch_features.size(0)
        
        val_loss /= len(val_loader.dataset)
        val_mae /= len(val_loader.dataset)
        
        # Store history
        training_history['train_loss'].append(train_loss)
        training_history['val_loss'].append(val_loss)
        training_history['train_mae'].append(train_mae)
        training_history['val_mae'].append(val_mae)
        
        # Print progress
        if (epoch + 1) % 10 == 0 or epoch == 0:
            print(f"Epoch [{epoch+1}/{args.epochs}] - "
                  f"Train Loss: {train_loss:.4f}, Train MAE: {train_mae:.4f} - "
                  f"Val Loss: {val_loss:.4f}, Val MAE: {val_mae:.4f}")
        
        # Early stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            # Save best model
            best_model_state = model.state_dict()
        else:
            patience_counter += 1
            if patience_counter >= args.early_stopping_patience:
                print(f"\nEarly stopping triggered after {epoch+1} epochs")
                break
    
    # Load best model
    model.load_state_dict(best_model_state)
    
    # Final evaluation on full training set
    model.eval()
    with torch.no_grad():
        X_train_full = torch.FloatTensor(X_scaled)
        y_train_full = torch.FloatTensor(training_df['optimal_weight'].values.reshape(-1, 1))
        predictions_full = model(X_train_full)
        
        final_mse = criterion(predictions_full, y_train_full).item()
        final_mae = torch.mean(torch.abs(predictions_full - y_train_full)).item()
        
        # Calculate R²
        ss_res = torch.sum((y_train_full - predictions_full) ** 2)
        ss_tot = torch.sum((y_train_full - torch.mean(y_train_full)) ** 2)
        r2_score = 1 - (ss_res / ss_tot).item()
    
    print("\n" + "="*70)
    print("TRAINING RESULTS")
    print("="*70)
    print(f"Final MSE: {final_mse:.4f}")
    print(f"Final MAE: {final_mae:.4f}")
    print(f"Final R²: {r2_score:.4f}")
    print(f"Best Validation Loss: {best_val_loss:.4f}")
    
    # Analyze predictions
    predictions_np = predictions_full.numpy()
    print(f"\nPrediction Range: [{predictions_np.min():.3f}, {predictions_np.max():.3f}]")
    print(f"Prediction Mean: {predictions_np.mean():.3f}")
    print(f"Prediction Std: {predictions_np.std():.3f}")
    
    # Save model and metadata
    print("\n" + "="*70)
    print("SAVING MODEL")
    print("="*70)
    
    # Create directory if it doesn't exist
    os.makedirs('dynamic_hybrid', exist_ok=True)
    
    # Save PyTorch model
    model_path = f'dynamic_hybrid/{args.model_name}.pth'
    torch.save({
        'model_state_dict': model.state_dict(),
        'model_architecture': {
            'input_dim': input_dim,
            'layer_sizes': [150, 100, 50, 1]
        },
        'scaler': scaler,
        'feature_columns': feature_columns,
        'training_history': training_history,
        'final_metrics': {
            'mse': final_mse,
            'mae': final_mae,
            'r2': r2_score
        },
        'training_args': vars(args)
    }, model_path)
    
    # Save metadata
    metadata = {
        'dataset': os.path.basename(args.dataset_path),
        'dataset_path': args.dataset_path,
        'train_size': len(training_df),
        'test_size': len(test_queries),
        'validation_size': len(X_val),
        'model_type': 'neural_network',
        'architecture': '3-layer MLP (150-100-50-1)',
        'activation': 'ReLU',
        'output_activation': 'Sigmoid',
        'final_mse': float(final_mse),
        'final_mae': float(final_mae),
        'r2_score': float(r2_score),
        'best_val_loss': float(best_val_loss),
        'epochs_trained': len(training_history['train_loss']),
        'feature_columns': feature_columns,
        'normalization': args.normalization,
        'combination': args.combination,
        'requires_split': args.requires_split,
        'binary_relevance': args.binary_relevance,
        'opensearch_config': {
            'host': args.opensearch_host,
            'port': args.opensearch_port,
            'index': args.index_name,
            'model_id': args.model_id,
            'neural_field': args.neural_field,
            'lexical_fields': args.lexical_fields
        },
        'training_params': {
            'batch_size': args.batch_size,
            'learning_rate': args.learning_rate,
            'epochs': args.epochs,
            'early_stopping_patience': args.early_stopping_patience,
            'validation_split': args.validation_split
        }
    }
    
    metadata_path = f'dynamic_hybrid/{args.model_name}_metadata.json'
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Save training data
    training_data_path = f'dynamic_hybrid/{args.model_name}_training_data.csv'
    training_df.to_csv(training_data_path, index=False)
    
    print(f"✓ Model saved to {model_path}")
    print(f"✓ Metadata saved to {metadata_path}")
    print(f"✓ Training data saved to {training_data_path}")
    print(f"✓ Trained on {len(training_df)} queries")
    print(f"✓ Test set has {len(test_queries)} queries for evaluation")
    
    # Display weight distribution analysis
    print("\n" + "="*70)
    print("WEIGHT DISTRIBUTION ANALYSIS")
    print("="*70)
    
    # Compare actual vs predicted weights on training set
    actual_weights = training_df['optimal_weight'].values
    predicted_weights = predictions_np.flatten()
    
    # Round predictions to nearest 0.1 for comparison
    predicted_weights_rounded = np.round(predicted_weights * 10) / 10
    
    print("\nActual Weight Distribution:")
    unique, counts = np.unique(actual_weights, return_counts=True)
    for w, c in zip(unique, counts):
        print(f"  {w:.1f}: {c} ({c/len(actual_weights)*100:.1f}%)")
    
    print("\nPredicted Weight Distribution (rounded to 0.1):")
    unique, counts = np.unique(predicted_weights_rounded, return_counts=True)
    for w, c in zip(unique, counts):
        print(f"  {w:.1f}: {c} ({c/len(predicted_weights_rounded)*100:.1f}%)")
    
    print("\n" + "="*70)
    print("TRAINING COMPLETE")
    print("="*70)


if __name__ == "__main__":
    main()
