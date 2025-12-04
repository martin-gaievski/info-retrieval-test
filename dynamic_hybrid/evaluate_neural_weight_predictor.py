#!/usr/bin/env python3
"""
Evaluation script for neural network dynamic weight predictors.
Evaluates the performance of the trained neural network model on test data.
"""

import json
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import string
import requests
import argparse
import random
import os
from collections import defaultdict
from opensearchpy import OpenSearch
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

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
        self.lexical_fields = lexical_fields or ["title^2", "text"]
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


def load_test_queries(dataset_path, requires_split):
    """Load test queries based on dataset type"""
    queries = {}
    queries_file = os.path.join(dataset_path, 'queries.jsonl')
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            queries[data['_id']] = data['text']
    
    if requires_split:
        # Load saved test split
        test_split_file = os.path.join('dynamic_hybrid', f'{os.path.basename(dataset_path)}_test_split.json')
        if not os.path.exists(test_split_file):
            raise FileNotFoundError(f"Test split file not found: {test_split_file}. Please run training first.")
        
        with open(test_split_file, 'r') as f:
            test_data = json.load(f)
            test_ids = test_data['test_ids']
        
        test_queries = {qid: queries[qid] for qid in test_ids if qid in queries}
    else:
        # Load test ratings to get test query IDs
        test_ratings_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
        test_query_ids = set()
        
        with open(test_ratings_file, 'r', encoding='utf-8') as f:
            next(f)  # Skip header
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    test_query_ids.add(parts[0])
        
        test_queries = {qid: queries[qid] for qid in test_query_ids if qid in queries}
    
    return test_queries


def load_ratings(dataset_path):
    """Load all ratings from dataset"""
    ratings_data = []
    
    # Check for test.tsv (datasets with split) or both train.tsv and test.tsv
    ratings_files = []
    test_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
    train_file = os.path.join(dataset_path, 'qrels', 'train.tsv')
    dev_file = os.path.join(dataset_path, 'qrels', 'dev.tsv')
    
    if os.path.exists(test_file):
        ratings_files.append(test_file)
    if os.path.exists(train_file):
        ratings_files.append(train_file)
    elif os.path.exists(dev_file):
        ratings_files.append(dev_file)
    
    for ratings_file in ratings_files:
        with open(ratings_file, 'r', encoding='utf-8') as f:
            next(f)  # Skip header
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    query_id = parts[0]
                    doc_id = parts[1]
                    rating = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
                    
                    ratings_data.append({
                        'query_id': query_id,
                        'doc_id': doc_id,
                        'rating': rating
                    })
    
    return ratings_data


def extract_query_features(query):
    """Extract query-only features from a query string."""
    features = {}
    
    features['query_length'] = len(query)
    features['has_numbers'] = 1 if any(c.isdigit() for c in query) else 0
    
    special_chars = set(string.punctuation) - {' ', '.', ',', '?', '!', '-', "'"}
    features['has_special_chars'] = 1 if any(c in special_chars for c in query) else 0
    
    terms = query.lower().split()
    features['num_terms'] = len(terms)
    
    unique_terms = set(terms)
    features['unique_terms_ratio'] = len(unique_terms) / len(terms) if terms else 0
    
    stopword_count = sum(1 for term in terms if term in STOPWORDS)
    features['stopword_ratio'] = stopword_count / len(terms) if terms else 0
    
    letters = [c for c in query if c.isalpha()]
    capital_letters = [c for c in letters if c.isupper()]
    features['capitalization_ratio'] = len(capital_letters) / len(letters) if letters else 0
    
    features['has_punctuation'] = 1 if query.rstrip() and query.rstrip()[-1] in string.punctuation else 0
    
    return features


def compute_ndcg_at_k(ranked_docs, relevance_dict, k=10, binary=False):
    """Compute NDCG@k for a ranked list of documents."""
    if not ranked_docs:
        return 0.0
    
    relevance_scores = []
    for doc_id in ranked_docs[:k]:
        if binary:
            # Binary relevance: 1 if in relevance_dict, 0 otherwise
            relevance_scores.append(1 if doc_id in relevance_dict else 0)
        else:
            # Graded relevance
            relevance_scores.append(relevance_dict.get(doc_id, 0))
    
    dcg = 0.0
    for i, rel in enumerate(relevance_scores):
        dcg += (2**rel - 1) / np.log2(i + 2)
    
    if binary:
        # For binary relevance, ideal is all 1s up to min(k, num_relevant)
        num_relevant = len(relevance_dict)
        ideal_scores = [1] * min(k, num_relevant) + [0] * max(0, k - num_relevant)
    else:
        # For graded relevance
        ideal_scores = sorted(relevance_dict.values(), reverse=True)[:k]
    
    idcg = 0.0
    for i, rel in enumerate(ideal_scores):
        idcg += (2**rel - 1) / np.log2(i + 2)
    
    return dcg / idcg if idcg > 0 else 0.0


def compute_ndcg_at_multiple_k(ranked_docs, relevance_dict, k_values=[1, 10, 100], binary=False):
    """Compute NDCG at multiple k values."""
    results = {}
    for k in k_values:
        results[f'ndcg@{k}'] = compute_ndcg_at_k(ranked_docs, relevance_dict, k, binary)
    return results


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Evaluate neural network dynamic weight predictor')
    parser.add_argument('--model-name', type=str, required=True,
                        help='Name of the model files to load')
    parser.add_argument('--dataset-path', type=str, default=None,
                        help='Override dataset path (default: use from model)')
    parser.add_argument('--sample-size', type=int, default=None,
                        help='Number of queries to sample for evaluation (default: use all)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for sampling (default: 42)')
    args = parser.parse_args()
    
    print("="*70)
    print("Neural Network Dynamic Weight Predictor Evaluation")
    print("="*70)
    
    # Load model and metadata
    print(f"\nLoading model: {args.model_name}...")
    model_path = f'dynamic_hybrid/{args.model_name}.pth'
    metadata_path = f'dynamic_hybrid/{args.model_name}_metadata.json'
    
    # Load PyTorch model checkpoint
    # weights_only=False needed because model contains sklearn.StandardScaler
    checkpoint = torch.load(model_path, map_location=torch.device('cpu'), weights_only=False)
    
    # Load metadata
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    # Initialize model
    model_architecture = checkpoint['model_architecture']
    input_dim = model_architecture['input_dim']
    model = WeightPredictorNetwork(input_dim)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    
    # Load scaler and feature columns
    scaler = checkpoint['scaler']
    feature_columns = checkpoint['feature_columns']
    
    # Get dataset configuration
    training_args = checkpoint['training_args']
    
    # Use provided dataset path or fall back to model's configuration
    if args.dataset_path:
        dataset_path = args.dataset_path
    else:
        dataset_path = training_args.get('dataset_path')
        if not dataset_path:
            raise ValueError("No dataset path found in model config. Please provide --dataset-path")
    
    requires_split = training_args.get('requires_split', False)
    binary_relevance = training_args.get('binary_relevance', False)
    neural_field = training_args.get('neural_field', 'passage_embedding')
    lexical_fields = training_args.get('lexical_fields', ['title_key^2', 'text_key'])
    
    # Print model information
    print(f"Dataset: {metadata['dataset']}")
    print(f"Model trained on {metadata['train_size']} queries")
    print(f"Model type: {metadata['model_type']}")
    print(f"Architecture: {metadata['architecture']}")
    print(f"Final MSE: {metadata['final_mse']:.4f}")
    print(f"Final MAE: {metadata['final_mae']:.4f}")
    print(f"R² Score: {metadata['r2_score']:.4f}")
    print(f"Epochs trained: {metadata['epochs_trained']}")
    
    # Get OpenSearch configuration
    opensearch_config = metadata['opensearch_config']
    
    # Initialize OpenSearch client
    opensearch_client = GenericOpenSearchClient(
        host=opensearch_config['host'],
        port=opensearch_config['port'],
        index_name=opensearch_config['index'],
        model_id=opensearch_config['model_id'],
        neural_field=neural_field,
        lexical_fields=lexical_fields,
        normalization=training_args.get('normalization', 'l2'),
        combination=training_args.get('combination', 'arithmetic_mean')
    )
    
    # Load test queries and ratings
    print("\nLoading test data...")
    test_queries = load_test_queries(dataset_path, requires_split)
    ratings_data = load_ratings(dataset_path)
    
    print(f"Loaded {len(test_queries)} test queries")
    print(f"Loaded {len(ratings_data)} ratings")
    
    # Sample queries if requested
    if args.sample_size and args.sample_size < len(test_queries):
        print(f"\nSampling {args.sample_size} queries from {len(test_queries)} total...")
        random.seed(args.seed)
        sampled_ids = random.sample(list(test_queries.keys()), args.sample_size)
        test_queries = {qid: test_queries[qid] for qid in sampled_ids}
        print(f"Using {len(test_queries)} sampled queries for evaluation")
    
    # Group ratings by query
    query_ratings = defaultdict(list)
    for rating in ratings_data:
        query_ratings[rating['query_id']].append(rating)
    
    # Define k values for evaluation
    k_values = [1, 10, 100]
    print(f"\nWill evaluate at k values: {k_values}")
    
    # 1. Evaluate static weights on test set
    print("\n" + "="*70)
    print("1. EVALUATING STATIC WEIGHTS ON TEST SET")
    print("="*70)
    
    print("\nTesting different weight combinations...")
    static_results_by_k = {k: {} for k in k_values}
    
    weight_steps = list(np.arange(0.0, 1.1, 0.1))
    for neural_weight in tqdm(weight_steps, desc="Testing weights"):
        neural_weight = round(neural_weight, 1)
        lexical_weight = round(1.0 - neural_weight, 1)
        
        # Accumulate NDCG for each k
        total_ndcg_by_k = {k: 0.0 for k in k_values}
        query_count = 0
        
        for query_id, query_text in test_queries.items():
            if query_id not in query_ratings:
                continue
            
            # Prepare relevance dict based on type
            if binary_relevance:
                relevant_docs = set(r['doc_id'] for r in query_ratings[query_id])
            else:
                relevant_docs = {r['doc_id']: r['rating'] for r in query_ratings[query_id]}
            
            ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
            ndcg_scores = compute_ndcg_at_multiple_k(ranked_docs, relevant_docs, k_values, binary_relevance)
            
            for k in k_values:
                total_ndcg_by_k[k] += ndcg_scores[f'ndcg@{k}']
            query_count += 1
        
        # Store average NDCG for each k
        for k in k_values:
            avg_ndcg = total_ndcg_by_k[k] / query_count if query_count > 0 else 0.0
            static_results_by_k[k][lexical_weight] = avg_ndcg
    
    # Display results for each k
    print("\nStatic Weight Results (Test Set):")
    for k in k_values:
        print(f"\n--- NDCG@{k} ---")
        print("-" * 40)
        for weight, ndcg in sorted(static_results_by_k[k].items()):
            print(f"Lexical {weight:.1f}: NDCG@{k} = {ndcg:.4f}")
        
        best_weight = max(static_results_by_k[k], key=static_results_by_k[k].get)
        best_ndcg = static_results_by_k[k][best_weight]
        print(f"Best for NDCG@{k}: Lexical {best_weight:.1f} = {best_ndcg:.4f}")
    
    # Use NDCG@10 as primary metric
    static_results = static_results_by_k[10]
    best_static_weight = max(static_results, key=static_results.get)
    best_static_ndcg = static_results[best_static_weight]
    
    # 2. Evaluate neural network model on test set
    print("\n" + "="*70)
    print("2. EVALUATING NEURAL NETWORK MODEL ON TEST SET")
    print("="*70)
    
    print("\nEvaluating neural network predictions...")
    query_results = []
    total_dynamic_ndcg_by_k = {k: 0.0 for k in k_values}
    
    for query_id, query_text in tqdm(sorted(test_queries.items()), desc="Processing queries"):
        if query_id not in query_ratings:
            continue
        
        # Extract features
        features = extract_query_features(query_text)
        X = pd.DataFrame([features])[feature_columns]
        X_scaled = scaler.transform(X)
        
        # Predict weight using neural network
        with torch.no_grad():
            X_tensor = torch.FloatTensor(X_scaled)
            predicted_weight_raw = model(X_tensor).item()
        
        # Round to nearest 0.1
        predicted_weight = round(np.clip(predicted_weight_raw, 0, 1) * 10) / 10
        
        # Find actual optimal weight for this query (for NDCG@10)
        if binary_relevance:
            relevant_docs = set(r['doc_id'] for r in query_ratings[query_id])
        else:
            relevant_docs = {r['doc_id']: r['rating'] for r in query_ratings[query_id]}
        
        best_weight = 0.0
        best_ndcg = 0.0
        for neural_weight in np.arange(0.0, 1.1, 0.1):
            neural_weight = round(neural_weight, 1)
            ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
            ndcg = compute_ndcg_at_k(ranked_docs, relevant_docs, k=10, binary=binary_relevance)
            lexical_weight = round(1.0 - neural_weight, 1)
            if ndcg >= best_ndcg:
                best_ndcg = ndcg
                best_weight = lexical_weight
        
        # Evaluate with predicted weight at multiple k values
        neural_weight = round(1.0 - predicted_weight, 1)
        ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
        ndcg_scores = compute_ndcg_at_multiple_k(ranked_docs, relevant_docs, k_values, binary_relevance)
        
        # Accumulate NDCG for each k
        for k in k_values:
            total_dynamic_ndcg_by_k[k] += ndcg_scores[f'ndcg@{k}']
        
        query_results.append({
            'query_id': query_id,
            'query_text': query_text[:50] + '...' if len(query_text) > 50 else query_text,
            'optimal_weight': best_weight,
            'predicted_weight': predicted_weight,
            'raw_prediction': predicted_weight_raw,
            'optimal_ndcg': best_ndcg,
            'dynamic_ndcg': ndcg_scores['ndcg@10'],
            'difference': abs(best_weight - predicted_weight)
        })
    
    # Calculate average NDCG for each k
    avg_dynamic_ndcg_by_k = {}
    for k in k_values:
        avg_dynamic_ndcg_by_k[k] = total_dynamic_ndcg_by_k[k] / len(query_results) if query_results else 0.0
    
    # Use NDCG@10 as primary metric
    avg_dynamic_ndcg = avg_dynamic_ndcg_by_k[10]
    
    # Display dynamic model results
    print("\nNeural Network Model Results:")
    print("-" * 40)
    for k in k_values:
        print(f"Neural Network NDCG@{k}: {avg_dynamic_ndcg_by_k[k]:.4f}")
    
    # 3. Summary
    print("\n" + "="*70)
    print("EVALUATION SUMMARY")
    print("="*70)
    
    print(f"\nDataset: {metadata['dataset']}")
    print(f"Training Set Size: {metadata['train_size']} queries")
    print(f"Test Set Size: {len(test_queries)} queries")
    
    # Show comparison for each k value
    print("\n" + "="*70)
    print("PERFORMANCE COMPARISON AT DIFFERENT K VALUES")
    print("="*70)
    
    for k in k_values:
        best_static_k = max(static_results_by_k[k], key=static_results_by_k[k].get)
        best_static_ndcg_k = static_results_by_k[k][best_static_k]
        dynamic_ndcg_k = avg_dynamic_ndcg_by_k[k]
        improvement_k = ((dynamic_ndcg_k - best_static_ndcg_k) / best_static_ndcg_k) * 100 if best_static_ndcg_k > 0 else 0
        
        print(f"\n--- NDCG@{k} ---")
        print(f"Best Static Weight: Lexical {best_static_k:.1f}")
        print(f"Best Static NDCG@{k}: {best_static_ndcg_k:.4f}")
        print(f"Neural Network NDCG@{k}: {dynamic_ndcg_k:.4f}")
        print(f"Improvement: {improvement_k:+.2f}%")
    
    # Primary metric (NDCG@10) summary
    print("\n" + "="*70)
    print("PRIMARY METRIC (NDCG@10)")
    print("="*70)
    
    print(f"\nBest Static Weight: Lexical {best_static_weight:.1f}")
    print(f"Best Static NDCG@10: {best_static_ndcg:.4f}")
    print(f"\nNeural Network NDCG@10: {avg_dynamic_ndcg:.4f}")
    
    improvement = ((avg_dynamic_ndcg - best_static_ndcg) / best_static_ndcg) * 100 if best_static_ndcg > 0 else 0
    print(f"Improvement over best static: {improvement:+.2f}%")
    
    # Save results
    results = {
        'dataset': metadata['dataset'],
        'model_type': 'neural_network',
        'train_queries': metadata['train_size'],
        'test_queries': len(test_queries),
        'static_results': {f'{k:.1f}': float(v) for k, v in static_results.items()},
        'static_results_by_k': {
            k: {f'{w:.1f}': float(v) for w, v in static_results_by_k[k].items()}
            for k in k_values
        },
        'best_static_weight': float(best_static_weight),
        'best_static_ndcg': float(best_static_ndcg),
        'dynamic_ndcg': float(avg_dynamic_ndcg),
        'dynamic_ndcg_by_k': {k: float(v) for k, v in avg_dynamic_ndcg_by_k.items()},
        'improvement_percent': float(improvement),
        'per_query_results': query_results[:10],  # Save first 10 for brevity
        'model': {
            'architecture': metadata['architecture'],
            'final_mse': metadata['final_mse'],
            'final_mae': metadata['final_mae'],
            'r2_score': metadata['r2_score'],
            'epochs_trained': metadata['epochs_trained']
        }
    }
    
    evaluation_path = f'dynamic_hybrid/{args.model_name}_evaluation.json'
    with open(evaluation_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {evaluation_path}")
    
    # Display prediction distribution
    print("\n" + "="*70)
    print("PREDICTION ANALYSIS")
    print("="*70)
    
    prediction_df = pd.DataFrame(query_results)
    print("\nPredicted Weight Distribution:")
    print(prediction_df['predicted_weight'].value_counts().sort_index())
    
    print("\nOptimal Weight Distribution:")
    print(prediction_df['optimal_weight'].value_counts().sort_index())
    
    print("\nMean Absolute Error: {:.2f}".format(prediction_df['difference'].mean()))
    
    # Show raw prediction statistics
    print("\nRaw Prediction Statistics (before rounding):")
    raw_predictions = prediction_df['raw_prediction'].values
    print(f"Min: {raw_predictions.min():.4f}")
    print(f"Max: {raw_predictions.max():.4f}")
    print(f"Mean: {raw_predictions.mean():.4f}")
    print(f"Std: {raw_predictions.std():.4f}")


if __name__ == "__main__":
    main()
