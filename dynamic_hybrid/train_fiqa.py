#!/usr/bin/env python3
"""
Train a dynamic weight predictor for FiQA dataset.
FiQA has separate train/test sets and binary ratings (only relevant docs listed).
"""

import json
import pandas as pd
import numpy as np
import pickle
import string
import requests
import argparse
import random
from collections import defaultdict
from opensearchpy import OpenSearch
from tqdm import tqdm
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_squared_error
import warnings
warnings.filterwarnings('ignore')

# OpenSearch configuration for FiQA
OPENSEARCH_HOST = 'opense-clust-CEpYj56iJ4nM-c3649350d257fde2.elb.us-east-1.amazonaws.com'
OPENSEARCH_PORT = 80
INDEX_NAME = 'fiqa'
MODEL_ID = 'vh0g4ZoB6hz8mTHzcrdG'

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


class FiQAOpenSearchClient:
    """OpenSearch client for FiQA hybrid search"""
    
    def __init__(self):
        self.client = OpenSearch(
            hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
        
        # Verify connection
        if not self.client.ping():
            raise Exception(f"Cannot connect to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
        
        print(f"Connected to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
        print(f"Using index: {INDEX_NAME}")
        print(f"Using model: {MODEL_ID}")
    
    def execute_hybrid_search(self, query, neural_weight, size=100):
        """Execute hybrid search with given neural/lexical weights"""
        lexical_weight = round(1.0 - neural_weight, 2)
        
        url = f"http://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}/{INDEX_NAME}/_search"
        headers = {'Content-Type': 'application/json'}
        
        # Use lexical-only search when neural weight is 0
        if neural_weight == 0.0:
            payload = {
                "_source": ["_id"],
                "query": {
                    "multi_match": {
                        "query": query,
                        "type": "best_fields",
                        "operator": "or",  # Changed to "or" for better recall
                        "fields": ["title_key^2", "text_key"]
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
                        "passage_embedding": {  # Fixed field name
                            "query_text": query,
                            "model_id": MODEL_ID,
                            "k": size
                        }
                    }
                },
                "size": size
            }
        # Use hybrid search for mixed weights
        else:
            payload = {
                "_source": {"excludes": ["passage_embedding"]},
                "query": {
                    "hybrid": {
                        "queries": [
                            {
                                "neural": {
                                    "passage_embedding": {  # Fixed field name
                                        "query_text": query,
                                        "model_id": MODEL_ID,
                                        "k": 100
                                    }
                                }
                            },
                            {
                                "multi_match": {
                                    "query": query,
                                    "type": "best_fields",
                                    "operator": "or",  # Changed to "or" for better recall
                                    "fields": ["title_key^2", "text_key"]
                                }
                            }
                        ]
                    }
                },
                "search_pipeline": {
                    "description": "FiQA hybrid search",
                    "phase_results_processors": [
                        {
                            "normalization-processor": {
                                "normalization": {"technique": "min_max"},  # Changed to min_max
                                "combination": {
                                    "technique": "arithmetic_mean",
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


def load_fiqa_data(split='train'):
    """Load FiQA queries and ratings for specified split"""
    # Load queries
    queries = {}
    with open('datasets/fiqa/queries.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            queries[data['_id']] = data['text']
    
    # Load ratings (only relevant documents are listed with score 1)
    ratings_file = f'datasets/fiqa/qrels/{split}.tsv'
    ratings_data = []
    
    with open(ratings_file, 'r', encoding='utf-8') as f:
        next(f)  # Skip header
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                query_id = parts[0]
                doc_id = parts[1]
                # Binary ratings: all listed docs are relevant (1), unlisted are not relevant (0)
                rating = 1
                
                ratings_data.append({
                    'query_id': query_id,
                    'doc_id': doc_id,
                    'rating': rating
                })
    
    # Get unique query IDs from ratings
    query_ids_in_ratings = set(r['query_id'] for r in ratings_data)
    
    # Filter queries to only those with ratings
    filtered_queries = {qid: text for qid, text in queries.items() if qid in query_ids_in_ratings}
    
    return filtered_queries, ratings_data


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


def compute_ndcg_at_k(ranked_docs, relevance_dict, k=10):
    """Compute NDCG@k for a ranked list of documents (binary relevance)."""
    if not ranked_docs:
        return 0.0
    
    relevance_scores = []
    for doc_id in ranked_docs[:k]:
        # Binary relevance: 1 if in relevance_dict, 0 otherwise
        relevance_scores.append(1 if doc_id in relevance_dict else 0)
    
    dcg = 0.0
    for i, rel in enumerate(relevance_scores):
        dcg += (2**rel - 1) / np.log2(i + 2)
    
    # For binary relevance, ideal is all 1s up to min(k, num_relevant)
    num_relevant = len(relevance_dict)
    ideal_scores = [1] * min(k, num_relevant) + [0] * max(0, k - num_relevant)
    
    idcg = 0.0
    for i, rel in enumerate(ideal_scores):
        idcg += (2**rel - 1) / np.log2(i + 2)
    
    return dcg / idcg if idcg > 0 else 0.0


def find_optimal_weight(query_text, relevant_docs, opensearch_client, weight_granularity=0.1):
    """Find the optimal weight for a query that maximizes NDCG@10"""
    best_weight = 0.0
    best_ndcg = 0.0
    
    for neural_weight in np.arange(0.0, 1.1, weight_granularity):
        neural_weight = round(neural_weight, 1)
        
        # Execute search
        ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
        
        # Compute NDCG (binary relevance)
        ndcg = compute_ndcg_at_k(ranked_docs, relevant_docs, k=10)
        
        # Track best weight
        lexical_weight = round(1.0 - neural_weight, 1)
        if ndcg >= best_ndcg:
            best_ndcg = ndcg
            best_weight = lexical_weight
    
    return best_weight, best_ndcg


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Train FiQA dynamic weight predictor')
    parser.add_argument('--sample-size', type=int, default=None,
                        help='Number of queries to sample for training (default: use all)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for sampling (default: 42)')
    args = parser.parse_args()
    
    print("="*70)
    print("FiQA Dynamic Weight Predictor Training")
    print("="*70)
    
    # Initialize OpenSearch client
    opensearch_client = FiQAOpenSearchClient()
    
    # Load training data
    print("\nLoading FiQA training data...")
    train_queries, train_ratings = load_fiqa_data('train')
    print(f"Loaded {len(train_queries)} training queries")
    print(f"Loaded {len(train_ratings)} training ratings")
    
    # Sample queries if requested
    if args.sample_size and args.sample_size < len(train_queries):
        print(f"\nSampling {args.sample_size} queries from {len(train_queries)} total...")
        random.seed(args.seed)
        sampled_ids = random.sample(list(train_queries.keys()), args.sample_size)
        train_queries = {qid: train_queries[qid] for qid in sampled_ids}
        print(f"Using {len(train_queries)} sampled queries for training")
    
    # Group ratings by query
    query_ratings = defaultdict(set)  # Use set for binary relevance
    for rating in train_ratings:
        query_ratings[rating['query_id']].add(rating['doc_id'])
    
    # Find optimal weights for training queries
    print("\n" + "="*70)
    print("FINDING OPTIMAL WEIGHTS FOR TRAINING QUERIES")
    print("="*70)
    
    training_data = []
    
    for query_id, query_text in tqdm(train_queries.items(), desc="Processing queries"):
        if query_id not in query_ratings:
            continue
        
        # Get relevant docs for this query
        relevant_docs = query_ratings[query_id]
        
        # Find optimal weight
        optimal_weight, optimal_ndcg = find_optimal_weight(
            query_text, relevant_docs, opensearch_client
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
    print("OPTIMAL WEIGHT DISTRIBUTION (training set only)")
    print("="*70)
    print("optimal_weight")
    print(training_df['optimal_weight'].value_counts().sort_index())
    
    # Prepare features and targets
    feature_columns = [
        'query_length', 'has_numbers', 'has_special_chars', 'num_terms',
        'unique_terms_ratio', 'stopword_ratio', 'capitalization_ratio', 'has_punctuation'
    ]
    
    X = training_df[feature_columns].values
    y = training_df['optimal_weight'].values
    
    # Train model
    print("\n" + "="*70)
    print("TRAINING RIDGE REGRESSION MODEL")
    print("="*70)
    print("\nTraining Ridge regression model...")
    
    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train Ridge regression with cross-validation
    alphas = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
    model = RidgeCV(alphas=alphas, cv=5)
    model.fit(X_scaled, y)
    
    # Calculate training metrics
    y_pred = model.predict(X_scaled)
    train_mse = mean_squared_error(y, y_pred)
    train_r2 = model.score(X_scaled, y)
    
    print("\nTraining Results:")
    print(f"Best alpha: {model.alpha_}")
    print(f"Training MSE: {train_mse:.4f}")
    print(f"Training R²: {train_r2:.4f}")
    
    # Display feature importance
    print("\nFeature Importance (sorted by absolute coefficient):")
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'coefficient': model.coef_
    })
    feature_importance['abs_coef'] = np.abs(feature_importance['coefficient'])
    feature_importance = feature_importance.sort_values('abs_coef', ascending=False)
    feature_importance = feature_importance[['feature', 'coefficient']]
    
    # Format the output similar to TREC-COVID
    print(feature_importance.to_string(index=False, float_format=lambda x: f'{x:12.6f}'))
    
    # Save model and metadata
    model_data = {
        'model': model,
        'scaler': scaler,
        'feature_columns': feature_columns
    }
    
    with open('dynamic_hybrid/fiqa_model.pkl', 'wb') as f:
        pickle.dump(model_data, f)
    
    # Save metadata
    metadata = {
        'dataset': 'fiqa',
        'train_size': len(training_df),
        'best_alpha': float(model.alpha_),
        'r2_score': float(model.score(X_scaled, y)),
        'feature_columns': feature_columns,
        'opensearch_config': {
            'host': OPENSEARCH_HOST,
            'port': OPENSEARCH_PORT,
            'index': INDEX_NAME,
            'model_id': MODEL_ID
        }
    }
    
    with open('dynamic_hybrid/fiqa_model_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Save training data
    training_df.to_csv('dynamic_hybrid/fiqa_training_data.csv', index=False)
    
    print("\n" + "="*70)
    print("TRAINING COMPLETE")
    print("="*70)
    print(f"✓ Model saved to dynamic_hybrid/fiqa_model.pkl")
    print(f"✓ Metadata saved to dynamic_hybrid/fiqa_model_metadata.json")
    print(f"✓ Training data saved to dynamic_hybrid/fiqa_training_data.csv")
    print(f"✓ Trained on {len(training_df)} queries")


if __name__ == "__main__":
    main()
