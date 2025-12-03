#!/usr/bin/env python3
"""
Generic training script for dynamic weight predictors across different datasets.
Supports both datasets with single query file (requiring split) and separate train/test files.
Automatically downloads missing datasets from BEIR repository.
"""

import json
import pandas as pd
import numpy as np
import pickle
import string
import requests
import argparse
import random
import os
import urllib.request
import zipfile
from collections import defaultdict
from opensearchpy import OpenSearch
from sklearn.model_selection import train_test_split
from tqdm import tqdm
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV
from sklearn.metrics import mean_squared_error
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


def load_dataset_with_split(dataset_path, split_ratio=0.8, random_seed=42):
    """
    Load dataset that has a single query file and requires train/test split.
    Used for datasets like trec-covid and nq.
    """
    # Load queries
    queries = {}
    queries_file = os.path.join(dataset_path, 'queries.jsonl')
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            queries[data['_id']] = data['text']
    
    print(f"Loaded {len(queries)} total queries")
    
    # Load ratings (assuming test.tsv contains all ratings)
    ratings_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
    ratings_data = []
    
    with open(ratings_file, 'r', encoding='utf-8') as f:
        next(f)  # Skip header
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                query_id = parts[0]
                doc_id = parts[1]
                rating = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
                
                if query_id in queries:
                    ratings_data.append({
                        'query_id': query_id,
                        'doc_id': doc_id,
                        'rating': rating
                    })
    
    print(f"Loaded {len(ratings_data)} ratings")
    
    # Split queries into train/test
    query_ids = list(queries.keys())
    train_ids, test_ids = train_test_split(
        query_ids, 
        train_size=split_ratio, 
        random_state=random_seed
    )
    
    print(f"Split: {len(train_ids)} train queries, {len(test_ids)} test queries")
    
    # Create train and test query dictionaries
    train_queries = {qid: queries[qid] for qid in train_ids}
    test_queries = {qid: queries[qid] for qid in test_ids}
    
    # Save test split for reproducibility
    test_split_file = os.path.join('dynamic_hybrid', f'{os.path.basename(dataset_path)}_test_split.json')
    with open(test_split_file, 'w') as f:
        json.dump({'test_ids': test_ids}, f, indent=2)
    print(f"Saved test split to {test_split_file}")
    
    return train_queries, test_queries, ratings_data


def load_dataset_with_separate_files(dataset_path):
    """
    Load dataset that has separate train/test query files.
    Used for datasets like fiqa and quora.
    Supports both train.tsv and dev.tsv for training data.
    """
    # Load queries
    queries = {}
    queries_file = os.path.join(dataset_path, 'queries.jsonl')
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            queries[data['_id']] = data['text']
    
    # Load train ratings - check for train.tsv first, then dev.tsv
    train_ratings_file = os.path.join(dataset_path, 'qrels', 'train.tsv')
    dev_ratings_file = os.path.join(dataset_path, 'qrels', 'dev.tsv')
    
    # Determine which training file to use
    if os.path.exists(train_ratings_file):
        training_file = train_ratings_file
        print(f"Using train.tsv for training data")
    elif os.path.exists(dev_ratings_file):
        training_file = dev_ratings_file
        print(f"Using dev.tsv for training data (train.tsv not found)")
    else:
        raise FileNotFoundError(f"No training data found. Checked for train.tsv and dev.tsv in {os.path.join(dataset_path, 'qrels')}")
    
    train_ratings = []
    
    with open(training_file, 'r', encoding='utf-8') as f:
        next(f)  # Skip header
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                query_id = parts[0]
                doc_id = parts[1]
                rating = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
                
                train_ratings.append({
                    'query_id': query_id,
                    'doc_id': doc_id,
                    'rating': rating
                })
    
    # Load test ratings
    test_ratings_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
    test_ratings = []
    
    with open(test_ratings_file, 'r', encoding='utf-8') as f:
        next(f)  # Skip header
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                query_id = parts[0]
                doc_id = parts[1]
                rating = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
                
                test_ratings.append({
                    'query_id': query_id,
                    'doc_id': doc_id,
                    'rating': rating
                })
    
    # Get unique query IDs from ratings
    train_query_ids = set(r['query_id'] for r in train_ratings)
    test_query_ids = set(r['query_id'] for r in test_ratings)
    
    # Filter queries
    train_queries = {qid: queries[qid] for qid in train_query_ids if qid in queries}
    test_queries = {qid: queries[qid] for qid in test_query_ids if qid in queries}
    
    print(f"Loaded {len(train_queries)} train queries, {len(test_queries)} test queries")
    print(f"Loaded {len(train_ratings)} train ratings, {len(test_ratings)} test ratings")
    
    # Combine ratings for processing
    all_ratings = train_ratings + test_ratings
    
    return train_queries, test_queries, all_ratings


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


def find_optimal_weight(query_text, query_ratings, opensearch_client, binary_relevance=False):
    """Find the optimal weight for a query that maximizes NDCG@10"""
    if binary_relevance:
        # For binary relevance, convert to set
        relevant_docs = set(r['doc_id'] for r in query_ratings)
    else:
        # For graded relevance, create dict
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


def download_and_extract_dataset(dataset_path, dataset_url):
    """
    Download and extract dataset if it doesn't exist.
    
    Args:
        dataset_path: Local path where dataset should be stored
        dataset_url: URL to download dataset from
    """
    print(f"Dataset not found at {dataset_path}")
    print(f"Downloading from {dataset_url}...")
    
    # Create parent directory if it doesn't exist
    parent_dir = os.path.dirname(dataset_path)
    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    
    # Download the zip file
    zip_path = dataset_path + '.zip'
    try:
        with tqdm(unit='B', unit_scale=True, desc="Downloading") as t:
            def download_hook(block_num, block_size, total_size):
                if total_size > 0:
                    t.total = total_size
                t.update(block_size)
            
            urllib.request.urlretrieve(dataset_url, zip_path, reporthook=download_hook)
        
        print(f"Downloaded to {zip_path}")
        
        # Extract the zip file
        print(f"Extracting dataset...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract to parent directory - the zip should contain the dataset folder
            zip_ref.extractall(parent_dir)
        
        # Remove the zip file after extraction
        os.remove(zip_path)
        print(f"Dataset extracted to {dataset_path}")
        
        # Verify extraction was successful
        if not os.path.exists(dataset_path):
            raise FileNotFoundError(f"Dataset extraction failed. Expected folder not found: {dataset_path}")
            
    except Exception as e:
        # Clean up zip file if download/extraction failed
        if os.path.exists(zip_path):
            os.remove(zip_path)
        raise Exception(f"Failed to download/extract dataset: {str(e)}")


def check_dataset_exists(dataset_path):
    """
    Check if dataset exists and has required files.
    
    Returns:
        bool: True if dataset exists with required files, False otherwise
    """
    if not os.path.exists(dataset_path):
        return False
    
    # Check for essential files
    queries_file = os.path.join(dataset_path, 'queries.jsonl')
    qrels_dir = os.path.join(dataset_path, 'qrels')
    
    if not os.path.exists(queries_file):
        return False
    
    if not os.path.exists(qrels_dir):
        return False
    
    # Check if qrels directory has any .tsv files
    tsv_files = [f for f in os.listdir(qrels_dir) if f.endswith('.tsv')]
    if not tsv_files:
        return False
    
    return True


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Train generic dynamic weight predictor')
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
                        help='Lexical field names with optional boost (default: title_key^2 text_key)')
    parser.add_argument('--requires-split', action='store_true',
                        help='Dataset requires train/test split (like trec-covid, nq)')
    parser.add_argument('--split-ratio', type=float, default=0.8,
                        help='Train split ratio if requires-split is true (default: 0.8)')
    parser.add_argument('--binary-relevance', action='store_true',
                        help='Use binary relevance (all relevant docs have score 1)')
    parser.add_argument('--sample-size', type=int, default=None,
                        help='Number of queries to sample for training (default: use all)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for sampling and splitting (default: 42)')
    parser.add_argument('--model-name', type=str, default=None,
                        help='Name for the model files (default: {dataset}_model)')
    parser.add_argument('--normalization', type=str, default='l2',
                        choices=['l2', 'min_max'],
                        help='Normalization technique for hybrid search (default: l2)')
    parser.add_argument('--combination', type=str, default='arithmetic_mean',
                        choices=['arithmetic_mean', 'geometric_mean', 'harmonic_mean'],
                        help='Combination technique for hybrid search (default: arithmetic_mean)')
    args = parser.parse_args()
    
    # Set default model name based on dataset
    if args.model_name is None:
        dataset_name = os.path.basename(args.dataset_path.rstrip('/'))
        args.model_name = f"{dataset_name}_model"
    
    print("="*70)
    print(f"Generic Dynamic Weight Predictor Training")
    print(f"Dataset: {args.dataset_path}")
    print("="*70)
    
    # Check if dataset exists, download if necessary
    if not check_dataset_exists(args.dataset_path):
        # Construct default BEIR URL if not provided
        if args.dataset_url is None:
            dataset_name = os.path.basename(args.dataset_path.rstrip('/'))
            args.dataset_url = f"https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{dataset_name}.zip"
        
        # Download and extract dataset
        download_and_extract_dataset(args.dataset_path, args.dataset_url)
        
        # Verify dataset is now available
        if not check_dataset_exists(args.dataset_path):
            raise FileNotFoundError(f"Dataset download/extraction failed. Please check the URL or manually download the dataset.")
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
    print("OPTIMAL WEIGHT DISTRIBUTION (training set only)")
    print("="*70)
    print("optimal_weight")
    weight_distribution = training_df['optimal_weight'].value_counts()
    print(weight_distribution.sort_index())
    
    # Check for diversity in optimal weights
    print("\n" + "="*70)
    print("DIVERSITY CHECK")
    print("="*70)
    
    # Calculate diversity metrics
    unique_weights = len(weight_distribution)
    most_common_weight = weight_distribution.iloc[0]
    most_common_percentage = (most_common_weight / len(training_df)) * 100
    
    print(f"Unique weight values: {unique_weights}")
    print(f"Most common weight frequency: {most_common_percentage:.1f}%")
    
    # Warning if distribution is too skewed
    if most_common_percentage > 80:
        print("\n⚠️  WARNING: Weight distribution is highly skewed!")
        print(f"   {most_common_percentage:.1f}% of queries have the same optimal weight.")
        print("   This may result in a model that cannot learn meaningful patterns.")
        print("\n   Consider:")
        print("   1. Increasing sample size to capture more diversity")
        print("   2. Using stratified sampling")
        print("   3. Trying different normalization/combination methods")
        print("   4. Using a fixed weight for this dataset")
    elif most_common_percentage > 60:
        print("\n⚠️  CAUTION: Weight distribution is moderately skewed.")
        print(f"   {most_common_percentage:.1f}% of queries have the same optimal weight.")
        print("   Model performance may be limited.")
    else:
        print("\n✓ Weight distribution shows good diversity for training.")
    
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
    
    print(feature_importance.to_string(index=False, float_format=lambda x: f'{x:12.6f}'))
    
    # Save model and metadata
    model_data = {
        'model': model,
        'scaler': scaler,
        'feature_columns': feature_columns,
        'normalization': args.normalization,
        'combination': args.combination,
        'dataset_config': {
            'dataset_path': args.dataset_path,
            'requires_split': args.requires_split,
            'binary_relevance': args.binary_relevance,
            'neural_field': args.neural_field,
            'lexical_fields': args.lexical_fields
        }
    }
    
    model_path = f'dynamic_hybrid/{args.model_name}.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    # Save metadata
    metadata = {
        'dataset': os.path.basename(args.dataset_path),
        'dataset_path': args.dataset_path,
        'train_size': len(training_df),
        'test_size': len(test_queries),
        'best_alpha': float(model.alpha_),
        'r2_score': float(model.score(X_scaled, y)),
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
        }
    }
    
    metadata_path = f'dynamic_hybrid/{args.model_name}_metadata.json'
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Save training data
    training_data_path = f'dynamic_hybrid/{args.model_name}_training_data.csv'
    training_df.to_csv(training_data_path, index=False)
    
    print("\n" + "="*70)
    print("TRAINING COMPLETE")
    print("="*70)
    print(f"✓ Model saved to {model_path}")
    print(f"✓ Metadata saved to {metadata_path}")
    print(f"✓ Training data saved to {training_data_path}")
    print(f"✓ Trained on {len(training_df)} queries")
    print(f"✓ Test set has {len(test_queries)} queries for evaluation")


if __name__ == "__main__":
    main()
