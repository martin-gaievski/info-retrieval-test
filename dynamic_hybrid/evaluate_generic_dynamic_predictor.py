#!/usr/bin/env python3
"""
Generic evaluation script for dynamic weight predictors across different datasets.
Supports both datasets with single query file (requiring split) and separate train/test files.
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


def load_test_queries(dataset_path, requires_split, split_ratio=None, seed=42):
    """Load test queries based on dataset type"""
    queries = {}
    queries_file = os.path.join(dataset_path, 'queries.jsonl')
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            queries[data['_id']] = data['text']
    
    if requires_split:
        # Try to load saved test split first
        test_split_file = os.path.join('dynamic_hybrid', f'{os.path.basename(dataset_path)}_test_split.json')
        if os.path.exists(test_split_file):
            with open(test_split_file, 'r') as f:
                test_data = json.load(f)
                test_ids = test_data['test_ids']
        else:
            # Create split on the fly for static-only mode
            if split_ratio is None:
                split_ratio = 0.8  # Default split ratio
            
            print(f"Creating train/test split with ratio {split_ratio}...")
            
            # Get all query IDs that have ratings
            ratings_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
            query_ids_with_ratings = set()
            
            with open(ratings_file, 'r', encoding='utf-8') as f:
                next(f)  # Skip header
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) >= 3:
                        query_ids_with_ratings.add(parts[0])
            
            # Filter queries to only those with ratings
            queries_with_ratings = [qid for qid in queries.keys() if qid in query_ids_with_ratings]
            
            # Create train/test split
            random.seed(seed)
            random.shuffle(queries_with_ratings)
            
            split_idx = int(len(queries_with_ratings) * split_ratio)
            test_ids = queries_with_ratings[split_idx:]
            
            print(f"Split created: {split_idx} train, {len(test_ids)} test queries")
        
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
    
    if os.path.exists(test_file):
        ratings_files.append(test_file)
    if os.path.exists(train_file):
        ratings_files.append(train_file)
    
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
    parser = argparse.ArgumentParser(description='Evaluate generic dynamic weight predictor')
    parser.add_argument('--model-name', type=str, default=None,
                        help='Name of the model files to load (required unless --static-only)')
    parser.add_argument('--dataset-path', type=str, default=None,
                        help='Override dataset path (default: use from model)')
    parser.add_argument('--requires-split', action='store_true',
                        help='Override: dataset requires train/test split (default: use from model)')
    parser.add_argument('--split-ratio', type=float, default=0.8,
                        help='Train/test split ratio for static-only mode (default: 0.8)')
    parser.add_argument('--use-full-dataset', action='store_true',
                        help='Use full dataset instead of test split (only for --static-only mode)')
    parser.add_argument('--sample-size', type=int, default=None,
                        help='Number of queries to sample for evaluation (default: use all)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for sampling (default: 42)')
    parser.add_argument('--static-only', action='store_true',
                        help='Only evaluate static weights without model prediction')
    # Add OpenSearch configuration for static-only mode
    parser.add_argument('--opensearch-host', type=str, default=None,
                        help='OpenSearch host (required for --static-only)')
    parser.add_argument('--opensearch-port', type=int, default=80,
                        help='OpenSearch port (default: 80)')
    parser.add_argument('--index-name', type=str, default=None,
                        help='OpenSearch index name (required for --static-only)')
    parser.add_argument('--model-id', type=str, default=None,
                        help='Neural model ID (required for --static-only)')
    parser.add_argument('--neural-field', type=str, default='passage_embedding',
                        help='Neural field name (default: passage_embedding)')
    parser.add_argument('--lexical-fields', type=str, nargs='+', 
                        default=['title_key^2', 'text_key'],
                        help='Lexical field names with optional boost')
    parser.add_argument('--binary-relevance', action='store_true',
                        help='Use binary relevance (for --static-only)')
    parser.add_argument('--normalization', type=str, nargs='+', default=['l2'],
                        choices=['l2', 'min_max'],
                        help='Normalization technique(s). Single value for normal mode, multiple for --collect-configuration-data')
    parser.add_argument('--combination', type=str, nargs='+', default=['arithmetic_mean'],
                        choices=['arithmetic_mean', 'geometric_mean', 'harmonic_mean'],
                        help='Combination technique(s). Single value for normal mode, multiple for --collect-configuration-data')
    parser.add_argument('--output-file', type=str, default=None,
                        help='Output file name for results (default: {model_name}_evaluation.json or static_evaluation.json)')
    parser.add_argument('--skip-static', action='store_true',
                        help='Skip static weight evaluation when running model evaluation')
    # Data collection parameters (only for static-only mode)
    parser.add_argument('--collect-configuration-data', action='store_true',
                        help='Collect detailed configuration data in CSV format (only with --static-only). Tests all combinations of normalization and combination parameters.')
    parser.add_argument('--csv-output-file', type=str, default=None,
                        help='Output CSV file for configuration data (default: search_configuration_data_{dataset}.csv)')
    args = parser.parse_args()
    
    # Validate arguments
    if not args.static_only and not args.model_name:
        parser.error("--model-name is required unless --static-only is specified")
    
    if args.static_only:
        if not args.dataset_path:
            parser.error("--dataset-path is required when using --static-only")
        if not args.opensearch_host:
            parser.error("--opensearch-host is required when using --static-only")
        if not args.index_name:
            parser.error("--index-name is required when using --static-only")
        if not args.model_id:
            parser.error("--model-id is required when using --static-only")
    
    if args.use_full_dataset and not args.static_only:
        parser.error("--use-full-dataset can only be used with --static-only mode")
    
    if args.skip_static and args.static_only:
        parser.error("--skip-static cannot be used with --static-only mode")
    
    if args.collect_configuration_data and not args.static_only:
        parser.error("--collect-configuration-data can only be used with --static-only mode")
    
    # When not collecting configuration data, only allow single normalization/combination
    if not args.collect_configuration_data:
        if len(args.normalization) > 1:
            parser.error("Multiple normalizations only allowed with --collect-configuration-data")
        if len(args.combination) > 1:
            parser.error("Multiple combinations only allowed with --collect-configuration-data")
        # Extract single values for normal operation
        args.normalization = args.normalization[0]
        args.combination = args.combination[0]
    
    print("="*70)
    if args.static_only:
        print("Static Weight Evaluation (No Model)")
    else:
        print("Generic Dynamic Weight Predictor Evaluation")
    print("="*70)
    
    # Initialize variables
    model = None
    scaler = None
    feature_columns = None
    metadata = {}
    
    if not args.static_only:
        # Load model and metadata
        print(f"\nLoading model: {args.model_name}...")
        model_path = f'dynamic_hybrid/{args.model_name}.pkl'
        metadata_path = f'dynamic_hybrid/{args.model_name}_metadata.json'
        
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        model = model_data['model']
        scaler = model_data['scaler']
        feature_columns = model_data['feature_columns']
        
        # Load dataset configuration from model
        dataset_config = model_data.get('dataset_config', {})
        
        # Use provided dataset path or fall back to model's configuration
        if args.dataset_path:
            dataset_path = args.dataset_path
            # If dataset path is provided, also check if requires_split is specified
            # Otherwise, try to infer from dataset structure
            if args.requires_split:
                requires_split = True
            else:
                # Check if separate train/test files exist
                train_file = os.path.join(dataset_path, 'qrels', 'train.tsv')
                test_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
                requires_split = not (os.path.exists(train_file) and os.path.exists(test_file))
            print(f"Using override dataset path: {dataset_path}")
            print(f"Dataset requires split: {requires_split}")
        else:
            dataset_path = dataset_config.get('dataset_path')
            requires_split = dataset_config.get('requires_split', False)
            if not dataset_path:
                raise ValueError("No dataset path found in model config. Please provide --dataset-path")
        
        binary_relevance = dataset_config.get('binary_relevance', False)
        neural_field = dataset_config.get('neural_field', 'passage_embedding')
        lexical_fields = dataset_config.get('lexical_fields', ['title_key^2', 'text_key'])
        
        # Use normalization and combination from model or override
        # Note: In non-static mode, these are already single values
        if args.normalization == 'l2':
            args.normalization = model_data.get('normalization', 'l2')
        if args.combination == 'arithmetic_mean':
            args.combination = model_data.get('combination', 'arithmetic_mean')
        
        # Load metadata
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        print(f"Dataset: {metadata['dataset']}")
        print(f"Model trained on {metadata['train_size']} queries")
        print(f"Model alpha: {metadata['best_alpha']}")
        print(f"Model R²: {metadata['r2_score']:.4f}")
        
        # Get OpenSearch configuration from model
        opensearch_config = metadata['opensearch_config']
        opensearch_host = opensearch_config['host']
        opensearch_port = opensearch_config['port']
        index_name = opensearch_config['index']
        model_id = opensearch_config['model_id']
        normalization = args.normalization
        combination = args.combination
    else:
        # Static-only mode: use command line arguments
        dataset_path = args.dataset_path
        requires_split = args.requires_split
        if not requires_split:
            # Check if separate train/test files exist
            train_file = os.path.join(dataset_path, 'qrels', 'train.tsv')
            test_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
            requires_split = not (os.path.exists(train_file) and os.path.exists(test_file))
        
        binary_relevance = args.binary_relevance
        neural_field = args.neural_field
        lexical_fields = args.lexical_fields
        opensearch_host = args.opensearch_host
        opensearch_port = args.opensearch_port
        index_name = args.index_name
        model_id = args.model_id
        # For static-only without data collection, use single values
        normalization = args.normalization if not args.collect_configuration_data else args.normalization[0]
        combination = args.combination if not args.collect_configuration_data else args.combination[0]
        
        print(f"Dataset path: {dataset_path}")
        print(f"Binary relevance: {binary_relevance}")
        print(f"Dataset requires split: {requires_split}")
    
    # Initialize OpenSearch client
    opensearch_client = GenericOpenSearchClient(
        host=opensearch_host,
        port=opensearch_port,
        index_name=index_name,
        model_id=model_id,
        neural_field=neural_field,
        lexical_fields=lexical_fields,
        normalization=normalization,
        combination=combination
    )
    
    # Load test queries and ratings
    print("\nLoading test data...")
    if args.static_only and args.use_full_dataset:
        # Use full dataset when requested in static-only mode
        print("Using FULL dataset for static evaluation (no train/test split)...")
        queries_file = os.path.join(dataset_path, 'queries.jsonl')
        test_queries = {}
        with open(queries_file, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                test_queries[data['_id']] = data['text']
        
        # Filter to only queries with ratings
        ratings_file = os.path.join(dataset_path, 'qrels', 'test.tsv')
        query_ids_with_ratings = set()
        with open(ratings_file, 'r', encoding='utf-8') as f:
            next(f)  # Skip header
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    query_ids_with_ratings.add(parts[0])
        
        test_queries = {qid: text for qid, text in test_queries.items() if qid in query_ids_with_ratings}
        print(f"Using ALL {len(test_queries)} queries with ratings")
    elif args.static_only:
        test_queries = load_test_queries(dataset_path, requires_split, args.split_ratio, args.seed)
    else:
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
    
    # 1. Evaluate static weights on test set (unless skipped)
    static_results_by_k = {k: {} for k in k_values}
    static_results = {}
    best_static_weight = None
    best_static_ndcg = None
    
    # New: Track per-query optimal weights and oracle performance
    per_query_optimal_weights = {}
    per_query_ndcg_by_weight = {}
    oracle_ndcg_by_k = {k: 0.0 for k in k_values}
    
    # Data collection mode for detailed configuration testing
    if args.static_only and args.collect_configuration_data:
        print("\n" + "="*70)
        print("COLLECTING SEARCH CONFIGURATION DATA")
        print("="*70)
        
        weight_steps = list(np.arange(0.0, 1.1, 0.1))
        
        # Calculate total configurations
        total_configs = len(args.normalization) * len(args.combination) * len(weight_steps)
        total_tests = len(test_queries) * total_configs
        
        print(f"\nTesting configurations:")
        print(f"- Normalizations: {args.normalization}")
        print(f"- Combinations: {args.combination}")
        print(f"- Weight steps: {len(weight_steps)}")
        print(f"- k values: {k_values}")
        print(f"Total tests: {total_tests} ({len(test_queries)} queries × {total_configs} configs)")
        
        # Collect detailed data
        configuration_results = []
        
        with tqdm(total=total_tests, desc="Testing configurations") as pbar:
            for query_id, query_text in test_queries.items():
                if query_id not in query_ratings:
                    pbar.update(total_configs)
                    continue
                
                # Prepare relevance dict
                if binary_relevance:
                    relevant_docs = set(r['doc_id'] for r in query_ratings[query_id])
                else:
                    relevant_docs = {r['doc_id']: r['rating'] for r in query_ratings[query_id]}
                
                # Test each configuration
                for norm_technique in args.normalization:
                    for comb_technique in args.combination:
                        # Create a new client for this configuration
                        config_client = GenericOpenSearchClient(
                            host=opensearch_host,
                            port=opensearch_port,
                            index_name=index_name,
                            model_id=model_id,
                            neural_field=neural_field,
                            lexical_fields=lexical_fields,
                            normalization=norm_technique,
                            combination=comb_technique
                        )
                        
                        for neural_weight in weight_steps:
                            neural_weight = round(neural_weight, 1)
                            lexical_weight = round(1.0 - neural_weight, 1)
                            
                            # Execute search
                            ranked_docs = config_client.execute_hybrid_search(query_text, neural_weight)
                            
                            # Calculate NDCG for each k
                            result_row = {
                                'query_id': query_id,
                                'query_text': query_text[:100],  # Truncate long queries
                                'normalization': norm_technique,
                                'combination': comb_technique,
                                'neural_weight': neural_weight,
                                'lexical_weight': lexical_weight,
                            }
                            
                            for k in k_values:
                                ndcg = compute_ndcg_at_k(ranked_docs, relevant_docs, k, binary_relevance)
                                result_row[f'ndcg@{k}'] = ndcg
                            
                            configuration_results.append(result_row)
                            pbar.update(1)
        
        # Convert to DataFrame and save CSV
        df = pd.DataFrame(configuration_results)
        
        if args.csv_output_file:
            csv_output_path = args.csv_output_file
        else:
            dataset_name = os.path.basename(dataset_path)
            csv_output_path = f'dynamic_hybrid/search_configuration_data_{dataset_name}.csv'
        
        df.to_csv(csv_output_path, index=False)
        
        print(f"\nConfiguration data saved to {csv_output_path}")
        print(f"Total rows: {len(df)}")
        
        # Display summary statistics
        print("\n" + "="*70)
        print("SUMMARY STATISTICS")
        print("="*70)
        
        for k in k_values:
            print(f"\nNDCG@{k}:")
            print(f"  Mean: {df[f'ndcg@{k}'].mean():.4f}")
            print(f"  Std:  {df[f'ndcg@{k}'].std():.4f}")
            print(f"  Min:  {df[f'ndcg@{k}'].min():.4f}")
            print(f"  Max:  {df[f'ndcg@{k}'].max():.4f}")
        
        # Best configuration for each normalization/combination
        print("\n" + "="*70)
        print("BEST CONFIGURATIONS")
        print("="*70)
        
        for norm in args.normalization:
            for comb in args.combination:
                subset = df[(df['normalization'] == norm) & (df['combination'] == comb)]
                if not subset.empty:
                    avg_by_weight = subset.groupby('lexical_weight')[f'ndcg@{k_values[1]}'].mean()
                    best_weight = avg_by_weight.idxmax()
                    best_ndcg = avg_by_weight.max()
                    print(f"\n{norm} + {comb}:")
                    print(f"  Best lexical weight: {best_weight}")
                    print(f"  Best NDCG@{k_values[1]}: {best_ndcg:.4f}")
        
        # Also run standard evaluation for the current configuration
        # (using the original normalization and combination from command line)
        print("\n" + "="*70)
        print("STANDARD STATIC WEIGHT EVALUATION")
        print("="*70)
        
        # Reset client to original configuration
        opensearch_client = GenericOpenSearchClient(
            host=opensearch_host,
            port=opensearch_port,
            index_name=index_name,
            model_id=model_id,
            neural_field=neural_field,
            lexical_fields=lexical_fields,
            normalization=normalization,
            combination=combination
        )
    
    if not args.skip_static or args.static_only:
        print("\n" + "="*70)
        print("1. EVALUATING STATIC WEIGHTS ON TEST SET")
        print("="*70)
        
        print("\nTesting different weight combinations...")
        
        weight_steps = list(np.arange(0.0, 1.1, 0.1))
        
        # First pass: collect NDCG for each query at each weight
        for neural_weight in tqdm(weight_steps, desc="Testing weights"):
            neural_weight = round(neural_weight, 1)
            lexical_weight = round(1.0 - neural_weight, 1)
            
            # Accumulate NDCG for each k
            total_ndcg_by_k = {k: 0.0 for k in k_values}
            query_count = 0
            
            for query_id, query_text in test_queries.items():
                if query_id not in query_ratings:
                    continue
                
                # Initialize per-query tracking if needed
                if query_id not in per_query_ndcg_by_weight:
                    per_query_ndcg_by_weight[query_id] = {}
                
                # Prepare relevance dict based on type
                if binary_relevance:
                    relevant_docs = set(r['doc_id'] for r in query_ratings[query_id])
                else:
                    relevant_docs = {r['doc_id']: r['rating'] for r in query_ratings[query_id]}
                
                ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
                ndcg_scores = compute_ndcg_at_multiple_k(ranked_docs, relevant_docs, k_values, binary_relevance)
                
                # Store per-query NDCG for this weight
                per_query_ndcg_by_weight[query_id][lexical_weight] = ndcg_scores
                
                for k in k_values:
                    total_ndcg_by_k[k] += ndcg_scores[f'ndcg@{k}']
                query_count += 1
            
            # Store average NDCG for each k
            for k in k_values:
                avg_ndcg = total_ndcg_by_k[k] / query_count if query_count > 0 else 0.0
                static_results_by_k[k][lexical_weight] = avg_ndcg
        
        # Second pass: find optimal weight per query and calculate oracle performance
        for query_id in per_query_ndcg_by_weight:
            query_optimal = {'weights': {}, 'ndcg': {}}
            
            for k in k_values:
                # Find best weight for this query at this k
                best_weight = None
                best_ndcg = 0.0
                
                for weight, ndcg_scores in per_query_ndcg_by_weight[query_id].items():
                    if ndcg_scores[f'ndcg@{k}'] >= best_ndcg:
                        best_ndcg = ndcg_scores[f'ndcg@{k}']
                        best_weight = weight
                
                query_optimal['weights'][k] = best_weight
                query_optimal['ndcg'][k] = best_ndcg
                oracle_ndcg_by_k[k] += best_ndcg
            
            per_query_optimal_weights[query_id] = query_optimal
        
        # Calculate average oracle NDCG
        query_count = len(per_query_optimal_weights)
        for k in k_values:
            oracle_ndcg_by_k[k] = oracle_ndcg_by_k[k] / query_count if query_count > 0 else 0.0
        
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
        
        # NEW: Display optimal weight distribution analysis
        print("\n" + "="*70)
        print("OPTIMAL WEIGHT DISTRIBUTION ANALYSIS")
        print("="*70)
        
        # Analyze distribution of optimal weights for NDCG@10
        optimal_weights_k10 = [q['weights'][10] for q in per_query_optimal_weights.values()]
        weight_counts = pd.Series(optimal_weights_k10).value_counts().sort_index()
        
        print("\nDistribution of Optimal Weights (NDCG@10):")
        print("-" * 40)
        for weight, count in weight_counts.items():
            percentage = (count / len(optimal_weights_k10)) * 100
            print(f"Lexical {weight:.1f}: {count:3d} queries ({percentage:5.1f}%)")
        
        # Calculate statistics
        weights_array = np.array(optimal_weights_k10)
        print(f"\nStatistics:")
        print(f"Mean optimal weight: {weights_array.mean():.2f}")
        print(f"Std deviation: {weights_array.std():.2f}")
        print(f"Median: {np.median(weights_array):.2f}")
        
        # Show oracle performance comparison
        print("\n" + "="*70)
        print("ORACLE PERFORMANCE (Perfect Weight Prediction)")
        print("="*70)
        
        for k in k_values:
            best_static_k = max(static_results_by_k[k], key=static_results_by_k[k].get)
            best_static_ndcg_k = static_results_by_k[k][best_static_k]
            oracle_improvement = ((oracle_ndcg_by_k[k] - best_static_ndcg_k) / best_static_ndcg_k) * 100 if best_static_ndcg_k > 0 else 0
            
            print(f"\n--- NDCG@{k} ---")
            print(f"Best Static Weight: {best_static_k:.1f}")
            print(f"Best Static NDCG@{k}: {best_static_ndcg_k:.4f}")
            print(f"Oracle NDCG@{k}: {oracle_ndcg_by_k[k]:.4f}")
            print(f"Maximum Possible Improvement: {oracle_improvement:+.2f}%")
    else:
        print("\n" + "="*70)
        print("SKIPPING STATIC WEIGHT EVALUATION (--skip-static)")
        print("="*70)
    
    # 2. Evaluate dynamic model on test set (if not static-only)
    query_results = []
    avg_dynamic_ndcg_by_k = {}
    avg_dynamic_ndcg = None
    
    if not args.static_only:
        print("\n" + "="*70)
        print("2. EVALUATING DYNAMIC MODEL ON TEST SET")
        print("="*70)
        
        print("\nEvaluating dynamic weight predictions...")
        total_dynamic_ndcg_by_k = {k: 0.0 for k in k_values}
        
        for query_id, query_text in tqdm(sorted(test_queries.items()), desc="Processing queries"):
            if query_id not in query_ratings:
                continue
            
            # Extract features
            features = extract_query_features(query_text)
            X = pd.DataFrame([features])[feature_columns]
            X_scaled = scaler.transform(X)
            
            # Predict weight
            predicted_weight_raw = model.predict(X_scaled)[0]
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
                'optimal_ndcg': best_ndcg,
                'dynamic_ndcg': ndcg_scores['ndcg@10'],
                'difference': abs(best_weight - predicted_weight)
            })
        
        # Calculate average NDCG for each k
        for k in k_values:
            avg_dynamic_ndcg_by_k[k] = total_dynamic_ndcg_by_k[k] / len(query_results) if query_results else 0.0
        
        # Use NDCG@10 as primary metric
        avg_dynamic_ndcg = avg_dynamic_ndcg_by_k[10]
        
        # Display dynamic model results
        print("\nDynamic Model Results:")
        print("-" * 40)
        for k in k_values:
            print(f"Dynamic Model NDCG@{k}: {avg_dynamic_ndcg_by_k[k]:.4f}")
    
    # 3. Summary
    print("\n" + "="*70)
    print("EVALUATION SUMMARY")
    print("="*70)
    
    if not args.static_only:
        print(f"\nDataset: {metadata['dataset']}")
        print(f"Training Set Size: {metadata['train_size']} queries")
    else:
        print(f"\nDataset: {os.path.basename(dataset_path)}")
        if args.use_full_dataset:
            print("Mode: Static-only (FULL dataset)")
        else:
            print("Mode: Static-only (test split only)")
    print(f"Evaluation Set Size: {len(test_queries)} queries")
    
    if not args.static_only:
        # Show comparison for each k value (if static was evaluated)
        if not args.skip_static:
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
                print(f"Dynamic Model NDCG@{k}: {dynamic_ndcg_k:.4f}")
                print(f"Improvement: {improvement_k:+.2f}%")
            
            # Primary metric (NDCG@10) summary
            print("\n" + "="*70)
            print("PRIMARY METRIC (NDCG@10)")
            print("="*70)
            
            print(f"\nBest Static Weight: Lexical {best_static_weight:.1f}")
            print(f"Best Static NDCG@10: {best_static_ndcg:.4f}")
            print(f"\nDynamic Model NDCG@10: {avg_dynamic_ndcg:.4f}")
            
            improvement = ((avg_dynamic_ndcg - best_static_ndcg) / best_static_ndcg) * 100 if best_static_ndcg > 0 else 0
            print(f"Improvement over best static: {improvement:+.2f}%")
        else:
            # Just show dynamic model results
            print("\n" + "="*70)
            print("DYNAMIC MODEL RESULTS (Static Evaluation Skipped)")
            print("="*70)
            
            for k in k_values:
                print(f"Dynamic Model NDCG@{k}: {avg_dynamic_ndcg_by_k[k]:.4f}")
    else:
        # Static-only summary
        print("\n" + "="*70)
        print("STATIC WEIGHT RESULTS")
        print("="*70)
        
        for k in k_values:
            best_static_k = max(static_results_by_k[k], key=static_results_by_k[k].get)
            best_static_ndcg_k = static_results_by_k[k][best_static_k]
            
            print(f"\n--- NDCG@{k} ---")
            print(f"Best Static Weight: Lexical {best_static_k:.1f}")
            print(f"Best Static NDCG@{k}: {best_static_ndcg_k:.4f}")
    
    # Save results
    if not args.static_only:
        results = {
            'dataset': metadata['dataset'],
            'train_queries': metadata['train_size'],
            'test_queries': len(test_queries),
            'dynamic_ndcg': float(avg_dynamic_ndcg),
            'dynamic_ndcg_by_k': {k: float(v) for k, v in avg_dynamic_ndcg_by_k.items()},
            'per_query_results': query_results[:10],  # Save first 10 for brevity
            'model': {
                'alpha': metadata['best_alpha'],
                'features': feature_columns,
                'r2_score': metadata['r2_score']
            }
        }
        
        # Add static results if they were computed
        if not args.skip_static:
            improvement = ((avg_dynamic_ndcg - best_static_ndcg) / best_static_ndcg) * 100 if best_static_ndcg > 0 else 0
            oracle_improvement = ((oracle_ndcg_by_k[10] - best_static_ndcg) / best_static_ndcg) * 100 if best_static_ndcg > 0 else 0
            
            # Calculate optimal weight distribution stats
            optimal_weights_k10 = [q['weights'][10] for q in per_query_optimal_weights.values()]
            
            results.update({
                'static_results': {f'{k:.1f}': float(v) for k, v in static_results.items()},
                'static_results_by_k': {
                    k: {f'{w:.1f}': float(v) for w, v in static_results_by_k[k].items()}
                    for k in k_values
                },
                'best_static_weight': float(best_static_weight),
                'best_static_ndcg': float(best_static_ndcg),
                'improvement_percent': float(improvement),
                'oracle_performance': {
                    'oracle_ndcg_by_k': {k: float(v) for k, v in oracle_ndcg_by_k.items()},
                    'oracle_improvement_percent': float(oracle_improvement),
                    'optimal_weight_distribution': {float(k): int(v) for k, v in pd.Series(optimal_weights_k10).value_counts().sort_index().items()},
                    'optimal_weight_stats': {
                        'mean': float(np.mean(optimal_weights_k10)),
                        'std': float(np.std(optimal_weights_k10)),
                        'median': float(np.median(optimal_weights_k10))
                    }
                }
            })
        else:
            results['static_evaluation'] = "skipped"
        
        # Use custom output file or default
        if args.output_file:
            evaluation_path = f'dynamic_hybrid/{args.output_file}'
        else:
            evaluation_path = f'dynamic_hybrid/{args.model_name}_evaluation.json'
    else:
        # Static-only results
        # Calculate optimal weight distribution stats for static-only mode
        optimal_weights_k10 = [q['weights'][10] for q in per_query_optimal_weights.values()]
        oracle_improvement = ((oracle_ndcg_by_k[10] - best_static_ndcg) / best_static_ndcg) * 100 if best_static_ndcg > 0 else 0
        
        results = {
            'dataset': os.path.basename(dataset_path),
            'mode': 'static_only',
            'test_queries': len(test_queries),
            'static_results': {f'{k:.1f}': float(v) for k, v in static_results.items()},
            'static_results_by_k': {
                k: {f'{w:.1f}': float(v) for w, v in static_results_by_k[k].items()}
                for k in k_values
            },
            'best_static_weight': float(best_static_weight),
            'best_static_ndcg': float(best_static_ndcg),
            'best_static_by_k': {
                k: {
                    'weight': float(max(static_results_by_k[k], key=static_results_by_k[k].get)),
                    'ndcg': float(max(static_results_by_k[k].values()))
                }
                for k in k_values
            },
            'oracle_performance': {
                'oracle_ndcg_by_k': {k: float(v) for k, v in oracle_ndcg_by_k.items()},
                'oracle_improvement_percent': float(oracle_improvement),
                'optimal_weight_distribution': {float(k): int(v) for k, v in pd.Series(optimal_weights_k10).value_counts().sort_index().items()},
                'optimal_weight_stats': {
                    'mean': float(np.mean(optimal_weights_k10)),
                    'std': float(np.std(optimal_weights_k10)),
                    'median': float(np.median(optimal_weights_k10))
                }
            },
            'opensearch_config': {
                'host': opensearch_host,
                'port': opensearch_port,
                'index': index_name,
                'model_id': model_id,
                'neural_field': neural_field,
                'lexical_fields': lexical_fields,
                'normalization': normalization,
                'combination': combination
            }
        }
        
        # Use custom output file or default
        if args.output_file:
            evaluation_path = f'dynamic_hybrid/{args.output_file}'
        else:
            evaluation_path = f'dynamic_hybrid/static_evaluation_{os.path.basename(dataset_path)}.json'
    
    with open(evaluation_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {evaluation_path}")
    
    # Display prediction distribution (only if not static-only)
    if not args.static_only and query_results:
        print("\n" + "="*70)
        print("PREDICTION ANALYSIS")
        print("="*70)
        
        prediction_df = pd.DataFrame(query_results)
        print("\nPredicted Weight Distribution:")
        print(prediction_df['predicted_weight'].value_counts().sort_index())
        
        print("\nOptimal Weight Distribution:")
        print(prediction_df['optimal_weight'].value_counts().sort_index())
        
        print("\nMean Absolute Error: {:.2f}".format(prediction_df['difference'].mean()))


if __name__ == "__main__":
    main()
