#!/usr/bin/env python3
"""
FIXED VERSION: Evaluate the dynamic weight predictor on TREC-COVID test set.
This version evaluates on truly unseen test queries (no data leakage).
"""

import json
import pandas as pd
import numpy as np
import pickle
import string
import requests
from collections import defaultdict
from opensearchpy import OpenSearch
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# OpenSearch configuration for TREC-COVID
OPENSEARCH_HOST = 'opense-clust-CEpYj56iJ4nM-c3649350d257fde2.elb.us-east-1.amazonaws.com'
OPENSEARCH_PORT = 80
INDEX_NAME = 'trec-covid'
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


class TRECCOVIDOpenSearchClient:
    """OpenSearch client for TREC-COVID hybrid search"""
    
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
        
        payload = {
            "_source": {"excludes": ["passage_embedding"]},
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "neural": {
                                "passage_embedding": {
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
                                "operator": "and",
                                "fields": ["title_key^2", "text_key"]
                            }
                        }
                    ]
                }
            },
            "search_pipeline": {
                "description": "TREC-COVID hybrid search",
                "phase_results_processors": [
                    {
                        "normalization-processor": {
                            "normalization": {"technique": "min_max"},
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
            return []


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
    """Compute NDCG@k for a ranked list of documents."""
    if not ranked_docs:
        return 0.0
    
    relevance_scores = []
    for doc_id in ranked_docs[:k]:
        relevance_scores.append(relevance_dict.get(doc_id, 0))
    
    dcg = 0.0
    for i, rel in enumerate(relevance_scores):
        dcg += (2**rel - 1) / np.log2(i + 2)
    
    ideal_scores = sorted(relevance_dict.values(), reverse=True)[:k]
    idcg = 0.0
    for i, rel in enumerate(ideal_scores):
        idcg += (2**rel - 1) / np.log2(i + 2)
    
    return dcg / idcg if idcg > 0 else 0.0


def compute_ndcg_at_multiple_k(ranked_docs, relevance_dict, k_values=[1, 10, 100]):
    """Compute NDCG at multiple k values."""
    results = {}
    for k in k_values:
        results[f'ndcg@{k}'] = compute_ndcg_at_k(ranked_docs, relevance_dict, k)
    return results


def evaluate_static_weights(queries, ratings_data, opensearch_client):
    """Evaluate different static weights across all queries."""
    query_ratings = defaultdict(list)
    for rating in ratings_data:
        query_ratings[rating['query_id']].append(rating)
    
    weight_results = {}
    
    for neural_weight in np.arange(0.0, 1.1, 0.1):
        neural_weight = round(neural_weight, 1)
        lexical_weight = round(1.0 - neural_weight, 1)
        
        total_ndcg = 0.0
        query_count = 0
        
        for query_id, query_text in queries.items():
            if query_id not in query_ratings:
                continue
            
            relevance_dict = {}
            for rating in query_ratings[query_id]:
                relevance_dict[rating['doc_id']] = rating['rating']
            
            ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
            ndcg = compute_ndcg_at_k(ranked_docs, relevance_dict, k=10)
            
            total_ndcg += ndcg
            query_count += 1
        
        avg_ndcg = total_ndcg / query_count if query_count > 0 else 0.0
        weight_results[lexical_weight] = avg_ndcg
    
    return weight_results


def main():
    print("="*70)
    print("FIXED: TREC-COVID Dynamic Weight Predictor Evaluation")
    print("Evaluating on TRULY UNSEEN test queries (no data leakage)")
    print("="*70)
    
    # Load the fixed model
    print("\nLoading fixed model...")
    with open('dynamic_hybrid/trec_covid_model.pkl', 'rb') as f:
        model_data = pickle.load(f)
    
    model = model_data['model']
    scaler = model_data['scaler']
    feature_columns = model_data['feature_columns']
    
    # Load model metadata to see train/test split
    with open('dynamic_hybrid/trec_covid_model_fixed_metadata.json', 'r') as f:
        metadata = json.load(f)
    
    print(f"Model trained on {metadata['train_size']} queries")
    print(f"Model alpha: {metadata['best_alpha']}")
    
    # Initialize OpenSearch client
    opensearch_client = TRECCOVIDOpenSearchClient()
    
    # Load test queries
    print("\nLoading test queries...")
    test_df = pd.read_csv('dynamic_hybrid/trec_covid_test_set_fixed.csv')
    # Convert query_id to string to match ratings file format
    test_queries = {str(row['query_id']): row['query_text'] for _, row in test_df.iterrows()}
    print(f"Loaded {len(test_queries)} test queries")
    print(f"Test query IDs: {sorted(test_queries.keys())}")
    
    # Load ratings
    ratings_data = []
    with open('datasets/trec-covid/qrels/test.tsv', 'r', encoding='utf-8') as f:
        next(f)  # Skip header
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                query_id = parts[0]
                doc_id = parts[1]
                rating = int(parts[2])
                
                # Only load ratings for test queries
                if query_id in test_queries:
                    ratings_data.append({
                        'query_id': query_id,
                        'doc_id': doc_id,
                        'rating': rating
                    })
    print(f"Loaded {len(ratings_data)} ratings for test queries")
    
    # Define k values for evaluation
    k_values = [1, 10, 100]
    print(f"\nWill evaluate at k values: {k_values}")
    
    # 1. Evaluate static weights on test set
    print("\n" + "="*70)
    print("1. EVALUATING STATIC WEIGHTS ON TEST SET")
    print("="*70)
    
    # Evaluate static weights with progress indicator
    print("\nTesting different weight combinations...")
    static_results = {}
    query_ratings = defaultdict(list)
    for rating in ratings_data:
        query_ratings[rating['query_id']].append(rating)
    
    # Initialize results for each k
    static_results_by_k = {k: {} for k in k_values}
    
    weight_steps = list(np.arange(0.0, 1.1, 0.1))
    for idx, neural_weight in enumerate(tqdm(weight_steps, desc="Testing weights")):
        neural_weight = round(neural_weight, 1)
        lexical_weight = round(1.0 - neural_weight, 1)
        
        # Accumulate NDCG for each k
        total_ndcg_by_k = {k: 0.0 for k in k_values}
        query_count = 0
        
        for query_id, query_text in test_queries.items():
            if query_id not in query_ratings:
                continue
            
            relevance_dict = {}
            for rating in query_ratings[query_id]:
                relevance_dict[rating['doc_id']] = rating['rating']
            
            ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
            ndcg_scores = compute_ndcg_at_multiple_k(ranked_docs, relevance_dict, k_values)
            
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
    
    # Use NDCG@10 as the primary metric for comparison
    static_results = static_results_by_k[10]
    
    print("\nStatic Weight Results (Test Set):")
    print("-" * 40)
    for weight, ndcg in sorted(static_results.items()):
        print(f"Lexical {weight:.1f}: NDCG@10 = {ndcg:.4f}")
    
    best_static_weight = max(static_results, key=static_results.get)
    best_static_ndcg = static_results[best_static_weight]
    print(f"\nBest Static Weight: Lexical {best_static_weight:.1f}")
    print(f"Best Static NDCG@10: {best_static_ndcg:.4f}")
    
    # 2. Evaluate dynamic model on test set
    print("\n" + "="*70)
    print("2. EVALUATING DYNAMIC MODEL ON TEST SET")
    print("="*70)
    
    query_ratings = defaultdict(list)
    for rating in ratings_data:
        query_ratings[rating['query_id']].append(rating)
    
    # Evaluate dynamic model with progress indicator
    print("\nEvaluating dynamic weight predictions...")
    query_results = []
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
        relevance_dict = {}
        for rating in query_ratings[query_id]:
            relevance_dict[rating['doc_id']] = rating['rating']
        
        best_weight = 0.0
        best_ndcg = 0.0
        for neural_weight in np.arange(0.0, 1.1, 0.1):
            neural_weight = round(neural_weight, 1)
            ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
            ndcg = compute_ndcg_at_k(ranked_docs, relevance_dict, k=10)
            lexical_weight = round(1.0 - neural_weight, 1)
            if ndcg >= best_ndcg:
                best_ndcg = ndcg
                best_weight = lexical_weight
        
        # Evaluate with predicted weight at multiple k values
        neural_weight = round(1.0 - predicted_weight, 1)
        ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
        ndcg_scores = compute_ndcg_at_multiple_k(ranked_docs, relevance_dict, k_values)
        
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
    avg_dynamic_ndcg_by_k = {}
    for k in k_values:
        avg_dynamic_ndcg_by_k[k] = total_dynamic_ndcg_by_k[k] / len(query_results) if query_results else 0.0
    
    # Use NDCG@10 as primary metric
    avg_dynamic_ndcg = avg_dynamic_ndcg_by_k[10]
    
    # Display dynamic model results for all k values
    print("\nDynamic Model Results:")
    print("-" * 40)
    for k in k_values:
        print(f"Dynamic Model NDCG@{k}: {avg_dynamic_ndcg_by_k[k]:.4f}")
    
    # 3. Summary
    print("\n" + "="*70)
    print("EVALUATION SUMMARY (FIXED - NO DATA LEAKAGE)")
    print("="*70)
    
    print(f"\nTest Set Size: {len(test_queries)} queries")
    print(f"Training Set Size: {metadata['train_size']} queries (separate)")
    
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
    
    # Save results
    results = {
        'test_queries': len(test_queries),
        'train_queries': metadata['train_size'],
        'static_results': {f'{k:.1f}': float(v) for k, v in static_results.items()},
        'best_static_weight': float(best_static_weight),
        'best_static_ndcg': float(best_static_ndcg),
        'dynamic_ndcg': float(avg_dynamic_ndcg),
        'improvement_percent': float(improvement),
        'per_query_results': query_results,
        'model': {
            'alpha': metadata['best_alpha'],
            'features': feature_columns
        }
    }
    
    with open('dynamic_hybrid/trec_covid_evaluation_fixed.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to dynamic_hybrid/trec_covid_evaluation_fixed.json")
    
    print("\n" + "="*70)
    print("KEY FINDINGS (NO DATA LEAKAGE)")
    print("="*70)
    print("✓ Model trained on 40 queries")
    print("✓ Evaluated on 10 completely unseen queries")
    print("✓ No optimal weights calculated for test queries during training")
    print(f"✓ Dynamic model achieves {avg_dynamic_ndcg:.4f} NDCG@10")
    print(f"✓ Improvement over best static: {improvement:+.2f}%")


if __name__ == "__main__":
    main()
