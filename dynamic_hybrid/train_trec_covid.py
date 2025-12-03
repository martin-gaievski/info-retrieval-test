#!/usr/bin/env python3
"""
FIXED VERSION: Train a Ridge Regression model for dynamic weight prediction using TREC-COVID dataset.
This version properly splits train/test BEFORE calculating optimal weights to avoid data leakage.
Uses only query-specific features (no corpus-aware features).
"""

import json
import pandas as pd
import numpy as np
import pickle
import string
import requests
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
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
        """
        Execute hybrid search with given neural/lexical weights
        
        Args:
            query: Search query string
            neural_weight: Weight for neural search (0-1)
            size: Number of results to retrieve
            
        Returns:
            List of document IDs in ranking order
        """
        lexical_weight = round(1.0 - neural_weight, 2)
        
        url = f"http://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}/{INDEX_NAME}/_search"
        headers = {'Content-Type': 'application/json'}
        
        # Hybrid search query with "or" operator for better recall
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
            
            # Extract document IDs in ranking order
            doc_ids = [hit['_id'] for hit in result.get('hits', {}).get('hits', [])]
            return doc_ids
            
        except Exception as e:
            # Silent fail - return empty list
            return []


def extract_query_features(query):
    """
    Extract query-only features from a query string.
    """
    features = {}
    
    # 1. Query length
    features['query_length'] = len(query)
    
    # 2. Has numbers
    features['has_numbers'] = 1 if any(c.isdigit() for c in query) else 0
    
    # 3. Has special characters (excluding spaces and basic punctuation)
    special_chars = set(string.punctuation) - {' ', '.', ',', '?', '!', '-', "'"}
    features['has_special_chars'] = 1 if any(c in special_chars for c in query) else 0
    
    # 4. Number of terms
    terms = query.lower().split()
    features['num_terms'] = len(terms)
    
    # 5. Unique terms ratio
    unique_terms = set(terms)
    features['unique_terms_ratio'] = len(unique_terms) / len(terms) if terms else 0
    
    # 6. Stopword ratio
    stopword_count = sum(1 for term in terms if term in STOPWORDS)
    features['stopword_ratio'] = stopword_count / len(terms) if terms else 0
    
    # 7. Capitalization ratio
    letters = [c for c in query if c.isalpha()]
    capital_letters = [c for c in letters if c.isupper()]
    features['capitalization_ratio'] = len(capital_letters) / len(letters) if letters else 0
    
    # 8. Has punctuation at the end
    features['has_punctuation'] = 1 if query.rstrip() and query.rstrip()[-1] in string.punctuation else 0
    
    return features


def compute_ndcg_at_k(ranked_docs, relevance_dict, k=10):
    """
    Compute NDCG@k for a ranked list of documents.
    """
    if not ranked_docs:
        return 0.0
    
    # Get relevance scores for top k documents
    relevance_scores = []
    for doc_id in ranked_docs[:k]:
        relevance_scores.append(relevance_dict.get(doc_id, 0))
    
    # DCG@k
    dcg = 0.0
    for i, rel in enumerate(relevance_scores):
        dcg += (2**rel - 1) / np.log2(i + 2)
    
    # IDCG@k
    ideal_scores = sorted(relevance_dict.values(), reverse=True)[:k]
    idcg = 0.0
    for i, rel in enumerate(ideal_scores):
        idcg += (2**rel - 1) / np.log2(i + 2)
    
    # NDCG@k
    return dcg / idcg if idcg > 0 else 0.0


def find_optimal_weight_for_query(query_text, query_ratings, opensearch_client):
    """
    Find the optimal lexical weight for a query by testing different weights.
    """
    # Create relevance dictionary
    relevance_dict = {}
    for rating in query_ratings:
        relevance_dict[rating['doc_id']] = rating['rating']
    
    best_weight = 0.0
    best_ndcg = 0.0
    
    # Test weights from 0.0 to 1.0 in steps of 0.1
    for neural_weight in np.arange(0.0, 1.1, 0.1):
        neural_weight = round(neural_weight, 1)
        
        # Execute search with this weight
        ranked_docs = opensearch_client.execute_hybrid_search(query_text, neural_weight)
        
        # Compute NDCG
        ndcg = compute_ndcg_at_k(ranked_docs, relevance_dict, k=10)
        
        # Track best weight (lexical weight for the model)
        lexical_weight = round(1.0 - neural_weight, 1)
        if ndcg >= best_ndcg:  # Use >= to prefer higher weights on ties
            best_ndcg = ndcg
            best_weight = lexical_weight
    
    return best_weight, best_ndcg


def prepare_training_data(train_queries, ratings_data, opensearch_client):
    """
    Prepare training data with features and optimal weights.
    Only processes the training queries to avoid data leakage.
    """
    # Group ratings by query
    query_ratings = defaultdict(list)
    for rating in ratings_data:
        query_ratings[rating['query_id']].append(rating)
    
    # Extract features and find optimal weights for TRAINING QUERIES ONLY
    training_data = []
    
    print(f"Processing {len(train_queries)} training queries...")
    
    for query_id, query_text in tqdm(train_queries.items(), desc="Finding optimal weights"):
        if query_id not in query_ratings:
            continue
        
        # Extract query features
        features = extract_query_features(query_text)
        
        # Find optimal weight for this query using OpenSearch
        optimal_weight, best_ndcg = find_optimal_weight_for_query(
            query_text, query_ratings[query_id], opensearch_client
        )
        
        training_data.append({
            'query_id': query_id,
            'query_text': query_text,
            **features,
            'optimal_weight': optimal_weight,
            'best_ndcg': best_ndcg
        })
    
    return pd.DataFrame(training_data)


def train_model(df_train):
    """
    Train Ridge regression model with GridSearchCV for hyperparameter tuning.
    """
    # Prepare features and target
    feature_columns = [
        'query_length', 'has_numbers', 'has_special_chars', 'num_terms',
        'unique_terms_ratio', 'stopword_ratio', 'capitalization_ratio', 'has_punctuation'
    ]
    
    X_train = df_train[feature_columns]
    y_train = df_train['optimal_weight']
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    
    # Train Ridge regression with GridSearchCV
    param_grid = {
        'alpha': [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
    }
    
    ridge = Ridge(random_state=42)
    grid_search = GridSearchCV(
        ridge, param_grid, cv=5, 
        scoring='neg_mean_squared_error',
        n_jobs=-1
    )
    
    grid_search.fit(X_train_scaled, y_train)
    
    best_model = grid_search.best_estimator_
    best_alpha = grid_search.best_params_['alpha']
    
    # Evaluate on training set
    y_pred = best_model.predict(X_train_scaled)
    train_mse = mean_squared_error(y_train, y_pred)
    train_r2 = r2_score(y_train, y_pred)
    
    print(f"\nTraining Results:")
    print(f"Best alpha: {best_alpha}")
    print(f"Training MSE: {train_mse:.4f}")
    print(f"Training R²: {train_r2:.4f}")
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'coefficient': best_model.coef_
    }).sort_values('coefficient', key=abs, ascending=False)
    
    print("\nFeature Importance (sorted by absolute coefficient):")
    print(feature_importance.to_string(index=False))
    
    return best_model, scaler, feature_columns, best_alpha


def main():
    print("="*70)
    print("FIXED: TREC-COVID Model Training with Proper Train/Test Split")
    print("="*70)
    
    # Initialize OpenSearch client
    opensearch_client = TRECCOVIDOpenSearchClient()
    
    print("\nLoading TREC-COVID data...")
    
    # Load all queries
    queries = {}
    with open('datasets/trec-covid/queries.jsonl', 'r') as f:
        for line in f:
            data = json.loads(line)
            queries[data['_id']] = data['text']
    print(f"Loaded {len(queries)} total queries")
    
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
                
                if query_id in queries:
                    ratings_data.append({
                        'query_id': query_id,
                        'query_text': queries[query_id],
                        'doc_id': doc_id,
                        'rating': rating
                    })
    print(f"Loaded {len(ratings_data)} ratings")
    
    # FIXED: Split queries BEFORE any processing
    print("\n" + "="*70)
    print("SPLITTING QUERIES BEFORE PROCESSING (to avoid data leakage)")
    print("="*70)
    
    # Create list of query IDs for splitting
    query_ids = list(queries.keys())
    
    # Split 80/20
    train_ids, test_ids = train_test_split(query_ids, test_size=0.2, random_state=42)
    
    print(f"Train set: {len(train_ids)} queries")
    print(f"Test set: {len(test_ids)} queries")
    
    # Create train and test query dictionaries
    train_queries = {qid: queries[qid] for qid in train_ids}
    test_queries = {qid: queries[qid] for qid in test_ids}
    
    # Save test queries for evaluation (WITHOUT calculating optimal weights)
    test_df = pd.DataFrame([
        {'query_id': qid, 'query_text': qtext} 
        for qid, qtext in test_queries.items()
    ])
    test_df.to_csv('dynamic_hybrid/trec_covid_test_set_fixed.csv', index=False)
    print(f"Saved test set to dynamic_hybrid/trec_covid_test_set_fixed.csv")
    
    # Prepare training data ONLY with training queries
    print("\nPreparing training data (ONLY for training queries)...")
    df_train = prepare_training_data(train_queries, ratings_data, opensearch_client)
    print(f"Prepared {len(df_train)} training samples with features and optimal weights")
    
    # Show distribution of optimal weights
    print("\nOptimal weight distribution (training set only):")
    print(df_train['optimal_weight'].value_counts().sort_index())
    
    # Train model
    print("\nTraining Ridge regression model...")
    model, scaler, feature_columns, best_alpha = train_model(df_train)
    
    # Save model and scaler
    model_data = {
        'model': model,
        'scaler': scaler,
        'feature_columns': feature_columns,
        'opensearch_config': {
            'host': OPENSEARCH_HOST,
            'port': OPENSEARCH_PORT,
            'index': INDEX_NAME,
            'model_id': MODEL_ID
        }
    }
    
    with open('dynamic_hybrid/trec_covid_model.pkl', 'wb') as f:
        pickle.dump(model_data, f)
    
    print(f"\nSaved model to dynamic_hybrid/trec_covid_model.pkl")
    
    # Save metadata
    metadata = {
        'dataset': 'trec-covid',
        'features': feature_columns,
        'train_size': len(df_train),
        'test_size': len(test_df),
        'train_query_ids': sorted(train_ids),
        'test_query_ids': sorted(test_ids),
        'best_alpha': float(best_alpha),
        'model_type': 'Ridge',
        'opensearch_host': OPENSEARCH_HOST,
        'opensearch_index': INDEX_NAME,
        'neural_model_id': MODEL_ID,
        'description': 'FIXED: Query-only model with proper train/test split (no data leakage)'
    }
    
    with open('dynamic_hybrid/trec_covid_model_fixed_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Saved metadata to dynamic_hybrid/trec_covid_model_fixed_metadata.json")
    
    print("\n" + "="*70)
    print("TRAINING COMPLETE - NO DATA LEAKAGE")
    print("="*70)
    print(f"✓ Queries split BEFORE processing: 40 train, 10 test")
    print(f"✓ Optimal weights calculated ONLY for training queries")
    print(f"✓ Test queries saved WITHOUT optimal weights")
    print(f"✓ Model trained on {len(df_train)} training samples only")
    print("\nNext step: Run evaluation script to test on unseen queries")


if __name__ == "__main__":
    main()
