"""
Data loading utilities for BEIR datasets.
Provides functions to load queries, ratings, and handle train/test splits.
"""

import json
import os
import urllib.request
import zipfile
from sklearn.model_selection import train_test_split
from tqdm import tqdm


def check_dataset_exists(dataset_path):
    """Check if dataset exists and has required files."""
    if not os.path.exists(dataset_path):
        return False
    
    queries_file = os.path.join(dataset_path, 'queries.jsonl')
    qrels_dir = os.path.join(dataset_path, 'qrels')
    
    if not os.path.exists(queries_file):
        return False
    
    if not os.path.exists(qrels_dir):
        return False
    
    tsv_files = [f for f in os.listdir(qrels_dir) if f.endswith('.tsv')]
    if not tsv_files:
        return False
    
    return True


def download_and_extract_dataset(dataset_path, dataset_url=None):
    """Download and extract dataset if it doesn't exist."""
    if dataset_url is None:
        dataset_name = os.path.basename(dataset_path.rstrip('/'))
        dataset_url = f"https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{dataset_name}.zip"
    
    print(f"Dataset not found at {dataset_path}")
    print(f"Downloading from {dataset_url}...")
    
    parent_dir = os.path.dirname(dataset_path)
    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    
    zip_path = dataset_path + '.zip'
    try:
        with tqdm(unit='B', unit_scale=True, desc="Downloading") as t:
            def download_hook(block_num, block_size, total_size):
                if total_size > 0:
                    t.total = total_size
                t.update(block_size)
            
            urllib.request.urlretrieve(dataset_url, zip_path, reporthook=download_hook)
        
        print(f"Downloaded to {zip_path}")
        
        print(f"Extracting dataset...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(parent_dir)
        
        os.remove(zip_path)
        print(f"Dataset extracted to {dataset_path}")
        
        if not os.path.exists(dataset_path):
            raise FileNotFoundError(f"Dataset extraction failed. Expected folder not found: {dataset_path}")
            
    except Exception as e:
        if os.path.exists(zip_path):
            os.remove(zip_path)
        raise Exception(f"Failed to download/extract dataset: {str(e)}")


def load_dataset_with_split(dataset_path, split_ratio=0.8, random_seed=42):
    """Load dataset that has a single query file and requires train/test split."""
    # Load queries
    queries = {}
    queries_file = os.path.join(dataset_path, 'queries.jsonl')
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            queries[data['_id']] = data['text']
    
    print(f"Loaded {len(queries)} total queries")
    
    # Load ratings
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
    os.makedirs('dynamic_hybrid', exist_ok=True)
    test_split_file = os.path.join('dynamic_hybrid', f'{os.path.basename(dataset_path)}_test_split.json')
    with open(test_split_file, 'w') as f:
        json.dump({'test_ids': test_ids}, f, indent=2)
    print(f"Saved test split to {test_split_file}")
    
    return train_queries, test_queries, ratings_data


def load_dataset_with_separate_files(dataset_path):
    """Load dataset that has separate train/test query files."""
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
        raise FileNotFoundError(f"No training data found in {os.path.join(dataset_path, 'qrels')}")
    
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


def load_all_ratings(dataset_path):
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
