#!/usr/bin/env python3
"""
O19S Hybrid Search Optimization Validation Implementation

This script replicates the O19S notebook methodology to validate their claims:
- 11.54% NDCG improvement over baseline
- Search result features approach
- Linear Regression and Random Forest models

Based on: https://github.com/o19s/opensearch-hybrid-search-optimization
"""

import sys
from pathlib import Path

# Add paths for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))
sys.path.append(str(current_dir.parent))  # Add parent directory for beir imports

import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Any
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import logging
import time

# Import our existing components
from beir.hybrid.search import RetrievalOpenSearch
from beir.hybrid.evaluation import EvaluateRetrieval
from dynamic_hybrid.feature_extractor_corpus_aware import ESCICorpusAwareFeatureExtractor
from dynamic_hybrid.feature_extractor_o19s_compatible import O19SCompatibleFeatureExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class O19SResultFeatureExtractor:
    """
    Extracts features from search results following O19S methodology.
    This includes both BM25 and neural search result statistics.
    """
    
    def __init__(self, searcher: RetrievalOpenSearch):
        self.searcher = searcher
        self.feature_names = [
            # Query features
            'query_length',
            'query_token_count',
            'query_has_numbers',
            'query_has_special_chars',
            
            # BM25 result features
            'bm25_top1_score',
            'bm25_top5_avg_score',
            'bm25_top10_avg_score',
            'bm25_score_variance',
            'bm25_score_range',
            'bm25_results_count',
            
            # Neural result features
            'neural_top1_score',
            'neural_top5_avg_score',
            'neural_top10_avg_score',
            'neural_score_variance',
            'neural_score_range',
            'neural_results_count',
            
            # Cross-method features
            'score_correlation',
            'result_overlap_top5',
            'result_overlap_top10',
            'avg_score_ratio'
        ]
    
    def extract_query_features(self, query: str) -> Dict[str, float]:
        """Extract basic query-level features."""
        tokens = query.split()
        
        return {
            'query_length': len(query),
            'query_token_count': len(tokens),
            'query_has_numbers': float(any(char.isdigit() for char in query)),
            'query_has_special_chars': float(any(not char.isalnum() and not char.isspace() for char in query))
        }
    
    def extract_result_statistics(self, results: List[Dict], prefix: str) -> Dict[str, float]:
        """Extract statistical features from search results."""
        if not results:
            return {f'{prefix}_{feat}': 0.0 for feat in ['top1_score', 'top5_avg_score', 'top10_avg_score', 
                                                         'score_variance', 'score_range', 'results_count']}
        
        scores = [float(result.get('_score', 0)) for result in results]
        
        features = {
            f'{prefix}_top1_score': scores[0] if scores else 0.0,
            f'{prefix}_top5_avg_score': np.mean(scores[:5]) if len(scores) >= 5 else np.mean(scores),
            f'{prefix}_top10_avg_score': np.mean(scores[:10]) if len(scores) >= 10 else np.mean(scores),
            f'{prefix}_score_variance': np.var(scores) if len(scores) > 1 else 0.0,
            f'{prefix}_score_range': max(scores) - min(scores) if len(scores) > 1 else 0.0,
            f'{prefix}_results_count': len(results)
        }
        
        return features
    
    def calculate_cross_method_features(self, bm25_results: List[Dict], 
                                      neural_results: List[Dict]) -> Dict[str, float]:
        """Calculate features comparing BM25 and neural results."""
        if not bm25_results or not neural_results:
            return {
                'score_correlation': 0.0,
                'result_overlap_top5': 0.0,
                'result_overlap_top10': 0.0,
                'avg_score_ratio': 1.0
            }
        
        # Get document IDs for overlap calculation
        bm25_ids = [result.get('_id', '') for result in bm25_results]
        neural_ids = [result.get('_id', '') for result in neural_results]
        
        # Calculate overlaps
        top5_overlap = len(set(bm25_ids[:5]) & set(neural_ids[:5])) / 5.0
        top10_overlap = len(set(bm25_ids[:10]) & set(neural_ids[:10])) / 10.0
        
        # Score statistics
        bm25_scores = [float(r.get('_score', 0)) for r in bm25_results]
        neural_scores = [float(r.get('_score', 0)) for r in neural_results]
        
        # Score correlation (simplified - just ratio of averages)
        bm25_avg = np.mean(bm25_scores) if bm25_scores else 1.0
        neural_avg = np.mean(neural_scores) if neural_scores else 1.0
        score_ratio = neural_avg / bm25_avg if bm25_avg > 0 else 1.0
        
        return {
            'score_correlation': min(abs(score_ratio), 10.0),  # Cap extreme ratios
            'result_overlap_top5': top5_overlap,
            'result_overlap_top10': top10_overlap,
            'avg_score_ratio': score_ratio
        }
    
    def extract_features(self, query: str, top_k: int = 100) -> Dict[str, float]:
        """
        Extract all features for a query using search results.
        This is the core O19S methodology - features from actual search execution.
        """
        try:
            # Run BM25 search - convert dict results to list format
            bm25_dict = self.searcher.search_bm25_single(query, top_k=top_k)
            bm25_results = [{'_id': doc_id, '_score': score} for doc_id, score in bm25_dict.items()]
            
            # Run neural search - convert dict results to list format
            neural_dict = self.searcher.search_neural_single(query, top_k=top_k)
            neural_results = [{'_id': doc_id, '_score': score} for doc_id, score in neural_dict.items()]
            
            # Extract features
            query_features = self.extract_query_features(query)
            bm25_features = self.extract_result_statistics(bm25_results, 'bm25')
            neural_features = self.extract_result_statistics(neural_results, 'neural')
            cross_features = self.calculate_cross_method_features(bm25_results, neural_results)
            
            # Combine all features
            all_features = {**query_features, **bm25_features, **neural_features, **cross_features}
            
            # Ensure all expected features are present
            for feature_name in self.feature_names:
                if feature_name not in all_features:
                    all_features[feature_name] = 0.0
            
            return all_features
            
        except Exception as e:
            logger.error(f"Error extracting features for query '{query}': {e}")
            # Return zero features on error
            return {feature_name: 0.0 for feature_name in self.feature_names}


class O19SValidationFramework:
    """
    Complete framework for validating O19S claims using their exact methodology.
    Updated to handle 5,000 queries (4,000 train + 1,000 test) as used by O19S.
    """
    
    def __init__(self, searcher: RetrievalOpenSearch, evaluator: EvaluateRetrieval, 
                 data_path: str = "datasets/esci", total_queries: int = 5000, train_ratio: float = 0.8):
        self.searcher = searcher
        self.evaluator = evaluator
        self.data_path = data_path
        self.o19s_extractor = O19SResultFeatureExtractor(searcher)
        # Use searcher's OpenSearch client for corpus-aware features
        self.corpus_extractor = ESCICorpusAwareFeatureExtractor(
            client=searcher.opensearch, 
            index_name=searcher.index_name
        )
        
        # O19S-compatible corpus extractor (search-based instead of termvectors)
        self.corpus_search_extractor = O19SCompatibleFeatureExtractor(
            client=searcher.opensearch,
            index_name=searcher.index_name, 
            model_id=searcher.model_id
        )
        
        # Configurable dataset configuration
        self.train_queries = int(total_queries * train_ratio)
        self.test_queries = total_queries - self.train_queries
        self.o19s_config = {
            'total_queries': total_queries,
            'train_queries': self.train_queries,
            'test_queries': self.test_queries,
            'train_test_ratio': train_ratio / (1 - train_ratio)  # Convert to ratio format
        }
        
        # Models to test (matching O19S)
        self.models = {
            'linear_regression': LinearRegression(),
            'random_forest': RandomForestRegressor(n_estimators=100, random_state=42)
        }
        
        # Batch processing for large dataset
        self.batch_size = 50
        self.feature_cache = {}
        self.results = {}
    
    def load_esci_data(self, target_size: int = None) -> Tuple[List[Dict], Dict]:
        """
        Load ESCI queries and relevance judgments, expanding to target size if needed.
        
        Args:
            target_size: Target number of queries (uses config if not specified)
        """
        if target_size is None:
            target_size = self.o19s_config['total_queries']
            
        logger.info(f"Loading ESCI data (target size: {target_size})")
        
        # Load queries using configurable data path
        queries_file = Path(self.data_path) / "shopping_queries_dataset_examples.parquet"
        if not queries_file.exists():
            raise FileNotFoundError(f"ESCI queries file not found: {queries_file}")
        
        df = pd.read_parquet(queries_file)
        logger.info(f"Raw ESCI data contains {len(df)} rows")
        
        queries = []
        qrels = {}
        
        for _, row in df.iterrows():
            query_id = str(row['query_id'])
            query_text = str(row['query'])
            product_id = str(row['product_id'])
            
            # Convert E/S/C/I to numerical relevance (3/2/1/0)
            relevance_map = {'E': 3, 'S': 2, 'C': 1, 'I': 0}
            relevance = relevance_map.get(row['esci_label'], 0)
            
            queries.append({
                'query_id': query_id,
                'query': query_text
            })
            
            if query_id not in qrels:
                qrels[query_id] = {}
            qrels[query_id][product_id] = relevance
        
        # Remove duplicates
        unique_queries = {}
        for q in queries:
            unique_queries[q['query_id']] = q
        
        unique_queries_list = list(unique_queries.values())
        logger.info(f"Found {len(unique_queries_list)} unique queries")
        
        # Expand dataset if needed to match O19S scale
        if len(unique_queries_list) < target_size:
            logger.info(f"Expanding dataset from {len(unique_queries_list)} to {target_size} queries")
            unique_queries_list = self.expand_dataset(unique_queries_list, qrels, target_size)
        elif len(unique_queries_list) > target_size:
            logger.info(f"Sampling {target_size} queries from {len(unique_queries_list)} available")
            # Use fixed seed for reproducibility
            np.random.seed(42)
            indices = np.random.choice(len(unique_queries_list), target_size, replace=False)
            unique_queries_list = [unique_queries_list[i] for i in indices]
        
        logger.info(f"Final dataset: {len(unique_queries_list)} queries with {len(qrels)} relevance judgments")
        return unique_queries_list, qrels
    
    def expand_dataset(self, queries: List[Dict], qrels: Dict, target_size: int) -> List[Dict]:
        """
        Expand dataset to target size using query variations and sampling strategies.
        """
        logger.info(f"Expanding dataset from {len(queries)} to {target_size} queries")
        
        if len(queries) >= target_size:
            return queries[:target_size]
        
        expanded_queries = queries.copy()
        current_size = len(expanded_queries)
        
        # Strategy 1: Sample with replacement if we have reasonable coverage
        if len(queries) >= target_size * 0.5:  # At least 50% coverage
            logger.info("Using sampling with replacement strategy")
            np.random.seed(42)
            additional_needed = target_size - current_size
            additional_indices = np.random.choice(len(queries), additional_needed, replace=True)
            
            for i, idx in enumerate(additional_indices):
                original_query = queries[idx]
                # Create new query ID to avoid conflicts
                new_query = {
                    'query_id': f"{original_query['query_id']}_dup_{i}",
                    'query': original_query['query']
                }
                expanded_queries.append(new_query)
                
                # Copy relevance judgments
                if original_query['query_id'] in qrels:
                    qrels[new_query['query_id']] = qrels[original_query['query_id']].copy()
        
        else:
            # Strategy 2: Create query variations
            logger.info("Using query variation strategy")
            variations = self.create_query_variations(queries, target_size - current_size)
            expanded_queries.extend(variations)
            
            # Add basic relevance judgments for variations
            for var_query in variations:
                if var_query['query_id'] not in qrels:
                    qrels[var_query['query_id']] = {}
        
        logger.info(f"Dataset expanded to {len(expanded_queries)} queries")
        return expanded_queries[:target_size]
    
    def create_query_variations(self, queries: List[Dict], num_variations: int) -> List[Dict]:
        """Create query variations for dataset expansion."""
        variations = []
        
        # Simple variations: add/remove common words, synonyms, etc.
        common_additions = ['best', 'top', 'good', 'quality', 'cheap', 'affordable']
        
        np.random.seed(42)
        for i in range(num_variations):
            base_query = queries[i % len(queries)]
            
            # Create variation
            if np.random.random() < 0.5:
                # Add word
                addition = np.random.choice(common_additions)
                new_text = f"{addition} {base_query['query']}"
            else:
                # Modify existing query slightly
                words = base_query['query'].split()
                if len(words) > 1:
                    # Remove a word
                    words = words[:-1] if np.random.random() < 0.5 else words[1:]
                    new_text = ' '.join(words)
                else:
                    new_text = base_query['query']
            
            variation = {
                'query_id': f"{base_query['query_id']}_var_{i}",
                'query': new_text
            }
            variations.append(variation)
        
        return variations
    
    def create_train_test_split(self, queries: List[Dict], qrels: Dict) -> Tuple[List[Dict], List[Dict], Dict, Dict]:
        """
        Create configurable train/test split using the configured parameters.
        """
        total_queries = self.o19s_config['total_queries']
        train_queries_count = self.o19s_config['train_queries']
        test_queries_count = self.o19s_config['test_queries']
        
        logger.info(f"Creating train/test split ({train_queries_count}/{test_queries_count})")
        
        if len(queries) != total_queries:
            logger.warning(f"Expected {total_queries} queries, got {len(queries)}")
        
        # Shuffle with fixed seed for reproducibility
        np.random.seed(42)
        shuffled_indices = np.random.permutation(len(queries))
        
        # Split indices
        train_indices = shuffled_indices[:train_queries_count]
        test_indices = shuffled_indices[train_queries_count:train_queries_count + test_queries_count]
        
        # Create splits
        train_queries = [queries[i] for i in train_indices]
        test_queries = [queries[i] for i in test_indices]
        
        # Split qrels
        train_qrels = {q['query_id']: qrels.get(q['query_id'], {}) for q in train_queries}
        test_qrels = {q['query_id']: qrels.get(q['query_id'], {}) for q in test_queries}
        
        logger.info(f"Created train set: {len(train_queries)} queries")
        logger.info(f"Created test set: {len(test_queries)} queries")
        
        return train_queries, test_queries, train_qrels, test_qrels
    
    def create_o19s_train_test_split(self, queries: List[Dict], qrels: Dict) -> Tuple[List[Dict], List[Dict], Dict, Dict]:
        """
        Create exact O19S-style train/test split: 4,000 train + 1,000 test.
        """
        logger.info("Creating O19S-style train/test split (4K/1K)")
        
        if len(queries) != self.o19s_config['total_queries']:
            logger.warning(f"Expected {self.o19s_config['total_queries']} queries, got {len(queries)}")
        
        # Shuffle with fixed seed for reproducibility
        np.random.seed(42)
        shuffled_indices = np.random.permutation(len(queries))
        
        # Split indices
        train_indices = shuffled_indices[:self.o19s_config['train_queries']]
        test_indices = shuffled_indices[self.o19s_config['train_queries']:self.o19s_config['train_queries'] + self.o19s_config['test_queries']]
        
        # Create splits
        train_queries = [queries[i] for i in train_indices]
        test_queries = [queries[i] for i in test_indices]
        
        # Split qrels
        train_qrels = {q['query_id']: qrels.get(q['query_id'], {}) for q in train_queries}
        test_qrels = {q['query_id']: qrels.get(q['query_id'], {}) for q in test_queries}
        
        logger.info(f"Created train set: {len(train_queries)} queries")
        logger.info(f"Created test set: {len(test_queries)} queries")
        
        return train_queries, test_queries, train_qrels, test_qrels
    
    def extract_training_data_batch(self, queries: List[Dict], qrels: Dict, 
                                  approach: str = 'o19s') -> Tuple[np.ndarray, np.ndarray]:
        """
        Extract features and optimal weights for training using batch processing.
        Optimized for large datasets (4,000+ queries).
        
        Args:
            queries: List of query dictionaries
            qrels: Relevance judgments
            approach: 'o19s' for search result features, 'corpus' for corpus-aware features, 
                     'corpus_search' for O19S-compatible search-based features
        """
        logger.info(f"Extracting training data using {approach} approach (batch processing)")
        logger.info(f"Processing {len(queries)} queries in batches of {self.batch_size}")
        
        features_list = []
        optimal_weights = []
        
        # Select appropriate extractor based on approach
        if approach == 'o19s':
            extractor = self.o19s_extractor
        elif approach == 'corpus':
            extractor = self.corpus_extractor
        elif approach == 'corpus_search':
            extractor = self.corpus_search_extractor
        else:
            raise ValueError(f"Unknown approach: {approach}")
        
        # Process in batches for memory efficiency
        for batch_start in range(0, len(queries), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(queries))
            batch_queries = queries[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//self.batch_size + 1}/{(len(queries)-1)//self.batch_size + 1} "
                       f"(queries {batch_start+1}-{batch_end})")
            
            batch_features, batch_weights = self.process_query_batch(
                batch_queries, qrels, extractor, approach
            )
            
            features_list.extend(batch_features)
            optimal_weights.extend(batch_weights)
        
        logger.info(f"Extracted {len(features_list)} training samples from {len(queries)} queries")
        return np.array(features_list), np.array(optimal_weights)
    
    def process_query_batch(self, batch_queries: List[Dict], qrels: Dict, 
                          extractor, approach: str) -> Tuple[List[List[float]], List[float]]:
        """Process a batch of queries for feature extraction."""
        batch_features = []
        batch_weights = []
        
        for query_data in batch_queries:
            query_id = query_data['query_id']
            query_text = query_data['query']
            
            if query_id not in qrels or not qrels[query_id]:
                continue
            
            try:
                # Check cache first
                cache_key = f"{approach}_{query_id}"
                if cache_key in self.feature_cache:
                    features, optimal_weight = self.feature_cache[cache_key]
                else:
                    # Extract features
                    if approach == 'o19s':
                        features = extractor.extract_features(query_text)
                    else:
                        features = extractor.extract_features(query_text)
                    
                    # Find optimal weight using grid search
                    optimal_weight = self.find_optimal_weight(query_text, qrels[query_id])
                    
                    # Cache results
                    self.feature_cache[cache_key] = (features, optimal_weight)
                
                # Convert features to array
                feature_vector = [features.get(name, 0.0) for name in extractor.feature_names]
                
                batch_features.append(feature_vector)
                batch_weights.append(optimal_weight)
                
            except Exception as e:
                logger.error(f"Error processing query {query_id}: {e}")
                continue
        
        return batch_features, batch_weights
    
    def find_optimal_weight(self, query: str, query_qrels: Dict) -> float:
        """Find optimal neural weight for a query using grid search."""
        best_weight = 0.5
        best_ndcg = 0.0
        
        # Grid search over neural weights
        for weight in np.arange(0.0, 1.1, 0.1):
            try:
                # Evaluate hybrid search with this weight
                results = self.searcher.search_hybrid(query, neural_weight=weight, top_k=100)
                
                # Calculate NDCG@10
                ndcg = self.evaluator.calculate_ndcg(results, query_qrels, k=10)
                
                if ndcg > best_ndcg:
                    best_ndcg = ndcg
                    best_weight = weight
                    
            except Exception as e:
                logger.error(f"Error evaluating weight {weight} for query '{query}': {e}")
                continue
        
        return best_weight
    
    def evaluate_o19s_baseline_query(self, query: str, top_k: int = 100) -> Dict[str, float]:
        """
        Execute the exact O19S baseline query (multi_match only, no neural search).
        This replicates their baseline that achieved 0.26 NDCG.
        """
        try:
            # Exact O19S multi_match query structure
            body = {
                "size": top_k,
                "query": {
                    "multi_match": {
                        "type": "best_fields",
                        "fields": [
                            "product_id^100",
                            "product_bullet_point^3", 
                            "product_color^2",
                            "product_brand^5",
                            "product_description",
                            "product_title^10"
                        ],
                        "operator": "and",
                        "query": query
                    }
                }
            }
            
            # Execute the search
            response = self.searcher.opensearch.search(
                index=self.searcher.index_name,
                body=body
            )
            
            # Convert response to standard format
            results = {}
            if 'hits' in response and 'hits' in response['hits']:
                for hit in response['hits']['hits']:
                    doc_id = hit['_id']
                    score = float(hit['_score'])
                    results[doc_id] = score
            
            return results
            
        except Exception as e:
            logger.error(f"Error executing O19S baseline query '{query}': {e}")
            return {}
    
    def train_and_evaluate_models(self, X: np.ndarray, y: np.ndarray, 
                                test_queries: List[Dict], test_qrels: Dict,
                                approach: str = 'o19s') -> Dict[str, Any]:
        """Train models and evaluate performance."""
        logger.info(f"Training and evaluating models for {approach} approach")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        results = {
            'approach': approach,
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'models': {}
        }
        
        for model_name, model in self.models.items():
            logger.info(f"Training {model_name}")
            
            # Train model
            start_time = time.time()
            model.fit(X_train, y_train)
            training_time = time.time() - start_time
            
            # Predict on test set
            y_pred = model.predict(X_test)
            
            # Calculate regression metrics
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            
            # Evaluate on actual search performance
            search_results = self.evaluate_search_performance(
                model, test_queries, test_qrels, approach
            )
            
            results['models'][model_name] = {
                'training_time': training_time,
                'mse': mse,
                'r2_score': r2,
                'search_performance': search_results
            }
            
            logger.info(f"{model_name} - MSE: {mse:.4f}, R²: {r2:.4f}")
        
        return results
    
    def evaluate_search_performance(self, model, test_queries: List[Dict], 
                                  test_qrels: Dict, approach: str) -> Dict[str, float]:
        """Evaluate actual search performance using trained model."""
        logger.info("Evaluating search performance")
        
        # Select appropriate extractor based on approach
        if approach == 'o19s':
            extractor = self.o19s_extractor
        elif approach == 'corpus':
            extractor = self.corpus_extractor
        elif approach == 'corpus_search':
            extractor = self.corpus_search_extractor
        else:
            raise ValueError(f"Unknown approach: {approach}")
        
        ndcg_scores = []
        predicted_weights = []
        
        # Also evaluate fixed weight baselines
        baseline_results = {}
        for fixed_weight in [0.1, 0.3, 0.5, 0.7, 0.9]:
            baseline_ndcgs = []
            
            for query_data in test_queries[:100]:  # Limit for performance
                query_id = query_data['query_id']
                query_text = query_data['query']
                
                if query_id not in test_qrels:
                    continue
                
                try:
                    results = self.searcher.search_hybrid(
                        query_text, neural_weight=fixed_weight, top_k=100
                    )
                    ndcg = self.evaluator.calculate_ndcg(
                        results, test_qrels[query_id], k=10
                    )
                    baseline_ndcgs.append(ndcg)
                except:
                    continue
            
            baseline_results[f'fixed_{fixed_weight}'] = np.mean(baseline_ndcgs) if baseline_ndcgs else 0.0
        
        # Evaluate dynamic predictions
        for query_data in test_queries[:100]:  # Limit for performance
            query_id = query_data['query_id']
            query_text = query_data['query']
            
            if query_id not in test_qrels:
                continue
            
            try:
                # Extract features
                if approach == 'o19s':
                    features = extractor.extract_features(query_text)
                else:
                    features = extractor.extract_features(query_text)
                
                feature_vector = np.array([features.get(name, 0.0) for name in extractor.feature_names]).reshape(1, -1)
                
                # Predict weight
                predicted_weight = model.predict(feature_vector)[0]
                predicted_weight = np.clip(predicted_weight, 0.0, 1.0)  # Ensure valid range
                
                # Evaluate with predicted weight
                results = self.searcher.search_hybrid(
                    query_text, neural_weight=predicted_weight, top_k=100
                )
                ndcg = self.evaluator.calculate_ndcg(results, test_qrels[query_id], k=10)
                
                ndcg_scores.append(ndcg)
                predicted_weights.append(predicted_weight)
                
            except Exception as e:
                logger.error(f"Error evaluating query {query_id}: {e}")
                continue
        
        avg_ndcg = np.mean(ndcg_scores) if ndcg_scores else 0.0
        avg_weight = np.mean(predicted_weights) if predicted_weights else 0.5
        
        # Calculate improvements over baselines
        improvements = {}
        for baseline_name, baseline_ndcg in baseline_results.items():
            if baseline_ndcg > 0:
                improvement = ((avg_ndcg - baseline_ndcg) / baseline_ndcg) * 100
                improvements[f'improvement_over_{baseline_name}'] = improvement
        
        return {
            'avg_ndcg': avg_ndcg,
            'avg_predicted_weight': avg_weight,
            'num_queries_evaluated': len(ndcg_scores),
            **baseline_results,
            **improvements
        }
    
    def run_o19s_scale_validation(self, extraction_method: str = 'all') -> Dict[str, Any]:
        """
        Run complete validation experiment with configurable parameters.
        
        Args:
            extraction_method: Which feature extraction method(s) to use:
                - 'o19s': Original O19S search result-based approach
                - 'corpus': Corpus-aware termvectors-based approach  
                - 'corpus_search': O19S-compatible search-based approach (avoids termvectors)
                - 'all': Run all approaches for comparison
        """
        logger.info(f"Starting validation experiment ({self.o19s_config['total_queries']} queries) with method: {extraction_method}")
        
        # Load queries using configured size
        queries, qrels = self.load_esci_data()
        
        # Create train/test split using configured parameters
        train_queries, test_queries, train_qrels, test_qrels = self.create_train_test_split(queries, qrels)
        
        results = {
            'experiment_config': {
                'total_queries': len(queries),
                'train_queries': len(train_queries),
                'test_queries': len(test_queries),
                'extraction_method': extraction_method,
                'o19s_config': self.o19s_config
            },
            'approaches': {}
        }
        
        # Determine which approaches to run based on extraction_method
        approaches_to_run = []
        if extraction_method == 'all':
            approaches_to_run = ['o19s', 'corpus', 'corpus_search']
        elif extraction_method in ['o19s', 'corpus', 'corpus_search']:
            approaches_to_run = [extraction_method]
        else:
            raise ValueError(f"Invalid extraction method: {extraction_method}. Must be one of: o19s, corpus, corpus_search, all")
        
        logger.info(f"Running approaches: {approaches_to_run}")
        
        # Test selected approaches
        for approach in approaches_to_run:
            logger.info(f"\n=== Testing {approach.upper()} Approach at O19S Scale ===")
            
            try:
                # Extract training data from 4K training queries
                logger.info(f"Extracting training data from {len(train_queries)} training queries")
                X_train, y_train = self.extract_training_data_batch(train_queries, train_qrels, approach)
                
                if len(X_train) == 0:
                    logger.error(f"No training data extracted for {approach} approach")
                    continue
                
                # Train models on 4K queries
                logger.info(f"Training models on {len(X_train)} samples")
                approach_results = self.train_and_evaluate_models_o19s_scale(
                    X_train, y_train, test_queries, test_qrels, approach
                )
                
                results['approaches'][approach] = approach_results
                
            except Exception as e:
                logger.error(f"Error in {approach} approach: {e}")
                results['approaches'][approach] = {'error': str(e)}
        
        return results
    
    def train_and_evaluate_models_o19s_scale(self, X_train: np.ndarray, y_train: np.ndarray, 
                                           test_queries: List[Dict], test_qrels: Dict,
                                           approach: str = 'o19s') -> Dict[str, Any]:
        """
        Train models on 4K samples and evaluate on 1K test set (O19S methodology).
        """
        logger.info(f"Training and evaluating models for {approach} approach at O19S scale")
        logger.info(f"Training samples: {len(X_train)}, Test queries: {len(test_queries)}")
        
        results = {
            'approach': approach,
            'training_samples': len(X_train),
            'test_queries': len(test_queries),
            'models': {}
        }
        
        for model_name, model in self.models.items():
            logger.info(f"Training {model_name} on {len(X_train)} samples")
            
            # Train model
            start_time = time.time()
            model.fit(X_train, y_train)
            training_time = time.time() - start_time
            
            # Evaluate on 1K test set
            search_results = self.evaluate_search_performance_o19s_scale(
                model, test_queries, test_qrels, approach
            )
            
            results['models'][model_name] = {
                'training_time': training_time,
                'training_samples': len(X_train),
                'search_performance': search_results
            }
            
            logger.info(f"{model_name} - Training time: {training_time:.2f}s, "
                       f"Test NDCG: {search_results['avg_ndcg']:.4f}")
        
        return results
    
    def evaluate_search_performance_o19s_scale(self, model, test_queries: List[Dict], 
                                             test_qrels: Dict, approach: str) -> Dict[str, float]:
        """
        Evaluate search performance on 1K test set using trained model.
        """
        logger.info(f"Evaluating search performance on {len(test_queries)} test queries")
        
        # Select appropriate extractor based on approach
        if approach == 'o19s':
            extractor = self.o19s_extractor
        elif approach == 'corpus':
            extractor = self.corpus_extractor
        elif approach == 'corpus_search':
            extractor = self.corpus_search_extractor
        else:
            raise ValueError(f"Unknown approach: {approach}")
        
        ndcg_scores = []
        predicted_weights = []
        
        # Sample subset for baseline evaluation (for performance)
        test_sample = test_queries[:200] if len(test_queries) > 200 else test_queries
        
        # 1. Evaluate O19S Baseline (multi_match only, no neural search)
        logger.info("Evaluating O19S baseline (multi_match only)")
        baseline_ndcgs = []
        for query_data in test_sample:
            query_id = query_data['query_id']
            query_text = query_data['query']
            
            if query_id not in test_qrels or not test_qrels[query_id]:
                continue
            
            try:
                results = self.evaluate_o19s_baseline_query(query_text, top_k=100)
                ndcg = self.evaluator.calculate_ndcg(results, test_qrels[query_id], k=10)
                baseline_ndcgs.append(ndcg)
            except Exception as e:
                logger.warning(f"O19S baseline evaluation failed for query {query_id}: {e}")
                continue
        
        o19s_baseline_ndcg = np.mean(baseline_ndcgs) if baseline_ndcgs else 0.0
        
        # 2. Evaluate hybrid fixed weight baselines (grid search)
        logger.info("Evaluating hybrid fixed weight combinations")
        baseline_results = {'o19s_baseline': o19s_baseline_ndcg}
        
        best_static_ndcg = 0.0
        best_static_weight = 0.5
        
        for fixed_weight in [0.1, 0.3, 0.5, 0.7, 0.9]:
            weight_ndcgs = []
            
            for query_data in test_sample:
                query_id = query_data['query_id']
                query_text = query_data['query']
                
                if query_id not in test_qrels or not test_qrels[query_id]:
                    continue
                
                try:
                    results = self.searcher.search_hybrid(
                        query_text, neural_weight=fixed_weight, top_k=100
                    )
                    ndcg = self.evaluator.calculate_ndcg(
                        results, test_qrels[query_id], k=10
                    )
                    weight_ndcgs.append(ndcg)
                except:
                    continue
            
            avg_ndcg = np.mean(weight_ndcgs) if weight_ndcgs else 0.0
            baseline_results[f'fixed_{fixed_weight}'] = avg_ndcg
            
            # Track best static combination
            if avg_ndcg > best_static_ndcg:
                best_static_ndcg = avg_ndcg
                best_static_weight = fixed_weight
        
        baseline_results['best_static_hybrid'] = best_static_ndcg
        baseline_results['best_static_weight'] = best_static_weight
        
        # Evaluate dynamic predictions on full test set
        for i, query_data in enumerate(test_queries):
            if i % 100 == 0:
                logger.info(f"Evaluating test query {i+1}/{len(test_queries)}")
            
            query_id = query_data['query_id']
            query_text = query_data['query']
            
            if query_id not in test_qrels or not test_qrels[query_id]:
                continue
            
            try:
                # Extract features
                if approach == 'o19s':
                    features = extractor.extract_features(query_text)
                else:
                    features = extractor.extract_features(query_text)
                
                feature_vector = np.array([features.get(name, 0.0) for name in extractor.feature_names]).reshape(1, -1)
                
                # Predict weight
                predicted_weight = model.predict(feature_vector)[0]
                predicted_weight = np.clip(predicted_weight, 0.0, 1.0)
                
                # Evaluate with predicted weight
                results = self.searcher.search_hybrid(
                    query_text, neural_weight=predicted_weight, top_k=100
                )
                ndcg = self.evaluator.calculate_ndcg(results, test_qrels[query_id], k=10)
                
                ndcg_scores.append(ndcg)
                predicted_weights.append(predicted_weight)
                
            except Exception as e:
                logger.error(f"Error evaluating query {query_id}: {e}")
                continue
        
        avg_ndcg = np.mean(ndcg_scores) if ndcg_scores else 0.0
        avg_weight = np.mean(predicted_weights) if predicted_weights else 0.5
        
        # Calculate improvements over baselines
        improvements = {}
        for baseline_name, baseline_ndcg in baseline_results.items():
            if baseline_ndcg > 0:
                improvement = ((avg_ndcg - baseline_ndcg) / baseline_ndcg) * 100
                improvements[f'improvement_over_{baseline_name}'] = improvement
        
        return {
            'avg_ndcg': avg_ndcg,
            'avg_predicted_weight': avg_weight,
            'num_queries_evaluated': len(ndcg_scores),
            **baseline_results,
            **improvements
        }
    
    def save_results(self, results: Dict[str, Any], filename: str = None):
        """Save validation results to JSON file."""
        if filename is None:
            filename = f"o19s_validation_results_{int(time.time())}.json"
        
        filepath = Path("dynamic_hybrid") / filename
        
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Results saved to {filepath}")
        return filepath


def main():
    """Main execution function."""
    logger.info("Starting O19S Validation Implementation")
    
    try:
        # Initialize components
        searcher = HybridSearcher(
            index_name="esci_products",
            host="localhost",
            port=9200
        )
        
        evaluator = HybridEvaluator()
        
        # Create validation framework
        validator = O19SValidationFramework(searcher, evaluator)
        
        # Run validation experiment
        # Start with 250 queries to match O19S paper
        results = validator.run_validation_experiment(query_limit=250)
        
        # Save results
        results_file = validator.save_results(results, "o19s_validation_250_queries.json")
        
        # Print summary
        print("\n" + "="*60)
        print("O19S VALIDATION RESULTS SUMMARY")
        print("="*60)
        
        for approach, approach_results in results['approaches'].items():
            if 'error' in approach_results:
                print(f"\n{approach.upper()} Approach: ERROR - {approach_results['error']}")
                continue
                
            print(f"\n{approach.upper()} Approach:")
            print(f"  Training samples: {approach_results['training_samples']}")
            
            for model_name, model_results in approach_results['models'].items():
                search_perf = model_results['search_performance']
                print(f"\n  {model_name.replace('_', ' ').title()}:")
                print(f"    Average NDCG@10: {search_perf['avg_ndcg']:.4f}")
                print(f"    Average predicted weight: {search_perf['avg_predicted_weight']:.3f}")
                
                # Show improvements
                for key, value in search_perf.items():
                    if key.startswith('improvement_over_'):
                        baseline = key.replace('improvement_over_', '').replace('_', ' ')
                        print(f"    Improvement over {baseline}: {value:+.2f}%")
        
        print(f"\nDetailed results saved to: {results_file}")
        
        # Run extended experiment with 1000 queries for comparison
        logger.info("\nRunning extended validation with 1000 queries...")
        extended_results = validator.run_validation_experiment(query_limit=1000)
        validator.save_results(extended_results, "o19s_validation_1000_queries.json")
        
        print("\nExtended validation (1000 queries) completed!")
        
    except Exception as e:
        logger.error(f"Validation experiment failed: {e}")
        raise


if __name__ == "__main__":
    main()
