"""
Feature extraction utilities for query processing.
Provides functions to extract query-only features for dynamic weight prediction.
"""

import string


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


def extract_query_features(query):
    """
    Extract query-only features from a query string.
    
    Args:
        query (str): The query text
        
    Returns:
        dict: Dictionary of feature names to values
    """
    features = {}
    
    # Basic length feature
    features['query_length'] = len(query)
    
    # Check for numbers
    features['has_numbers'] = 1 if any(c.isdigit() for c in query) else 0
    
    # Check for special characters (excluding common punctuation)
    special_chars = set(string.punctuation) - {' ', '.', ',', '?', '!', '-', "'"}
    features['has_special_chars'] = 1 if any(c in special_chars for c in query) else 0
    
    # Term-based features
    terms = query.lower().split()
    features['num_terms'] = len(terms)
    
    # Unique terms ratio
    unique_terms = set(terms)
    features['unique_terms_ratio'] = len(unique_terms) / len(terms) if terms else 0
    
    # Stopword ratio
    stopword_count = sum(1 for term in terms if term in STOPWORDS)
    features['stopword_ratio'] = stopword_count / len(terms) if terms else 0
    
    # Capitalization ratio
    letters = [c for c in query if c.isalpha()]
    capital_letters = [c for c in letters if c.isupper()]
    features['capitalization_ratio'] = len(capital_letters) / len(letters) if letters else 0
    
    # Check for ending punctuation
    features['has_punctuation'] = 1 if query.rstrip() and query.rstrip()[-1] in string.punctuation else 0
    
    return features


def get_feature_columns():
    """
    Get the standard list of feature column names.
    
    Returns:
        list: List of feature column names in consistent order
    """
    return [
        'query_length',
        'has_numbers',
        'has_special_chars',
        'num_terms',
        'unique_terms_ratio',
        'stopword_ratio',
        'capitalization_ratio',
        'has_punctuation'
    ]


def extract_batch_features(queries):
    """
    Extract features for a batch of queries.
    
    Args:
        queries (list or dict): List of query strings or dict of query_id: query_text
        
    Returns:
        list: List of feature dictionaries
    """
    if isinstance(queries, dict):
        queries = queries.values()
    
    return [extract_query_features(query) for query in queries]
