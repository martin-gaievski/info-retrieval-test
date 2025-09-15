
import numpy as np

def dcg_at_10(df, k=10, position=None, **kwargs):
    # Sort by position and take the top k results
    # if position is given, the caller has already sorted the dataframe
    # an additional position argument is used to avoid excessive dataframe copying
    if position is None:
        df = df.sort_values('position')
        position = df["position"]
    if k:
        df = df.head(k)
        position = position[:10]

    # Apply DCG formula - revert to exponential gain (original)
    dcg = np.sum(((2.0 ** df['rating']) - 1) / np.log2(position + 2))
    
    return dcg


def ndcg_at_10(df, reference=None):
    # Calculate DCG@10
    dcg = dcg_at_10(df)
    
    # Use reference judgments, because best ratings may be outside of search results
    assert reference is not None

    # Reset the positional information - otherwise it uses the original positions and the 
    # changed sorting would have no effect
    ideal_top_10 = reference.sort_values("rating", ascending=False)
    position = np.arange(ideal_top_10.shape[0])
    
    # Calculate iDCG (ideal DCG)
    idcg = dcg_at_10(ideal_top_10, position=position, k=None)
    
    # Handle cases where iDCG is 0
    if idcg == 0:
        return 0
    
    # Normalize DCG
    ndcg = dcg / idcg
    
    return ndcg

def precision_at_k(df, k=10, **kwargs):
    # Sort by position and take the top k results
    top_k = df.sort_values('position').head(k)
    
    # Calculate the number of relevant results (assuming relevance > 1 is relevant)
    relevant_count = np.sum(top_k['rating'] > 1)
    
    # Calculate precision
    precision = relevant_count / k
    
    return precision

def ratio_of_ratings(df, k=10, **kwargs):
    top_k = df.sort_values('position').head(k)
    num_of_ratings = top_k[~top_k['rating'].isna()].shape[0]
    num_of_shown_results = top_k.shape[0]
    if num_of_shown_results == 0:
        return 0
    else:
        return num_of_ratings/num_of_shown_results


def calculate_ndcg_at_k(relevance_scores, k=10):
    """
    Calculate NDCG@k for a list of relevance scores.
    
    Args:
        relevance_scores: List of relevance scores in ranking order
        k: Number of top results to consider
        
    Returns:
        NDCG@k score
    """
    if not relevance_scores or k <= 0:
        return 0.0
    
    # Truncate to k items
    scores = relevance_scores[:k]
    
    # Calculate DCG
    dcg = 0.0
    for i, score in enumerate(scores):
        if score > 0:
            dcg += (2**score - 1) / np.log2(i + 2)
    
    # Calculate IDCG (ideal DCG)
    ideal_scores = sorted(relevance_scores, reverse=True)[:k]
    idcg = 0.0
    for i, score in enumerate(ideal_scores):
        if score > 0:
            idcg += (2**score - 1) / np.log2(i + 2)
    
    # Return NDCG
    if idcg == 0:
        return 0.0
    
    return dcg / idcg
