#!/usr/bin/env python3
"""
Extract MSNBC queries and judgments in OpenSearch import format.
This version is designed to work with existing DBPedia index.
"""

import json
from pathlib import Path
from collections import defaultdict

def convert_msnbc_to_opensearch():
    """Convert existing MSNBC results to OpenSearch format."""
    
    # Load existing MSNBC queries and judgments
    queries_path = Path('results/msnbc_queries.json')
    judgments_path = Path('results/msnbc_judgments.json')
    
    if not queries_path.exists() or not judgments_path.exists():
        print("Error: MSNBC results not found")
        return None, None
    
    with open(queries_path, 'r', encoding='utf-8') as f:
        existing_queries = json.load(f)
    
    with open(judgments_path, 'r', encoding='utf-8') as f:
        existing_judgments = json.load(f)
    
    # Build mapping of query_id to query text
    query_id_to_text = {}
    for q in existing_queries:
        query_id_to_text[q['query_id']] = q['query']
    
    # Convert to OpenSearch format
    queries = []
    judgments_map = defaultdict(list)
    
    # Process judgments and build unique queries
    for j in existing_judgments:
        query_id = j['query_id']
        
        # Get the query text from our mapping
        if query_id not in query_id_to_text:
            continue
        
        query_text = query_id_to_text[query_id]
        
        # Extract entity name from DBPedia URI
        dbpedia_uri = j['dbpedia_uri']
        entity = dbpedia_uri.replace('http://dbpedia.org/resource/', '')
        
        # Format document ID for OpenSearch
        doc_id = f"<dbpedia:{entity}>"
        
        # Add to judgments map
        judgments_map[query_text].append({
            'docId': doc_id,
            'rating': str(j['relevance'])
        })
    
    # Get unique queries from judgments map
    unique_queries = list(judgments_map.keys())
    
    return unique_queries, judgments_map

def save_opensearch_format(queries, judgments_map):
    """Save queries and judgments in OpenSearch import format."""
    
    # Create queries in OpenSearch format
    queries_data = {
        "name": "msnbc_entity_linking_queries",
        "description": f"MSNBC entity linking queries - {len(queries)} unique entity mentions from news articles",
        "querySetQueries": [{"queryText": q} for q in queries]
    }
    
    # Create judgments in OpenSearch format
    judgments_data = {
        "name": "MSNBC Entity Linking Relevance Judgments",
        "description": f"MSNBC entity linking judgments - {len(judgments_map)} queries with ground truth DBPedia entity annotations",
        "type": "IMPORT_JUDGMENT",
        "judgmentRatings": []
    }
    
    # Add judgments for each query
    for query, ratings in judgments_map.items():
        judgments_data["judgmentRatings"].append({
            "query": query,
            "ratings": ratings
        })
    
    # Save queries
    queries_path = Path('results/msnbc_opensearch_queries.json')
    queries_path.parent.mkdir(exist_ok=True)
    with open(queries_path, 'w', encoding='utf-8') as f:
        json.dump(queries_data, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(queries)} queries to {queries_path}")
    
    # Save judgments
    judgments_path = Path('results/msnbc_opensearch_judgments.json')
    with open(judgments_path, 'w', encoding='utf-8') as f:
        json.dump(judgments_data, f, indent=2, ensure_ascii=False)
    print(f"Saved judgments for {len(judgments_map)} queries to {judgments_path}")
    
    return queries_path, judgments_path

def main():
    print("Converting MSNBC entity linking data to OpenSearch format...")
    
    # Convert existing MSNBC data
    queries, judgments_map = convert_msnbc_to_opensearch()
    
    if not queries:
        print("Error: Could not convert MSNBC data")
        return
    
    # Save in OpenSearch format
    queries_path, judgments_path = save_opensearch_format(queries, judgments_map)
    
    # Print statistics
    print(f"\n=== MSNBC Dataset Statistics ===")
    print(f"Total unique queries: {len(queries)}")
    print(f"Total queries with judgments: {len(judgments_map)}")
    
    # Calculate total judgments
    total_judgments = sum(len(ratings) for ratings in judgments_map.values())
    print(f"Total judgments: {total_judgments}")
    
    # Show example queries
    print(f"\n=== Example Queries ===")
    for i, query in enumerate(queries[:10]):
        print(f"  {i+1}. {query}")
    
    # Show example judgments
    print(f"\n=== Example Judgments ===")
    for i, (query, ratings) in enumerate(list(judgments_map.items())[:5]):
        print(f"  Query: '{query}'")
        for rating in ratings[:3]:  # Show first 3 ratings
            doc_id = rating['docId'].replace('<dbpedia:', '').replace('>', '')
            print(f"    -> {doc_id} (rating: {rating['rating']})")
    
    print(f"\n=== Instructions for Use ===")
    print("1. Ensure your DBPedia index is loaded in OpenSearch")
    print("2. Import queries using:")
    print(f"   {queries_path}")
    print("3. Import judgments using:")
    print(f"   {judgments_path}")
    print("4. Run experiments with various hybrid search weight configurations")
    print("5. Expected: 9% improvement with pure semantic weights (1.0/0.0)")
    
    print("\n=== Important Notes ===")
    print("- Document IDs use format: <dbpedia:Entity_Name>")
    print("- Ensure your DBPedia index uses matching document ID format")
    print("- MSNBC contains news entity mentions")
    print("- This dataset tests entity disambiguation in news context")

if __name__ == "__main__":
    main()
