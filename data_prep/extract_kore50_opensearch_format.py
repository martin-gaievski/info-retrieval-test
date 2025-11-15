#!/usr/bin/env python3
"""
Extract KORE50 queries and judgments in OpenSearch import format.
This version is designed to work with existing DBPedia index.
"""

import json
import re
from pathlib import Path
from collections import defaultdict

def parse_kore50_sentence(sentence_text):
    """Parse a KORE50 sentence to extract entity mentions and their targets."""
    mentions = []
    
    # Pattern to find entity mentions: [[surface form|entity]]
    pattern = r'\[\[([^\|\]]+)\|([^\]]+)\]\]'
    
    for match in re.finditer(pattern, sentence_text):
        surface_form = match.group(1)
        entity = match.group(2)
        mentions.append({
            'surface_form': surface_form,
            'entity': entity
        })
    
    return mentions

def extract_kore50_data():
    """Extract KORE50 data from the dataset files."""
    base_path = Path('aida/gerbil_data/datasets/kore50')
    
    if not base_path.exists():
        print(f"KORE50 dataset not found at {base_path}")
        return [], []
    
    # Read all KORE50 files
    queries = []
    judgments_map = defaultdict(list)
    
    # KORE50 has different category files
    categories = ['AIDA', 'MSNBC', 'AQUAINT', 'ACE2004', 'CLUEWEB', 
                  'WIKI', 'REUTERS', 'ECB', 'RSS', 'BROWN']
    
    # Try to find KORE50 files in the dataset directory
    kore_files = list(base_path.glob('*.txt'))
    
    if not kore_files:
        # Alternative path structure
        kore_files = list(base_path.glob('*/*.txt'))
    
    print(f"Found {len(kore_files)} KORE50 files")
    
    for file_path in kore_files:
        if file_path.is_file():
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # Each line in KORE50 is a sentence with entity annotations
                for line in content.split('\n'):
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Extract mentions from the sentence
                    mentions = parse_kore50_sentence(line)
                    
                    for mention in mentions:
                        surface_form = mention['surface_form']
                        entity = mention['entity']
                        
                        # Add query (surface form)
                        queries.append(surface_form)
                        
                        # Add judgment - map query to DBPedia entity page
                        # Use the entity name as the document ID (should match DBPedia index)
                        doc_id = f"<dbpedia:{entity}>"
                        judgments_map[surface_form].append({
                            'docId': doc_id,
                            'rating': "1.0"
                        })
    
    # Remove duplicate queries while preserving order
    seen = set()
    unique_queries = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            unique_queries.append(q)
    
    return unique_queries, judgments_map

def save_opensearch_format(queries, judgments_map):
    """Save queries and judgments in OpenSearch import format."""
    
    # Create queries in OpenSearch format
    queries_data = {
        "name": "kore50_entity_linking_queries",
        "description": f"KORE50 entity linking queries - {len(queries)} unique entity mentions from highly ambiguous contexts",
        "querySetQueries": [{"queryText": q} for q in queries]
    }
    
    # Create judgments in OpenSearch format
    judgments_data = {
        "name": "KORE50 Entity Linking Relevance Judgments",
        "description": f"KORE50 entity linking judgments - {len(judgments_map)} queries with ground truth DBPedia entity annotations",
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
    queries_path = Path('results/kore50_opensearch_queries.json')
    queries_path.parent.mkdir(exist_ok=True)
    with open(queries_path, 'w', encoding='utf-8') as f:
        json.dump(queries_data, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(queries)} queries to {queries_path}")
    
    # Save judgments
    judgments_path = Path('results/kore50_opensearch_judgments.json')
    with open(judgments_path, 'w', encoding='utf-8') as f:
        json.dump(judgments_data, f, indent=2, ensure_ascii=False)
    print(f"Saved judgments for {len(judgments_map)} queries to {judgments_path}")
    
    return queries_path, judgments_path

def fallback_to_existing_results():
    """Use existing KORE50 results if dataset files are not found."""
    print("\nFalling back to existing KORE50 results...")
    
    # Load existing queries and judgments
    existing_queries_path = Path('results/kore50_queries.json')
    existing_judgments_path = Path('results/kore50_judgments.json')
    
    if not existing_queries_path.exists() or not existing_judgments_path.exists():
        print("Error: Existing KORE50 results not found")
        return None, None
    
    with open(existing_queries_path, 'r') as f:
        existing_queries = json.load(f)
    
    with open(existing_judgments_path, 'r') as f:
        existing_judgments = json.load(f)
    
    # Build mapping of query_id to query text
    query_id_to_text = {}
    for q in existing_queries:
        query_id_to_text[q['query_id']] = q['query']
    
    # Convert to OpenSearch format
    queries = []
    judgments_map = defaultdict(list)
    
    # Process judgments and link to queries
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

def main():
    print("Extracting KORE50 entity linking data for OpenSearch...")
    
    # Try to extract from original dataset files
    queries, judgments_map = extract_kore50_data()
    
    # If no data found, use existing results
    if not queries:
        queries, judgments_map = fallback_to_existing_results()
        if not queries:
            print("Error: Could not extract KORE50 data")
            return
    
    # Save in OpenSearch format
    queries_path, judgments_path = save_opensearch_format(queries, judgments_map)
    
    # Print statistics
    print(f"\n=== KORE50 Dataset Statistics ===")
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
    print("5. Expected: 10-15% improvement with semantic-heavy weights (0.9/0.1)")
    
    print("\n=== Important Notes ===")
    print("- Document IDs use format: <dbpedia:Entity_Name>")
    print("- Ensure your DBPedia index uses matching document ID format")
    print("- KORE50 focuses on highly ambiguous entity mentions")
    print("- This dataset is perfect for testing entity disambiguation capabilities")

if __name__ == "__main__":
    main()
