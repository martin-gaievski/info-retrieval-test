#!/usr/bin/env python3
"""
Extract Webis-Touche2020 relevance judgments and format them for OpenSearch Search Relevance Workbench import.
"""

import json
import csv
import os
import re
from typing import Dict, List
from collections import defaultdict

def sanitize_query_text(text: str) -> str:
    """Sanitize query text to remove invalid characters for OpenSearch.
    
    OpenSearch doesn't allow:
    - Quotes (single or double)
    - Backslashes
    - HTML tags
    """
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Replace various quote types with space
    text = text.replace('"', ' ')
    text = text.replace("'", ' ')
    text = text.replace('`', ' ')
    text = text.replace('"', ' ')  # Left double quotation mark
    text = text.replace('"', ' ')  # Right double quotation mark
    text = text.replace(''', ' ')  # Left single quotation mark
    text = text.replace(''', ' ')  # Right single quotation mark
    
    # Replace backslashes with space
    text = text.replace('\\', ' ')
    
    # Replace forward slashes that might be problematic
    text = text.replace('/', ' ')
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    return text

def load_queries(queries_file: str) -> Dict[str, str]:
    """Load queries from the JSONL file."""
    queries = {}
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            query_data = json.loads(line)
            queries[query_data['_id']] = query_data['text']
    
    return queries

def load_qrels(qrels_file: str) -> Dict[str, List[Dict[str, str]]]:
    """Load relevance judgments from the qrels TSV file.
    
    Returns a dictionary mapping query_id to a list of {docId, rating} dicts.
    """
    qrels = defaultdict(list)
    
    with open(qrels_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip header
        
        for row in reader:
            if len(row) >= 3:
                query_id = row[0]
                doc_id = row[1]
                score = int(row[2])
                
                # Normalize score from 0-2 range to 0-1 range
                # 0 = irrelevant (0.0)
                # 1 = relevant (0.33)
                # 2 = highly relevant (1.0)
                if score == 0:
                    normalized_score = 0.0
                elif score == 1:
                    normalized_score = 0.33
                else:  # score == 2
                    normalized_score = 1.0
                
                qrels[query_id].append({
                    "docId": doc_id,
                    "rating": str(normalized_score)
                })
    
    return dict(qrels)

def create_opensearch_judgments_format(qrels: Dict, all_queries: Dict) -> Dict:
    """Create the OpenSearch Search Relevance Workbench judgment import format."""
    
    judgment_ratings = []
    skipped_queries = []
    queries_with_judgments = set()
    
    # Process each query that has relevance judgments
    for query_id in sorted(qrels.keys()):
        if query_id not in all_queries:
            skipped_queries.append(query_id)
            continue
        
        # Get and sanitize the query text
        original_text = all_queries[query_id]
        sanitized_text = sanitize_query_text(original_text)
        
        # Check if sanitization removed too much content
        if len(sanitized_text) < 3:
            skipped_queries.append(query_id)
            continue
        
        queries_with_judgments.add(query_id)
        
        # Add the judgment rating entry
        judgment_ratings.append({
            "query": sanitized_text,
            "ratings": qrels[query_id]
        })
    
    # Create the final structure
    opensearch_judgments = {
        "name": "Webis-Touche2020 Relevance Judgments",
        "description": f"Webis-Touche 2020 containing relevance judgments for {len(judgment_ratings)} queries from the BEIR benchmark dataset",
        "type": "IMPORT_JUDGMENT",
        "judgmentRatings": judgment_ratings
    }
    
    if skipped_queries:
        print(f"   Warning: Skipped {len(skipped_queries)} queries without text")
        for qid in skipped_queries[:5]:  # Show first 5
            print(f"     - Query ID: {qid}")
    
    return opensearch_judgments, queries_with_judgments

def main():
    """Main function to extract and format Webis-Touche2020 judgments."""
    
    # File paths
    qrels_file = "datasets/webis-touche2020/qrels/test.tsv"
    queries_file = "datasets/webis-touche2020/queries.jsonl"
    output_file = "touche2020_judgments.json"
    
    print("=" * 80)
    print("Webis-Touche2020 Judgment Extraction")
    print("=" * 80)
    
    # Step 1: Load all queries
    print("\n1. Loading all queries from queries.jsonl...")
    all_queries = load_queries(queries_file)
    print(f"   Loaded {len(all_queries)} total queries")
    
    # Step 2: Load relevance judgments
    print("\n2. Loading relevance judgments from test.tsv...")
    qrels = load_qrels(qrels_file)
    print(f"   Loaded judgments for {len(qrels)} queries")
    
    # Count total judgments
    total_judgments = sum(len(ratings) for ratings in qrels.values())
    print(f"   Total query-document pairs: {total_judgments:,}")
    
    # Step 3: Create OpenSearch import format
    print("\n3. Creating OpenSearch judgment import format...")
    opensearch_data, queries_with_judgments = create_opensearch_judgments_format(qrels, all_queries)
    print(f"   Formatted {len(opensearch_data['judgmentRatings'])} queries with judgments")
    print(f"   Note: Scores normalized from 0-2 to 0-1 range")
    print(f"     - 0 → 0.0 (irrelevant)")
    print(f"     - 1 → 0.33 (relevant)")
    print(f"     - 2 → 1.0 (highly relevant)")
    
    # Step 4: Save to file
    print(f"\n4. Saving to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(opensearch_data, f, indent=2, ensure_ascii=False)
    print(f"   ✓ Successfully saved!")
    
    # Step 5: Display statistics and sample
    print("\n" + "=" * 80)
    print("Extraction Summary")
    print("=" * 80)
    print(f"Total queries with judgments: {len(opensearch_data['judgmentRatings'])}")
    print(f"Total query-document pairs: {total_judgments:,}")
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file):,} bytes")
    
    # Show sample judgments
    print("\nSample judgments (first 2 queries):")
    for i, judgment in enumerate(opensearch_data['judgmentRatings'][:2], 1):
        query_text = judgment['query']
        if len(query_text) > 80:
            query_text = query_text[:80] + "..."
        print(f"\n  Query {i}: {query_text}")
        print(f"  Number of rated documents: {len(judgment['ratings'])}")
        
        # Show first 3 ratings for this query
        for j, rating in enumerate(judgment['ratings'][:3], 1):
            print(f"    Doc {j}: {rating['docId'][:40]}... -> rating: {rating['rating']}")
        
        if len(judgment['ratings']) > 3:
            print(f"    ... and {len(judgment['ratings']) - 3} more documents")
    
    # Show score distribution for first query
    if opensearch_data['judgmentRatings']:
        first_query_ratings = opensearch_data['judgmentRatings'][0]['ratings']
        score_dist = defaultdict(int)
        for rating in first_query_ratings:
            score_dist[rating['rating']] += 1
        
        print("\n  Score distribution for first query:")
        for score in sorted(score_dist.keys()):
            count = score_dist[score]
            print(f"    Score {score}: {count} documents")
    
    print("\n" + "=" * 80)
    print("✓ Extraction complete! The file is ready for OpenSearch import.")
    print("=" * 80)

if __name__ == "__main__":
    main()
