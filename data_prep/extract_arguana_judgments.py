#!/usr/bin/env python3
"""
Extract Arguana relevance judgments and format them for OpenSearch Search Relevance Workbench import.
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
                
                # Arguana uses binary relevance
                # 1 = relevant (keep as 1.0)
                # Documents not in the file are implicitly irrelevant (0.0)
                if score == 1:
                    rating = 1.0
                else:
                    # Shouldn't happen based on the dataset description
                    rating = float(score)
                
                qrels[query_id].append({
                    "docId": doc_id,
                    "rating": str(rating)
                })
    
    return dict(qrels)

def create_opensearch_judgments_format(qrels: Dict, all_queries: Dict) -> Dict:
    """Create the OpenSearch Search Relevance Workbench judgment import format."""
    
    judgment_ratings = []
    skipped_queries = []
    queries_with_judgments = set()
    truncated_queries = 0
    
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
        
        # Truncate very long queries to avoid issues (max 5000 chars)
        if len(sanitized_text) > 5000:
            sanitized_text = sanitized_text[:5000] + "..."
            truncated_queries += 1
        
        queries_with_judgments.add(query_id)
        
        # Add the judgment rating entry
        judgment_ratings.append({
            "query": sanitized_text,
            "ratings": qrels[query_id]
        })
    
    # Create the final structure
    opensearch_judgments = {
        "name": "Arguana Relevance Judgments",
        "description": f"Arguana containing relevance judgments for {len(judgment_ratings)} queries from the BEIR benchmark dataset",
        "type": "IMPORT_JUDGMENT",
        "judgmentRatings": judgment_ratings
    }
    
    if skipped_queries:
        print(f"   Warning: Skipped {len(skipped_queries)} queries without text")
        for qid in skipped_queries[:5]:  # Show first 5
            print(f"     - Query ID: {qid}")
    
    if truncated_queries > 0:
        print(f"   Info: Truncated {truncated_queries} long queries to 5000 characters")
    
    return opensearch_judgments, queries_with_judgments

def main():
    """Main function to extract and format Arguana judgments."""
    
    # File paths
    qrels_file = "datasets/arguana/qrels/test.tsv"
    queries_file = "datasets/arguana/queries.jsonl"
    output_file = "arguana_judgments.json"
    
    print("=" * 80)
    print("Arguana Judgment Extraction")
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
    print(f"   Note: Arguana uses binary relevance (1.0 = relevant)")
    print(f"   Documents not in the qrels file are implicitly irrelevant (not included)")
    
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
        if len(query_text) > 150:
            query_text = query_text[:150] + "..."
        print(f"\n  Query {i}: {query_text}")
        print(f"  Number of rated documents: {len(judgment['ratings'])}")
        
        # Show first 3 ratings for this query
        for j, rating in enumerate(judgment['ratings'][:3], 1):
            doc_id = rating['docId']
            if len(doc_id) > 50:
                doc_id = doc_id[:50] + "..."
            print(f"    Doc {j}: {doc_id} -> rating: {rating['rating']}")
        
        if len(judgment['ratings']) > 3:
            print(f"    ... and {len(judgment['ratings']) - 3} more documents")
    
    # Show score distribution for all queries
    all_scores = []
    for judgment in opensearch_data['judgmentRatings']:
        for rating in judgment['ratings']:
            all_scores.append(rating['rating'])
    
    if all_scores:
        score_dist = defaultdict(int)
        for score in all_scores:
            score_dist[score] += 1
        
        print("\n  Overall score distribution:")
        for score in sorted(score_dist.keys()):
            count = score_dist[score]
            print(f"    Score {score}: {count} documents")
    
    # Check query length statistics
    query_lengths = [len(j['query']) for j in opensearch_data['judgmentRatings']]
    if query_lengths:
        avg_length = sum(query_lengths) / len(query_lengths)
        max_length = max(query_lengths)
        min_length = min(query_lengths)
        print(f"\n  Query length statistics:")
        print(f"    Average: {avg_length:.0f} characters")
        print(f"    Min: {min_length} characters")
        print(f"    Max: {max_length} characters")
    
    print("\n" + "=" * 80)
    print("✓ Extraction complete! The file is ready for OpenSearch import.")
    print("=" * 80)

if __name__ == "__main__":
    main()
