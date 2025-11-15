#!/usr/bin/env python3
"""
Extract Quora queries that have labels and format them for OpenSearch Search Relevance Workbench import.
"""

import json
import csv
import os
import re
from typing import Dict, List, Set

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

def load_queries_with_labels(queries_file: str, qrels_file: str) -> Dict[str, str]:
    """Load only queries that have labels in the qrels file."""
    
    # First, get the set of query IDs that have labels
    print("   Loading query IDs from qrels file...")
    labeled_query_ids = set()
    with open(qrels_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip header
        for row in reader:
            if len(row) >= 1:
                labeled_query_ids.add(row[0])
    
    print(f"   Found {len(labeled_query_ids)} unique queries with labels")
    
    # Now load only those queries
    print("   Loading query texts for labeled queries...")
    queries = {}
    total_queries = 0
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            total_queries += 1
            query_data = json.loads(line)
            query_id = query_data['_id']
            
            # Only include queries that have labels
            if query_id in labeled_query_ids:
                queries[query_id] = query_data['text']
    
    print(f"   Loaded {len(queries)} query texts from {total_queries} total queries")
    
    return queries

def create_opensearch_import_format(queries: Dict[str, str]) -> Dict:
    """Create the OpenSearch Search Relevance Workbench import format."""
    
    # Extract and sanitize queries
    query_set_queries = []
    skipped_queries = []
    
    # Sort by query ID for consistent ordering
    for query_id in sorted(queries.keys()):
        original_text = queries[query_id]
        sanitized_text = sanitize_query_text(original_text)
        
        # Check if sanitization removed too much content
        if len(sanitized_text) < 3:
            skipped_queries.append((query_id, original_text[:100]))
            continue
            
        query_set_queries.append({
            "queryText": sanitized_text
        })
    
    # Create the final structure
    opensearch_import = {
        "name": "quora_queries",
        "description": f"Quora containing {len(query_set_queries)} duplicate question detection queries from the BEIR benchmark dataset",
        "sampling": "manual",
        "querySetQueries": query_set_queries
    }
    
    if skipped_queries:
        print(f"   Warning: Skipped {len(skipped_queries)} queries due to sanitization")
        for qid, text in skipped_queries[:3]:  # Show first 3
            print(f"     - Query {qid}: {text}...")
    
    return opensearch_import

def main():
    """Main function to extract and format Quora queries."""
    
    # File paths
    queries_file = "datasets/quora/queries.jsonl"
    qrels_file = "datasets/quora/qrels/test.tsv"
    output_file = "quora_queries.json"
    
    print("=" * 80)
    print("Quora Query Extraction (Only Labeled Queries)")
    print("=" * 80)
    
    # Step 1: Load queries that have labels
    print("\n1. Loading queries that have labels...")
    queries = load_queries_with_labels(queries_file, qrels_file)
    print(f"   Total queries with labels: {len(queries)}")
    
    # Step 2: Show sample queries
    print("\n2. Sample queries (first 3):")
    sample_queries = list(queries.items())[:3]
    for i, (qid, text) in enumerate(sample_queries, 1):
        if len(text) > 100:
            text = text[:100] + "..."
        print(f"   {i}. ID: {qid}")
        print(f"      Text: {text}")
    
    # Step 3: Create OpenSearch import format
    print("\n3. Creating OpenSearch import format with sanitization...")
    opensearch_data = create_opensearch_import_format(queries)
    print(f"   Formatted {len(opensearch_data['querySetQueries'])} queries")
    print(f"   Note: Removed quotes, backslashes, HTML tags, and forward slashes")
    
    # Step 4: Save to file
    print(f"\n4. Saving to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(opensearch_data, f, indent=2, ensure_ascii=False)
    print(f"   ✓ Successfully saved!")
    
    # Step 5: Display statistics and sample
    print("\n" + "=" * 80)
    print("Extraction Summary")
    print("=" * 80)
    print(f"Total queries extracted: {len(opensearch_data['querySetQueries'])}")
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file):,} bytes")
    
    # Show sample sanitized queries
    print("\nSample SANITIZED queries (first 3):")
    for i, query_obj in enumerate(opensearch_data['querySetQueries'][:3], 1):
        query_text = query_obj['queryText']
        if len(query_text) > 100:
            query_text = query_text[:100] + "..."
        print(f"  {i}. {query_text}")
    
    # Check query length distribution
    query_lengths = [len(q['queryText']) for q in opensearch_data['querySetQueries']]
    if query_lengths:
        avg_length = sum(query_lengths) / len(query_lengths)
        max_length = max(query_lengths)
        min_length = min(query_lengths)
        print(f"\nQuery length statistics:")
        print(f"  Average: {avg_length:.0f} characters")
        print(f"  Min: {min_length} characters")
        print(f"  Max: {max_length} characters")
    
    print("\n" + "=" * 80)
    print("✓ Extraction complete! The file is ready for OpenSearch import.")
    print("=" * 80)

if __name__ == "__main__":
    main()
