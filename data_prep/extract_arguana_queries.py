#!/usr/bin/env python3
"""
Extract Arguana queries and format them for OpenSearch Search Relevance Workbench import.
"""

import json
import os
import re
from typing import Dict, List

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

def load_queries(queries_file: str) -> List[Dict]:
    """Load queries from the JSONL file."""
    queries = []
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for line in f:
            query_data = json.loads(line)
            queries.append({
                'id': query_data['_id'],
                'text': query_data['text'],
                'metadata': query_data.get('metadata', {})
            })
    
    return queries

def create_opensearch_import_format(queries: List[Dict]) -> Dict:
    """Create the OpenSearch Search Relevance Workbench import format."""
    
    # Extract and sanitize queries
    query_set_queries = []
    skipped_queries = []
    long_queries_truncated = 0
    
    for query in queries:
        original_text = query['text']
        sanitized_text = sanitize_query_text(original_text)
        
        # Check if sanitization removed too much content
        if len(sanitized_text) < 3:
            skipped_queries.append((query['id'], original_text[:100]))
            continue
        
        # Truncate very long queries to avoid issues (max 5000 chars)
        if len(sanitized_text) > 5000:
            sanitized_text = sanitized_text[:5000] + "..."
            long_queries_truncated += 1
            
        query_set_queries.append({
            "queryText": sanitized_text
        })
    
    # Create the final structure
    opensearch_import = {
        "name": "arguana_queries",
        "description": f"Arguana containing {len(query_set_queries)} argumentative queries from the BEIR benchmark dataset",
        "sampling": "manual",
        "querySetQueries": query_set_queries
    }
    
    if skipped_queries:
        print(f"   Warning: Skipped {len(skipped_queries)} queries due to sanitization")
        for qid, text in skipped_queries[:3]:  # Show first 3
            print(f"     - Query {qid}: {text}...")
    
    if long_queries_truncated > 0:
        print(f"   Info: Truncated {long_queries_truncated} long queries to 5000 characters")
    
    return opensearch_import

def main():
    """Main function to extract and format Arguana queries."""
    
    # File paths
    queries_file = "datasets/arguana/queries.jsonl"
    output_file = "arguana_queries.json"
    
    print("=" * 80)
    print("Arguana Query Extraction")
    print("=" * 80)
    
    # Step 1: Load queries
    print("\n1. Loading queries from queries.jsonl...")
    queries = load_queries(queries_file)
    print(f"   Loaded {len(queries)} queries")
    
    # Step 2: Show sample queries (original)
    print("\n2. Sample original queries (first 2):")
    for i, query in enumerate(queries[:2], 1):
        text = query['text']
        # Show first 200 chars for these long argumentative texts
        if len(text) > 200:
            text = text[:200] + "..."
        print(f"   {i}. ID: {query['id']}")
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
    print("\nSample SANITIZED queries (first 2):")
    for i, query_obj in enumerate(opensearch_data['querySetQueries'][:2], 1):
        query_text = query_obj['queryText']
        if len(query_text) > 200:
            query_text = query_text[:200] + "..."
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
