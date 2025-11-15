#!/usr/bin/env python3
"""
Extract Webis-Touche2020 queries and format them for OpenSearch Search Relevance Workbench import.
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
    
    for query in queries:
        original_text = query['text']
        sanitized_text = sanitize_query_text(original_text)
        
        # Check if sanitization removed too much content
        if len(sanitized_text) < 3:
            skipped_queries.append((query['id'], original_text))
            continue
            
        query_set_queries.append({
            "queryText": sanitized_text
        })
    
    # Create the final structure
    opensearch_import = {
        "name": "webis_touche2020_queries",
        "description": f"Webis-Touche 2020 containing {len(query_set_queries)} conversational argument retrieval queries from the BEIR benchmark dataset",
        "sampling": "manual",
        "querySetQueries": query_set_queries
    }
    
    if skipped_queries:
        print(f"   Warning: Skipped {len(skipped_queries)} queries due to sanitization")
        for qid, text in skipped_queries:
            print(f"     - Query {qid}: {text[:50]}...")
    
    return opensearch_import

def main():
    """Main function to extract and format Webis-Touche2020 queries."""
    
    # File paths
    queries_file = "datasets/webis-touche2020/queries.jsonl"
    output_file = "touche2020_queries.json"
    
    print("=" * 80)
    print("Webis-Touche2020 Query Extraction")
    print("=" * 80)
    
    # Step 1: Load queries
    print("\n1. Loading queries from queries.jsonl...")
    queries = load_queries(queries_file)
    print(f"   Loaded {len(queries)} queries")
    
    # Step 2: Show sample queries (original)
    print("\n2. Sample original queries (first 3):")
    for i, query in enumerate(queries[:3], 1):
        text = query['text']
        if len(text) > 80:
            text = text[:80] + "..."
        print(f"   {i}. ID: {query['id']}")
        print(f"      Text: {text}")
        if 'description' in query['metadata']:
            desc = query['metadata']['description']
            if len(desc) > 100:
                desc = desc[:100] + "..."
            print(f"      Description: {desc}")
    
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
    
    # Show comparison of original vs sanitized for first query
    if queries and opensearch_data['querySetQueries']:
        print("\nExample of sanitization (first query):")
        original = queries[0]['text']
        sanitized = opensearch_data['querySetQueries'][0]['queryText']
        print(f"  Original:  {original}")
        print(f"  Sanitized: {sanitized}")
    
    print("\n" + "=" * 80)
    print("✓ Extraction complete! The file is ready for OpenSearch import.")
    print("=" * 80)

if __name__ == "__main__":
    main()
