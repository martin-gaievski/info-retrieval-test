#!/usr/bin/env python3
"""
Extract first 1,000 NQ queries with ultra-aggressive sanitization for OpenSearch.
"""

import json
import os
import re
import string
import unicodedata
from typing import Dict, List

def ultra_sanitize_query_text(text: str) -> str:
    """Ultra-aggressive sanitization - removes ALL non-ASCII characters."""
    
    # First pass: normalize unicode
    text = unicodedata.normalize('NFKD', text)
    
    # Convert to ASCII only, replacing everything else with space
    text = text.encode('ascii', 'ignore').decode('ascii')
    
    # Remove any remaining special characters except basic punctuation
    # Keep only: letters, numbers, spaces, and . , ? ! - : ;
    allowed_chars = set(string.ascii_letters + string.digits + ' .,?!-:;%+')
    text = ''.join(char if char in allowed_chars else ' ' for char in text)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    # Final check: if the text is too short after sanitization, return None
    if len(text) < 5:
        return None
    
    return text

def load_first_n_queries(queries_file: str, n: int = 1000) -> Dict[str, str]:
    """Load first N queries from the JSONL file."""
    
    print(f"   Loading first {n} queries from {queries_file}...")
    queries = {}
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            try:
                query_data = json.loads(line)
                query_id = query_data['_id']
                query_text = query_data['text']
                queries[query_id] = query_text
            except json.JSONDecodeError:
                continue
    
    print(f"   Loaded {len(queries)} queries")
    
    return queries

def create_opensearch_import_format(queries: Dict[str, str]) -> Dict:
    """Create the OpenSearch Search Relevance Workbench import format."""
    
    # Extract and sanitize queries
    query_set_queries = []
    skipped_queries = []
    sanitization_stats = {
        'total': 0,
        'skipped_too_short': 0,
        'contains_non_ascii': 0
    }
    
    # Sort by query ID for consistent ordering
    for query_id in sorted(queries.keys()):
        sanitization_stats['total'] += 1
        original_text = queries[query_id]
        sanitized_text = ultra_sanitize_query_text(original_text)
        
        # Check if sanitization failed (returned None) or removed too much
        if sanitized_text is None:
            skipped_queries.append((query_id, original_text[:50]))
            sanitization_stats['skipped_too_short'] += 1
            continue
        
        # Final ASCII check
        try:
            sanitized_text.encode('ascii')
        except UnicodeEncodeError:
            sanitization_stats['contains_non_ascii'] += 1
            # Force to ASCII
            sanitized_text = sanitized_text.encode('ascii', 'ignore').decode('ascii')
            
        query_set_queries.append({
            "queryText": sanitized_text
        })
    
    # Create the final structure
    opensearch_import = {
        "name": "nq_queries",
        "description": f"Natural Questions containing {len(query_set_queries)} queries from the BEIR benchmark dataset",
        "sampling": "manual",
        "querySetQueries": query_set_queries
    }
    
    print(f"\n   Sanitization statistics:")
    print(f"     - Total queries processed: {sanitization_stats['total']}")
    print(f"     - Skipped (too short after sanitization): {sanitization_stats['skipped_too_short']}")
    print(f"     - Had non-ASCII after first pass: {sanitization_stats['contains_non_ascii']}")
    
    if skipped_queries:
        print(f"\n   Warning: Skipped {len(skipped_queries)} queries due to sanitization")
        for qid, text in skipped_queries[:3]:  # Show first 3
            print(f"     - Query {qid}: {text}...")
    
    return opensearch_import

def validate_output(opensearch_data: Dict) -> None:
    """Validate that the output is pure ASCII."""
    print("\n5. Validating output is pure ASCII...")
    
    issues_found = []
    
    for i, query_obj in enumerate(opensearch_data['querySetQueries']):
        text = query_obj['queryText']
        try:
            text.encode('ascii')
        except UnicodeEncodeError as e:
            issues_found.append((i, str(e), text[:50]))
    
    if issues_found:
        print(f"   ⚠️  WARNING: Found {len(issues_found)} queries with non-ASCII characters")
        for idx, error, text in issues_found[:5]:
            print(f"     - Query {idx}: {text}...")
    else:
        print("   ✓ All queries are pure ASCII")
        
    # Also check for any quotes or special chars
    special_chars = ["'", '"', '`', '\\', '<', '>', '|', '#', '@', '*', '[', ']', '{', '}', '(', ')']
    special_found = []
    for i, query_obj in enumerate(opensearch_data['querySetQueries'][:100]):
        text = query_obj['queryText']
        for char in special_chars:
            if char in text:
                special_found.append((i, char, text[:50]))
                break
    
    if special_found:
        print(f"   ⚠️  Found {len(special_found)} queries with special characters")
        for idx, char, text in special_found[:3]:
            print(f"     - Query {idx} has '{char}': {text}...")
    else:
        print("   ✓ No special characters found")

def main():
    """Main function to extract and format NQ queries."""
    
    # File paths
    queries_file = "datasets/nq/queries.jsonl"
    output_file = "results/nq_queries.json"
    
    print("=" * 80)
    print("NQ (Natural Questions) Query Extraction with ULTRA-AGGRESSIVE Sanitization")
    print("=" * 80)
    
    # Step 1: Load first 1000 queries
    print("\n1. Loading first 1,000 queries...")
    queries = load_first_n_queries(queries_file, 1000)
    print(f"   Total queries loaded: {len(queries)}")
    
    # Step 2: Show sample queries
    print("\n2. Sample ORIGINAL queries (first 3):")
    sample_queries = list(queries.items())[:3]
    for i, (qid, text) in enumerate(sample_queries, 1):
        if len(text) > 100:
            text = text[:100] + "..."
        print(f"   {i}. ID: {qid}")
        print(f"      Text: {text}")
    
    # Step 3: Create OpenSearch import format
    print("\n3. Creating OpenSearch import format with ULTRA sanitization...")
    opensearch_data = create_opensearch_import_format(queries)
    print(f"   Formatted {len(opensearch_data['querySetQueries'])} queries")
    print(f"   Note: ALL non-ASCII characters removed")
    
    # Step 4: Save to file with pure ASCII
    print(f"\n4. Saving to {output_file} (pure ASCII)...")
    os.makedirs("results", exist_ok=True)
    with open(output_file, 'w', encoding='ascii') as f:
        json.dump(opensearch_data, f, indent=2, ensure_ascii=True)
    print(f"   ✓ Successfully saved!")
    
    # Step 5: Validate output
    validate_output(opensearch_data)
    
    # Step 6: Display statistics and sample
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
        print(f"\nQuery length statistics after sanitization:")
        print(f"  Average: {avg_length:.0f} characters")
        print(f"  Min: {min_length} characters")
        print(f"  Max: {max_length} characters")
    
    print("\n" + "=" * 80)
    print("✓ Extraction complete with ULTRA sanitization!")
    print("File should now be 100% compatible with OpenSearch.")
    print("=" * 80)

if __name__ == "__main__":
    main()
