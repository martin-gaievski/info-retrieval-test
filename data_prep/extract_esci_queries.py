#!/usr/bin/env python3
"""
Extract ESCI Amazon Products queries with ultra-aggressive sanitization for OpenSearch.
ESCI (Shopping Queries Dataset) for e-commerce search relevance.
Supports both small and full datasets with optional query limit.
"""

import json
import os
import re
import string
import unicodedata
import pandas as pd
import argparse
from typing import Dict, List, Set, Optional

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
    if len(text) < 2:  # Allow shorter queries for product searches
        return None
    
    return text

def load_esci_queries(examples_file: str, max_queries: Optional[int] = None) -> Dict[str, str]:
    """Load unique US-locale queries from the ESCI examples parquet file.
    
    Args:
        examples_file: Path to the parquet file
        max_queries: Optional limit on number of valid queries to extract
    """
    
    print(f"   Loading queries from {examples_file}...")
    
    # Read the parquet file
    examples_df = pd.read_parquet(examples_file)
    
    # Filter for US locale only
    print("   Filtering for US locale only...")
    us_examples = examples_df[examples_df['product_locale'] == 'us']
    
    # Get unique queries
    unique_queries = us_examples[['query_id', 'query']].drop_duplicates()
    
    # Sort by query_id to ensure consistent ordering
    unique_queries = unique_queries.sort_values('query_id')
    
    # If max_queries specified, keep processing until we have that many valid queries
    if max_queries and max_queries > 0:
        print(f"   Target: {max_queries} valid queries after sanitization")
        
        queries = {}
        processed = 0
        
        for _, row in unique_queries.iterrows():
            if len(queries) >= max_queries:
                break
                
            query_id = str(row['query_id'])
            query_text = str(row['query'])
            
            # Apply sanitization to check if query will be kept
            sanitized = ultra_sanitize_query_text(query_text)
            
            if sanitized is not None and len(sanitized) >= 2:
                # Query is valid, add it
                queries[query_id] = query_text
            else:
                print(f"     Skipping query {query_id}: too short after sanitization")
            
            processed += 1
            
            # Safety check - if we've processed too many without reaching target
            if processed > max_queries * 10:
                print(f"     Warning: Processed {processed} queries but only found {len(queries)} valid")
                break
    else:
        # No limit - process all US queries
        queries = {}
        for _, row in unique_queries.iterrows():
            query_id = str(row['query_id'])
            query_text = str(row['query'])
            
            # Apply sanitization to check if query will be kept
            sanitized = ultra_sanitize_query_text(query_text)
            
            if sanitized is not None and len(sanitized) >= 2:
                queries[query_id] = query_text
    
    print(f"   Loaded {len(queries)} valid US-locale queries")
    
    return queries

def create_opensearch_import_format(queries: Dict[str, str], name: str) -> tuple[Dict, set]:
    """Create the OpenSearch Search Relevance Workbench import format.
    
    Returns:
        Tuple of (opensearch_data, kept_query_ids) where kept_query_ids are the IDs that were successfully processed
    """
    
    # Extract and sanitize queries
    query_set_queries = []
    kept_query_ids = set()  # Track which queries we kept
    skipped_queries = []
    sanitization_stats = {
        'total': 0,
        'skipped_too_short': 0,
        'contains_non_ascii': 0
    }
    
    # Sort by query ID for consistent ordering
    for query_id in sorted(queries.keys(), key=int):  # Sort numerically
        sanitization_stats['total'] += 1
        original_text = queries[query_id]
        sanitized_text = ultra_sanitize_query_text(original_text)
        
        # Check if sanitization failed (returned None) or removed too much
        if sanitized_text is None or len(sanitized_text) < 2:
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
        kept_query_ids.add(query_id)  # Track this query ID as kept
    
    # Create the final structure
    opensearch_import = {
        "name": name,
        "description": f"ESCI (Shopping Queries Dataset) containing {len(query_set_queries)} e-commerce product search queries",
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
    
    return opensearch_import, kept_query_ids

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
    for i, query_obj in enumerate(opensearch_data['querySetQueries']):
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
    """Main function to extract and format ESCI queries."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Extract ESCI queries for OpenSearch')
    parser.add_argument('--dataset', choices=['small', 'full'], default='full',
                        help='Dataset size to use (default: full)')
    parser.add_argument('--max-queries', type=int, default=None,
                        help='Maximum number of queries to extract (default: all)')
    args = parser.parse_args()
    
    # Set file paths based on dataset choice
    if args.dataset == 'small':
        examples_file = "esci_data/shopping_queries_dataset_examples_us_small.parquet"
        output_file = "results/esci_queries_small.json"
    else:
        examples_file = "esci_data/shopping_queries_dataset_examples.parquet"
        if args.max_queries:
            output_file = f"results/esci_queries_{args.max_queries}.json"
        else:
            output_file = "results/esci_queries_full.json"
    
    print("=" * 80)
    print("ESCI Query Extraction with ULTRA-AGGRESSIVE Sanitization")
    print("=" * 80)
    
    # Check if file exists
    if not os.path.exists(examples_file):
        print(f"\n❌ Error: File not found: {examples_file}")
        print("   Please ensure the ESCI dataset is properly extracted.")
        return
    
    # Step 1: Load queries from parquet file
    print("\n1. Loading ESCI queries from parquet file...")
    queries = load_esci_queries(examples_file, args.max_queries)
    print(f"   Total unique queries: {len(queries)}")
    
    # Step 2: Show dataset statistics
    print("\n2. Dataset statistics:")
    examples_df = pd.read_parquet(examples_file)
    print(f"   Total query-product pairs: {len(examples_df)}")
    print(f"   Unique products in judgments: {examples_df['product_id'].nunique()}")
    
    # Label distribution
    label_counts = examples_df['esci_label'].value_counts()
    print("\n   Label distribution:")
    for label, count in label_counts.items():
        print(f"     - {label}: {count} ({count/len(examples_df)*100:.1f}%)")
    
    print("\n   Label meanings:")
    print("     - E: Exact match (highly relevant)")
    print("     - S: Substitute (somewhat relevant)")
    print("     - C: Complement (slightly relevant)")
    print("     - I: Irrelevant (not relevant)")
    
    # Step 3: Show sample queries
    print("\n3. Sample ORIGINAL queries:")
    sample_queries = list(queries.items())[:5]
    for i, (qid, text) in enumerate(sample_queries, 1):
        if len(text) > 100:
            text = text[:100] + "..."
        print(f"   {i}. ID: {qid}")
        print(f"      Text: {text}")
    
    # Set name based on dataset
    if args.dataset == 'small':
        name = "esci_queries_small"
    elif args.max_queries:
        name = f"esci_queries_{args.max_queries}"
    else:
        name = "esci_queries_full"
    
    # Step 4: Create OpenSearch import format
    print("\n4. Creating OpenSearch import format with ULTRA sanitization...")
    opensearch_data, kept_query_ids = create_opensearch_import_format(queries, name)
    print(f"   Formatted {len(opensearch_data['querySetQueries'])} queries")
    print(f"   Note: ALL non-ASCII characters removed")
    
    # Save the kept query IDs for use by the judgments script
    import pickle
    kept_ids_file = output_file.replace('.json', '_kept_ids.pkl')
    with open(kept_ids_file, 'wb') as f:
        pickle.dump(kept_query_ids, f)
    print(f"   Saved kept query IDs to {kept_ids_file}")
    
    # Step 5: Save to file with pure ASCII
    print(f"\n5. Saving to {output_file} (pure ASCII)...")
    os.makedirs("results", exist_ok=True)
    with open(output_file, 'w', encoding='ascii') as f:
        json.dump(opensearch_data, f, indent=2, ensure_ascii=True)
    print(f"   ✓ Successfully saved!")
    
    # Step 6: Validate output
    validate_output(opensearch_data)
    
    # Step 7: Display statistics and sample
    print("\n" + "=" * 80)
    print("Extraction Summary")
    print("=" * 80)
    print(f"Total queries extracted: {len(opensearch_data['querySetQueries'])}")
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file):,} bytes")
    
    # Show sample sanitized queries
    print("\nSample SANITIZED queries:")
    for i, query_obj in enumerate(opensearch_data['querySetQueries'], 1):
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
