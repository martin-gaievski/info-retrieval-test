#!/usr/bin/env python3
"""
Extract ESCI Amazon Products relevance judgments with ultra-aggressive sanitization for OpenSearch.
Maps ESCI scale (E, S, C, I) to numeric ratings (1.0, 0.7, 0.3, 0.0)
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
from collections import defaultdict
import os

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

def load_esci_data(examples_file: str, kept_query_ids_file: Optional[str] = None) -> pd.DataFrame:
    """Load ESCI examples from parquet file.
    
    Args:
        examples_file: Path to the parquet file
        kept_query_ids_file: Optional path to pickle file with kept query IDs
    """
    
    print(f"   Loading data from {examples_file}...")
    examples_df = pd.read_parquet(examples_file)
    
    # Filter for US locale only
    print("   Filtering for US locale only...")
    examples_df = examples_df[examples_df['product_locale'] == 'us']
    
    if kept_query_ids_file and os.path.exists(kept_query_ids_file):
        # Load the kept query IDs from the queries extraction
        import pickle
        with open(kept_query_ids_file, 'rb') as f:
            kept_query_ids = pickle.load(f)
        
        print(f"   Loaded {len(kept_query_ids)} kept query IDs from {kept_query_ids_file}")
        
        # Convert string IDs to int for matching
        kept_query_ids_int = {int(qid) for qid in kept_query_ids}
        
        # Filter dataframe to only include kept queries
        examples_df = examples_df[examples_df['query_id'].isin(kept_query_ids_int)]
        print(f"   Filtered to {examples_df['query_id'].nunique()} queries that passed sanitization")
    
    print(f"   Loaded {len(examples_df)} query-product pairs")
    print(f"   Unique queries: {examples_df['query_id'].nunique()}")
    
    return examples_df

def create_opensearch_judgments_format(examples_df: pd.DataFrame, name: str) -> Dict:
    """Create the OpenSearch Search Relevance Workbench judgment import format.
    
    Maps ESCI labels to OpenSearch ratings:
    - E (Exact match) → 1.0 (highly relevant)
    - S (Substitute) → 0.7 (somewhat relevant)
    - C (Complement) → 0.3 (slightly relevant)
    - I (Irrelevant) → 0.0 (not relevant)
    """
    
    # Rating mapping from ESCI labels to OpenSearch scale
    rating_map = {
        'E': 1.0,  # Exact match - highly relevant
        'S': 0.7,  # Substitute - somewhat relevant
        'C': 0.3,  # Complement - slightly relevant
        'I': 0.0   # Irrelevant - not relevant
    }
    
    # Group by query to get all judgments for each query
    query_groups = examples_df.groupby(['query_id', 'query'])
    
    judgment_ratings = []
    skipped_queries = []
    sanitization_stats = {
        'total': 0,
        'skipped_too_short': 0,
        'contains_non_ascii': 0
    }
    
    # Process each unique query (should already be filtered by kept_query_ids)
    for (query_id, query_text), group in query_groups:
        sanitization_stats['total'] += 1
        
        # Sanitize the query text
        sanitized_text = ultra_sanitize_query_text(str(query_text))
        
        # This shouldn't happen if we're using kept_query_ids, but check anyway
        if sanitized_text is None or len(sanitized_text) < 2:
            skipped_queries.append((query_id, str(query_text)[:50]))
            sanitization_stats['skipped_too_short'] += 1
            continue
        
        # Final ASCII check
        try:
            sanitized_text.encode('ascii')
        except UnicodeEncodeError:
            sanitization_stats['contains_non_ascii'] += 1
            # Force to ASCII
            sanitized_text = sanitized_text.encode('ascii', 'ignore').decode('ascii')
        
        # Create ratings list for this query
        ratings = []
        for _, row in group.iterrows():
            product_id = str(row['product_id'])
            esci_label = row['esci_label']
            
            # Map the ESCI label to numeric rating
            if esci_label in rating_map:
                rating = rating_map[esci_label]
            else:
                print(f"Warning: Unexpected label {esci_label} for query {query_id}, product {product_id}")
                rating = 0.0  # Default to not relevant
            
            ratings.append({
                "docId": product_id,
                "rating": str(rating)
            })
        
        # Add the judgment rating entry
        judgment_ratings.append({
            "query": sanitized_text,
            "ratings": ratings
        })
    
    # Create the final structure
    opensearch_judgments = {
        "name": name,
        "description": f"ESCI (Shopping Queries Dataset) containing relevance judgments for {len(judgment_ratings)} e-commerce product search queries",
        "type": "IMPORT_JUDGMENT",
        "judgmentRatings": judgment_ratings
    }
    
    print(f"\n   Sanitization statistics:")
    print(f"     - Total queries processed: {sanitization_stats['total']}")
    print(f"     - Skipped (too short after sanitization): {sanitization_stats['skipped_too_short']}")
    print(f"     - Had non-ASCII after first pass: {sanitization_stats['contains_non_ascii']}")
    
    if skipped_queries:
        print(f"\n   Warning: Skipped {len(skipped_queries)} queries")
    
    return opensearch_judgments

def validate_output(opensearch_data: Dict) -> None:
    """Validate that the output is pure ASCII."""
    print("\n5. Validating output is pure ASCII...")
    
    issues_found = []
    
    for i, judgment in enumerate(opensearch_data['judgmentRatings']):
        text = judgment['query']
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
    for i, judgment in enumerate(opensearch_data['judgmentRatings']):
        text = judgment['query']
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
    """Main function to extract and format ESCI judgments."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Extract ESCI judgments for OpenSearch')
    parser.add_argument('--dataset', choices=['small', 'full'], default='full',
                        help='Dataset size to use (default: full)')
    parser.add_argument('--max-queries', type=int, default=None,
                        help='Maximum number of queries to extract judgments for (default: all)')
    args = parser.parse_args()
    
    # Set file paths based on dataset choice
    if args.dataset == 'small':
        examples_file = "esci_data/shopping_queries_dataset_examples_us_small.parquet"
        output_file = "results/esci_judgments_small.json"
        kept_ids_file = None  # Small dataset doesn't use kept IDs file
    else:
        examples_file = "esci_data/shopping_queries_dataset_examples.parquet"
        if args.max_queries:
            output_file = f"results/esci_judgments_{args.max_queries}.json"
            kept_ids_file = f"results/esci_queries_{args.max_queries}_kept_ids.pkl"
        else:
            output_file = "results/esci_judgments_full.json"
            kept_ids_file = "results/esci_queries_full_kept_ids.pkl"
    
    print("=" * 80)
    print("ESCI Judgment Extraction with ULTRA-AGGRESSIVE Sanitization")
    print("=" * 80)
    
    # Check if file exists
    if not os.path.exists(examples_file):
        print(f"\n❌ Error: File not found: {examples_file}")
        print("   Please ensure the ESCI dataset is properly extracted.")
        return
    
    # Step 1: Load data from parquet file
    print("\n1. Loading ESCI data from parquet file...")
    examples_df = load_esci_data(examples_file, kept_ids_file)
    
    # Step 2: Show dataset statistics
    print("\n2. Dataset statistics:")
    print(f"   Total query-product pairs: {len(examples_df)}")
    print(f"   Unique queries: {examples_df['query_id'].nunique()}")
    print(f"   Unique products: {examples_df['product_id'].nunique()}")
    
    # Label distribution
    label_counts = examples_df['esci_label'].value_counts()
    print("\n   Label distribution:")
    for label, count in label_counts.items():
        print(f"     - {label}: {count} ({count/len(examples_df)*100:.1f}%)")
    
    print("\n   Rating mapping:")
    print("     - E (Exact match)  → 1.0 (highly relevant)")
    print("     - S (Substitute)   → 0.7 (somewhat relevant)")
    print("     - C (Complement)   → 0.3 (slightly relevant)")
    print("     - I (Irrelevant)   → 0.0 (not relevant)")
    
    # Set name based on dataset
    if args.dataset == 'small':
        name = "ESCI Relevance Judgments (Small)"
    elif args.max_queries:
        name = f"ESCI Relevance Judgments ({args.max_queries} queries)"
    else:
        name = "ESCI Relevance Judgments (Full)"
    
    # Step 3: Create OpenSearch import format
    print("\n3. Creating OpenSearch judgment import format with ULTRA sanitization...")
    opensearch_data = create_opensearch_judgments_format(examples_df, name)
    print(f"   Formatted {len(opensearch_data['judgmentRatings'])} queries with judgments")
    print(f"   Note: ESCI uses 4-level scale (E/S/C/I)")
    print(f"   Mapped to OpenSearch scale: 1.0, 0.7, 0.3, 0.0")
    print(f"   ALL non-ASCII characters removed")
    
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
    
    # Count total judgments
    total_judgments = sum(len(j['ratings']) for j in opensearch_data['judgmentRatings'])
    print(f"Total queries with judgments: {len(opensearch_data['judgmentRatings'])}")
    print(f"Total query-document pairs: {total_judgments}")
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file):,} bytes")
    
    # Show sample judgments
    print("\nSample judgments (all queries):")
    for i, judgment in enumerate(opensearch_data['judgmentRatings'], 1):
        query_text = judgment['query']
        if len(query_text) > 100:
            query_text = query_text[:100] + "..."
        print(f"\n  Query {i}: {query_text}")
        print(f"  Number of rated documents: {len(judgment['ratings'])}")
        
        # Show first 3 ratings for this query
        for j, rating in enumerate(judgment['ratings'][:3], 1):
            print(f"    Doc {j}: {rating['docId']} -> rating: {rating['rating']}")
        
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
        
        print("\n  Overall score distribution (mapped values):")
        for score in sorted(score_dist.keys()):
            count = score_dist[score]
            # Show the mapping
            if score == '1.0':
                label = "E (Exact)"
            elif score == '0.7':
                label = "S (Substitute)"
            elif score == '0.3':
                label = "C (Complement)"
            elif score == '0.0':
                label = "I (Irrelevant)"
            else:
                label = "Unknown"
            print(f"    Score {score} ({label}): {count} documents")
    
    # Show number of judgments per query statistics
    judgments_per_query = [len(j['ratings']) for j in opensearch_data['judgmentRatings']]
    if judgments_per_query:
        avg_judgments = sum(judgments_per_query) / len(judgments_per_query)
        max_judgments = max(judgments_per_query)
        min_judgments = min(judgments_per_query)
        print(f"\n  Judgments per query statistics:")
        print(f"    Average: {avg_judgments:.1f} documents")
        print(f"    Min: {min_judgments} documents")
        print(f"    Max: {max_judgments} documents")
    
    # Check query length statistics
    query_lengths = [len(j['query']) for j in opensearch_data['judgmentRatings']]
    if query_lengths:
        avg_length = sum(query_lengths) / len(query_lengths)
        max_length = max(query_lengths)
        min_length = min(query_lengths)
        print(f"\n  Query length statistics after sanitization:")
        print(f"    Average: {avg_length:.0f} characters")
        print(f"    Min: {min_length} characters")
        print(f"    Max: {max_length} characters")
    
    print("\n" + "=" * 80)
    print("✓ Extraction complete with ULTRA sanitization!")
    print("File should now be 100% compatible with OpenSearch.")
    print("=" * 80)

if __name__ == "__main__":
    main()
