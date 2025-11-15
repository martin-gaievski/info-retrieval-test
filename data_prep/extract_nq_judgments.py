#!/usr/bin/env python3
"""
Extract NQ relevance judgments for first 1,000 queries with ultra-aggressive sanitization for OpenSearch.
"""

import json
import csv
import os
import re
import string
import unicodedata
from typing import Dict, List, Set
from collections import defaultdict

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
    
    queries = {}
    query_ids = []  # To maintain order
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= n:
                break
            try:
                query_data = json.loads(line)
                query_id = query_data['_id']
                query_text = query_data['text']
                queries[query_id] = query_text
                query_ids.append(query_id)
            except json.JSONDecodeError:
                continue
    
    return queries, query_ids

def load_qrels(qrels_file: str, query_ids: Set[str]) -> Dict[str, List[Dict[str, str]]]:
    """Load relevance judgments from the qrels TSV file for specific query IDs.
    
    Returns a dictionary mapping query_id to a list of {docId, rating} dicts.
    """
    qrels = defaultdict(list)
    
    with open(qrels_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)  # Skip header
        
        for row in reader:
            if len(row) >= 3:
                query_id = row[0]
                
                # Only include judgments for our first 1000 queries
                if query_id not in query_ids:
                    continue
                    
                doc_id = row[1]
                score = int(row[2])
                
                # NQ uses binary relevance
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

def create_opensearch_judgments_format(qrels: Dict, all_queries: Dict, query_ids: List[str]) -> Dict:
    """Create the OpenSearch Search Relevance Workbench judgment import format."""
    
    judgment_ratings = []
    skipped_queries = []
    queries_with_judgments = set()
    sanitization_stats = {
        'total': 0,
        'skipped_too_short': 0,
        'contains_non_ascii': 0,
        'no_judgments': 0
    }
    
    # Process queries in the order they appeared in the file
    for query_id in query_ids:
        sanitization_stats['total'] += 1
        
        # Skip if no judgments for this query
        if query_id not in qrels:
            sanitization_stats['no_judgments'] += 1
            continue
        
        if query_id not in all_queries:
            skipped_queries.append(query_id)
            continue
        
        # Get and sanitize the query text
        original_text = all_queries[query_id]
        sanitized_text = ultra_sanitize_query_text(original_text)
        
        # Check if sanitization failed (returned None)
        if sanitized_text is None:
            skipped_queries.append(query_id)
            sanitization_stats['skipped_too_short'] += 1
            continue
        
        # Final ASCII check
        try:
            sanitized_text.encode('ascii')
        except UnicodeEncodeError:
            sanitization_stats['contains_non_ascii'] += 1
            # Force to ASCII
            sanitized_text = sanitized_text.encode('ascii', 'ignore').decode('ascii')
        
        queries_with_judgments.add(query_id)
        
        # Add the judgment rating entry
        judgment_ratings.append({
            "query": sanitized_text,
            "ratings": qrels[query_id]
        })
    
    # Create the final structure
    opensearch_judgments = {
        "name": "NQ Relevance Judgments",
        "description": f"Natural Questions containing relevance judgments for {len(judgment_ratings)} queries from the BEIR benchmark dataset",
        "type": "IMPORT_JUDGMENT",
        "judgmentRatings": judgment_ratings
    }
    
    print(f"\n   Sanitization statistics:")
    print(f"     - Total queries processed: {sanitization_stats['total']}")
    print(f"     - Queries without judgments: {sanitization_stats['no_judgments']}")
    print(f"     - Skipped (too short after sanitization): {sanitization_stats['skipped_too_short']}")
    print(f"     - Had non-ASCII after first pass: {sanitization_stats['contains_non_ascii']}")
    
    if skipped_queries:
        print(f"\n   Warning: Skipped {len(skipped_queries)} queries")
        print(f"     - {len(skipped_queries)} total (no text or too short after sanitization)")
    
    return opensearch_judgments, queries_with_judgments

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
    for i, judgment in enumerate(opensearch_data['judgmentRatings'][:100]):
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
    """Main function to extract and format NQ judgments."""
    
    # File paths
    queries_file = "datasets/nq/queries.jsonl"
    qrels_file = "datasets/nq/qrels/test.tsv"
    output_file = "results/nq_judgments.json"
    
    print("=" * 80)
    print("NQ (Natural Questions) Judgment Extraction with ULTRA-AGGRESSIVE Sanitization")
    print("=" * 80)
    
    # Step 1: Load first 1000 queries
    print("\n1. Loading first 1,000 queries from queries.jsonl...")
    all_queries, query_ids = load_first_n_queries(queries_file, 1000)
    print(f"   Loaded {len(all_queries)} queries")
    
    # Step 2: Load relevance judgments for those queries
    print("\n2. Loading relevance judgments from test.tsv...")
    query_ids_set = set(query_ids)
    qrels = load_qrels(qrels_file, query_ids_set)
    print(f"   Loaded judgments for {len(qrels)} queries")
    
    # Count total judgments
    total_judgments = sum(len(ratings) for ratings in qrels.values())
    print(f"   Total query-document pairs: {total_judgments:,}")
    
    # Step 3: Create OpenSearch import format
    print("\n3. Creating OpenSearch judgment import format with ULTRA sanitization...")
    opensearch_data, queries_with_judgments = create_opensearch_judgments_format(qrels, all_queries, query_ids)
    print(f"   Formatted {len(opensearch_data['judgmentRatings'])} queries with judgments")
    print(f"   Note: NQ uses binary relevance (1.0 = relevant)")
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
    print(f"Total queries with judgments: {len(opensearch_data['judgmentRatings'])}")
    print(f"Total query-document pairs: {total_judgments:,}")
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file):,} bytes")
    
    # Show sample judgments
    print("\nSample judgments (first 3 queries):")
    for i, judgment in enumerate(opensearch_data['judgmentRatings'][:3], 1):
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
        
        print("\n  Overall score distribution:")
        for score in sorted(score_dist.keys()):
            count = score_dist[score]
            print(f"    Score {score}: {count} documents")
    
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
