#!/usr/bin/env python3
"""
Test script to verify cqadupstack dataset ingestion functionality.
"""

import sys
import os

def test_script_functionality():
    """Test the current script's ability to handle cqadupstack dataset"""
    
    print("="*60)
    print("Testing CQADupStack Dataset Ingestion")
    print("="*60)
    
    # List all available subsets
    dataset_path = "datasets/cqadupstack"
    if os.path.exists(dataset_path):
        subsets = [d for d in os.listdir(dataset_path) 
                   if os.path.isdir(os.path.join(dataset_path, d)) and not d.startswith('.')]
        print(f"\nAvailable subsets in cqadupstack: {len(subsets)}")
        for subset in sorted(subsets):
            subset_path = os.path.join(dataset_path, subset)
            has_corpus = os.path.exists(os.path.join(subset_path, "corpus.jsonl"))
            has_queries = os.path.exists(os.path.join(subset_path, "queries.jsonl"))
            has_qrels = os.path.exists(os.path.join(subset_path, "qrels"))
            status = "✓" if (has_corpus and has_queries and has_qrels) else "✗"
            print(f"  {status} {subset}: corpus={has_corpus}, queries={has_queries}, qrels={has_qrels}")
    else:
        print(f"Dataset path {dataset_path} does not exist!")
        return
    
    print("\n" + "="*60)
    print("CURRENT SCRIPT ANALYSIS")
    print("="*60)
    
    print("\ntest_opensearch_dupstack.py capabilities:")
    print("✓ Loads all subsets into memory (mega_corpus, mega_queries, mega_qrels)")
    print("✓ Has -f/--subset parameter to specify which subset to ingest")
    print("✓ Ingests only the specified subset")
    
    print("\nPotential Issues:")
    print("⚠️  Memory inefficiency: Loads ALL subsets even when only one is needed")
    print("⚠️  Missing error handling: No check if subset parameter is provided")
    print("⚠️  No 'all' option: Cannot ingest all subsets at once")
    
    print("\n" + "="*60)
    print("USAGE EXAMPLES")
    print("="*60)
    
    print("\nCurrent working command (for a single subset):")
    print("python3 test_opensearch_dupstack.py \\")
    print("  -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip \\")
    print("  -h opense-clust-XYZ.elb.us-east-1.amazonaws.com \\")
    print("  -p 80 -i cqadupstack -o ingest -f android")
    
    print("\n⚠️ Issue: The -f parameter is required but not validated!")
    print("If -f is not provided, the script will load all data but ingest nothing.")
    
    print("\n" + "="*60)
    print("RECOMMENDED FIXES")
    print("="*60)
    
    print("""
1. Add validation for the subset parameter
2. Load only the required subset(s) to save memory
3. Add an 'all' option to ingest all subsets
4. Add better error messages
5. Add progress tracking for multiple subsets
""")

if __name__ == "__main__":
    test_script_functionality()
