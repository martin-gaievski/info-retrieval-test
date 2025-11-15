#!/usr/bin/env python3
"""
Extract queries and judgments from ACE2004 entity linking dataset (GERBIL format)
ACE2004 is a newswire dataset with entity mentions that require disambiguation.
"""

import json
import os
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple

def extract_ace2004_data(ace2004_dir="aida/gerbil_data/datasets/ACE2004_Coref_Turking/Dev/ProblemsNoTranscripts"):
    """
    Extract queries (entity mentions) and judgments from ACE2004 XML files.
    
    Args:
        ace2004_dir: Path to ACE2004 dataset directory from GERBIL
    
    Returns:
        Tuple of (queries, judgments)
    """
    queries = []
    judgments = []
    query_id_counter = 0
    
    if not os.path.exists(ace2004_dir):
        raise FileNotFoundError(f"ACE2004 directory not found at {ace2004_dir}")
    
    # Process each XML file
    for filename in sorted(os.listdir(ace2004_dir)):
        if not filename.endswith('.eng'):
            continue
            
        filepath = os.path.join(ace2004_dir, filename)
        doc_id = filename.replace('.eng', '')
        
        print(f"Processing {filename}...")
        
        # Parse XML file
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        # Extract each reference instance (entity mention)
        for instance in root.findall('ReferenceInstance'):
            # Get surface form (the entity mention text)
            surface_form_elem = instance.find('SurfaceForm')
            if surface_form_elem is None:
                continue
            surface_form = surface_form_elem.text.strip()
            
            # Get Wikipedia annotation
            annotation_elem = instance.find('ChosenAnnotation')
            if annotation_elem is None:
                continue
            wiki_url = annotation_elem.text.strip()
            
            # Skip null annotations
            if wiki_url == '*null*' or wiki_url == 'null':
                continue
            
            # Extract Wikipedia entity ID from URL
            # http://en.wikipedia.org/wiki/Jerusalem -> Jerusalem
            if 'wikipedia.org/wiki/' in wiki_url:
                wiki_entity = wiki_url.split('wikipedia.org/wiki/')[-1]
                # Handle URL encoding and special characters
                wiki_entity = wiki_entity.replace('_', ' ')
                wiki_entity = wiki_entity.replace('%20', ' ')
                # Convert back to underscore format for DBPedia
                wiki_entity = wiki_entity.replace(' ', '_')
            else:
                continue
            
            # Get offset and length for unique ID
            offset_elem = instance.find('Offset')
            length_elem = instance.find('Length')
            if offset_elem is None or length_elem is None:
                continue
            offset = offset_elem.text.strip()
            length = length_elem.text.strip()
            
            # Create unique query ID
            query_id = f"ace2004_{query_id_counter}"
            query_id_counter += 1
            
            # Add query (the ambiguous entity mention)
            queries.append({
                "query_id": query_id,
                "query": surface_form,
                "doc_id": doc_id,
                "offset": int(offset),
                "length": int(length)
            })
            
            # Add judgment (ground truth Wikipedia entity)
            # Convert to DBPedia format: Jerusalem -> <dbpedia:Jerusalem>
            dbpedia_id = f"<dbpedia:{wiki_entity}>"
            judgments.append({
                "query_id": query_id,
                "doc_id": dbpedia_id,  # DBPedia-formatted entity ID
                "relevance": 1.0  # Binary relevance for entity linking
            })
    
    return queries, judgments

def save_ace2004_data(output_dir="results"):
    """
    Extract and save ACE2004 queries and judgments in OpenSearch-compatible format.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    print("Extracting ACE2004 entity linking data...")
    queries_raw, judgments = extract_ace2004_data()
    
    # Format queries for OpenSearch - simple list format
    queries_for_search = []
    for q in queries_raw:
        # Clean query text - remove special characters that might cause issues
        query_text = q["query"].replace('"', '').replace("'", "").strip()
        queries_for_search.append({
            "queryText": query_text  # OpenSearch format uses queryText field
        })
    
    # Format judgments for OpenSearch - grouped by query with ratings
    # Build a mapping from query_id to query_text
    query_id_to_text = {q["query_id"]: q["query"] for q in queries_raw}
    
    # Group judgments by query
    query_judgments = {}
    for j in judgments:
        query_id = j["query_id"]
        query_text = query_id_to_text[query_id]
        if query_text not in query_judgments:
            query_judgments[query_text] = []
        query_judgments[query_text].append({
            "docId": j["doc_id"],
            "rating": str(j["relevance"])  # Convert to string for OpenSearch
        })
    
    # Create judgmentRatings array
    judgment_ratings = []
    for query_text, ratings in query_judgments.items():
        # Clean query text
        query_text_clean = query_text.replace('"', '').replace("'", "").strip()
        judgment_ratings.append({
            "query": query_text_clean,
            "ratings": ratings
        })
    
    # Create OpenSearch-compatible query structure
    queries_opensearch = {
        "name": "ace2004_entity_linking_queries",
        "description": f"ACE2004 entity linking queries - {len(queries_for_search)} entity mentions from newswire",
        "querySetQueries": queries_for_search
    }
    
    # Save queries in OpenSearch format
    queries_file = os.path.join(output_dir, "ace2004_queries.json")
    with open(queries_file, 'w', encoding='utf-8') as f:
        json.dump(queries_opensearch, f, indent=2, ensure_ascii=True)
    print(f"Saved {len(queries_for_search)} queries to {queries_file}")
    
    # Create OpenSearch-compatible judgment structure
    judgments_opensearch = {
        "name": "ACE2004 Entity Linking Relevance Judgments",
        "description": f"ACE2004 entity linking judgments - {len(judgment_ratings)} queries with ground truth entity annotations",
        "type": "IMPORT_JUDGMENT",
        "judgmentRatings": judgment_ratings
    }
    
    # Save judgments in OpenSearch format
    judgments_file = os.path.join(output_dir, "ace2004_judgments.json")
    with open(judgments_file, 'w', encoding='utf-8') as f:
        json.dump(judgments_opensearch, f, indent=2, ensure_ascii=True)
    print(f"Saved {len(judgment_ratings)} judgment entries to {judgments_file}")
    
    # Print statistics
    print("\n=== Dataset Statistics ===")
    print(f"Total queries (entity mentions): {len(queries_raw)}")
    print(f"Total judgments: {len(judgments)}")
    print(f"Unique queries: {len(judgment_ratings)}")
    
    # Show some example ambiguous mentions
    print("\n=== Example Ambiguous Mentions ===")
    unique_mentions = {}
    for q in queries_raw[:50]:  # Check first 50
        mention = q["query"]
        if mention not in unique_mentions:
            unique_mentions[mention] = True
            if len(unique_mentions) <= 10:
                # Find corresponding judgment
                for j in judgments:
                    if j["query_id"] == q["query_id"]:
                        print(f"'{mention}' -> {j['doc_id']}")
                        break
    
    return queries_file, judgments_file

def create_experiment_config(queries_file, judgments_file, output_dir="configs"):
    """
    Create experiment configuration for ACE2004 dataset.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    config = {
        "dataset_name": "ace2004_entity_linking",
        "queries_file": queries_file,
        "judgments_file": judgments_file,
        "corpus_file": "datasets/dbpedia-entity/corpus.jsonl",  # Use existing Wikipedia/DBPedia corpus
        "index_name": "ace2004_wikipedia_index",
        "embedding_model_id": "your_model_id_here",  # Replace with actual model ID
        "experiment_configs": [
            {
                "name": "pure_lexical",
                "semantic_weight": 0.0,
                "lexical_weight": 1.0,
                "description": "Pure lexical search - expected to fail on ambiguous mentions"
            },
            {
                "name": "balanced",
                "semantic_weight": 0.5,
                "lexical_weight": 0.5,
                "description": "Balanced hybrid search - baseline"
            },
            {
                "name": "semantic_heavy",
                "semantic_weight": 0.9,
                "lexical_weight": 0.1,
                "description": "Semantic-heavy search - expected to excel at disambiguation"
            },
            {
                "name": "pure_semantic",
                "semantic_weight": 1.0,
                "lexical_weight": 0.0,
                "description": "Pure semantic search - may perform best like MSNBC"
            }
        ]
    }
    
    config_file = os.path.join(output_dir, "experiment_ace2004.json")
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"\nCreated experiment config: {config_file}")
    print("\nExpected Results (based on ACE2004 characteristics):")
    print("- Lexical (0.0/1.0): Poor performance on ambiguous mentions")
    print("- Balanced (0.5/0.5): Baseline performance")
    print("- Semantic-heavy (0.9/0.1): Significant improvement")
    print("- Pure semantic (1.0/0.0): Potentially best (8-12% improvement expected)")
    
    return config_file

if __name__ == "__main__":
    # Extract and save ACE2004 data
    queries_file, judgments_file = save_ace2004_data()
    
    # Create experiment configuration
    config_file = create_experiment_config(queries_file, judgments_file)
    
    print("\n=== Setup Complete ===")
    print("1. Queries extracted and saved")
    print("2. Judgments extracted and saved")
    print("3. Experiment configuration created")
    print("\nNext steps:")
    print("1. Update the embedding_model_id in the config")
    print("2. Run the experiment with your OpenSearch pipeline")
    print("3. Compare results with DBPedia and MSNBC for validation")
