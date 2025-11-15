#!/usr/bin/env python3
"""
Extract KORE50 entity linking data for OpenSearch experiments.
KORE50 is a benchmark for entity linking with 50 sentences containing ambiguous entity mentions.
"""

import json
import re
from pathlib import Path

def parse_kore50_ttl(ttl_path):
    """Parse KORE50 TTL file to extract entity mentions and their DBpedia links."""
    with open(ttl_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    sentences = {}
    entity_mentions = []
    
    # Parse sentences
    sentence_pattern = r'<[^>]+/(CEL|MUS|BUS|SPO|POL)\d+#char=0,>\s+.*?\s+nif:isString\s+"([^"]+)"'
    for match in re.finditer(sentence_pattern, content, re.MULTILINE | re.DOTALL):
        doc_id = f"{match.group(1)}{match.group().split(match.group(1))[1].split('#')[0]}"
        sentence = match.group(2).replace('^^xsd:string', '').strip()
        sentences[doc_id] = sentence
    
    # Parse entity mentions and their DBpedia links
    mention_pattern = r'<[^>]+/(CEL|MUS|BUS|SPO|POL)\d+#char=(\d+),(\d+)>.*?nif:anchorOf\s+"([^"]+)".*?itsrdf:taIdentRef\s+<([^>]+)>'
    for match in re.finditer(mention_pattern, content, re.MULTILINE | re.DOTALL):
        doc_type = match.group(1)
        doc_num = match.group(0).split(doc_type)[1].split('#')[0]
        doc_id = f"{doc_type}{doc_num}"
        start_pos = int(match.group(2))
        end_pos = int(match.group(3))
        mention_text = match.group(4).replace('^^xsd:string', '').strip()
        dbpedia_uri = match.group(5)
        
        if doc_id in sentences:
            entity_mentions.append({
                'doc_id': doc_id,
                'sentence': sentences[doc_id],
                'mention': mention_text,
                'start': start_pos,
                'end': end_pos,
                'dbpedia_uri': dbpedia_uri,
                'category': doc_type  # CEL=Celebrity, MUS=Music, BUS=Business, SPO=Sports, POL=Politics
            })
    
    return entity_mentions

def create_opensearch_format(entity_mentions):
    """Convert KORE50 data to OpenSearch experiment format."""
    queries = []
    judgments = []
    
    # Create queries and judgments from entity mentions
    # Each mention becomes a query, with its sentence as the document
    query_id = 1
    doc_ids = {}
    
    for mention_data in entity_mentions:
        # Use the mention text as the query
        query_text = mention_data['mention']
        
        # Use the document ID based on the sentence
        doc_id = mention_data['doc_id']
        
        # Store the sentence as the document
        if doc_id not in doc_ids:
            doc_ids[doc_id] = {
                'id': doc_id,
                'text': mention_data['sentence'],
                'category': mention_data['category']
            }
        
        # Create query
        queries.append({
            'query_id': f"kore50_{query_id:03d}",
            'query': query_text,
            'context': mention_data['sentence'],
            'dbpedia_entity': mention_data['dbpedia_uri'].split('/')[-1],
            'category': mention_data['category']
        })
        
        # Create judgment (relevance: 1.0 for the sentence containing the mention)
        judgments.append({
            'query_id': f"kore50_{query_id:03d}",
            'doc_id': doc_id,
            'relevance': 1.0,
            'dbpedia_uri': mention_data['dbpedia_uri']
        })
        
        query_id += 1
    
    return queries, judgments, list(doc_ids.values())

def save_results(queries, judgments, documents):
    """Save extracted data to JSON files."""
    # Save queries
    queries_path = Path('results/kore50_queries.json')
    queries_path.parent.mkdir(exist_ok=True)
    with open(queries_path, 'w', encoding='utf-8') as f:
        json.dump(queries, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(queries)} queries to {queries_path}")
    
    # Save judgments
    judgments_path = Path('results/kore50_judgments.json')
    with open(judgments_path, 'w', encoding='utf-8') as f:
        json.dump(judgments, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(judgments)} judgments to {judgments_path}")
    
    # Save documents for reference
    docs_path = Path('results/kore50_documents.json')
    with open(docs_path, 'w', encoding='utf-8') as f:
        json.dump(documents, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(documents)} documents to {docs_path}")
    
    return queries_path, judgments_path

def create_experiment_config(queries_path, judgments_path):
    """Create OpenSearch experiment configuration."""
    config = {
        "experiment_name": "kore50_entity_linking",
        "dataset_name": "kore50",
        "index_name": "kore50_index",
        "embedding_model_id": "YOUR_MODEL_ID_HERE",
        "queries_file": str(queries_path),
        "judgments_file": str(judgments_path),
        "corpus_file": "results/kore50_documents.json",
        "search_configs": [
            {"name": "lexical_only", "weights": [0.0, 1.0]},
            {"name": "semantic_only", "weights": [1.0, 0.0]},
            {"name": "balanced", "weights": [0.5, 0.5]},
            {"name": "semantic_heavy_70", "weights": [0.7, 0.3]},
            {"name": "semantic_heavy_80", "weights": [0.8, 0.2]},
            {"name": "semantic_heavy_90", "weights": [0.9, 0.1]}
        ],
        "metrics": ["ndcg@10", "map", "precision@10"]
    }
    
    config_path = Path('configs/experiment_kore50.json')
    config_path.parent.mkdir(exist_ok=True)
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)
    print(f"\nCreated experiment config: {config_path}")

def main():
    print("Extracting KORE50 entity linking data...")
    
    # Path to KORE50 TTL file
    ttl_path = Path('aida/gerbil_data/datasets/KORE50/kore50-nif.ttl')
    
    if not ttl_path.exists():
        print(f"Error: {ttl_path} not found!")
        return
    
    # Parse KORE50 data
    entity_mentions = parse_kore50_ttl(ttl_path)
    
    # Convert to OpenSearch format
    queries, judgments, documents = create_opensearch_format(entity_mentions)
    
    # Save results
    queries_path, judgments_path = save_results(queries, judgments, documents)
    
    # Create experiment configuration
    create_experiment_config(queries_path, judgments_path)
    
    # Print statistics
    print(f"\n=== Dataset Statistics ===")
    print(f"Total entity mentions: {len(entity_mentions)}")
    print(f"Total sentences: {len(documents)}")
    print(f"Queries created: {len(queries)}")
    
    # Show category distribution
    categories = {}
    for mention in entity_mentions:
        cat = mention['category']
        categories[cat] = categories.get(cat, 0) + 1
    
    print(f"\n=== Category Distribution ===")
    category_names = {
        'CEL': 'Celebrities',
        'MUS': 'Music',
        'BUS': 'Business', 
        'SPO': 'Sports',
        'POL': 'Politics'
    }
    for cat, count in sorted(categories.items()):
        print(f"{category_names.get(cat, cat)}: {count} mentions")
    
    # Show example ambiguous mentions
    print(f"\n=== Example Ambiguous Mentions ===")
    examples = [
        ('David', 'David_Beckham'),
        ('Victoria', 'Victoria_Beckham'),
        ('Tiger', 'Tiger_Woods'),
        ('Madonna', 'Madonna_(entertainer)'),
        ('Dylan', 'Bob_Dylan'),
        ('Steve', 'Steve_Jobs'),
        ('Apple', 'Apple_Inc.'),
        ('City', 'Manchester_City_F.C.'),
        ('Real', 'Real_Madrid_C.F.'),
        ('Greece', 'Greece')
    ]
    
    for mention_text, expected_entity in examples:
        matching = [m for m in entity_mentions if m['mention'] == mention_text]
        if matching:
            actual_entity = matching[0]['dbpedia_uri'].split('/')[-1]
            print(f"'{mention_text}' -> <dbpedia:{actual_entity}>")
    
    print(f"\nExpected Results (based on KORE50 characteristics):")
    print("- Lexical (0.0/1.0): Poor performance on ambiguous mentions")
    print("- Balanced (0.5/0.5): Baseline performance")
    print("- Semantic-heavy (0.9/0.1): Significant improvement")
    print("- Pure semantic (1.0/0.0): Potentially best (8-12% improvement expected)")
    
    print(f"\n=== Setup Complete ===")
    print("1. Queries extracted and saved")
    print("2. Judgments extracted and saved")
    print("3. Experiment configuration created")
    print("\nNext steps:")
    print("1. Update the embedding_model_id in the config")
    print("2. Run the experiment with your OpenSearch pipeline")
    print("3. Compare results with DBPedia, MSNBC, and ACE2004")

if __name__ == "__main__":
    main()
