#!/usr/bin/env python3
"""
Extract AQUAINT entity linking data for OpenSearch experiments.
AQUAINT is a news corpus with entity annotations from the Wikification ACL 2011 dataset.
"""

import json
import re
import os
from pathlib import Path
from html.parser import HTMLParser
from xml.etree import ElementTree as ET

class HTMLTextExtractor(HTMLParser):
    """Extract plain text from HTML."""
    def __init__(self):
        super().__init__()
        self.text = []
        
    def handle_data(self, data):
        self.text.append(data)
        
    def get_text(self):
        return ''.join(self.text)

def parse_aquaint_document(raw_text_path, annotations_path):
    """Parse a single AQUAINT document and its annotations."""
    
    # Read the raw text (HTML format)
    with open(raw_text_path, 'r', encoding='utf-8', errors='ignore') as f:
        html_content = f.read()
    
    # Extract plain text from HTML
    parser = HTMLTextExtractor()
    parser.feed(html_content)
    text = parser.get_text().strip()
    
    # Parse annotations XML
    with open(annotations_path, 'r', encoding='utf-8', errors='ignore') as f:
        xml_content = f.read()
    
    # Parse XML annotations
    entity_mentions = []
    
    # Find all ReferenceInstance elements
    pattern = r'<ReferenceInstance>(.*?)</ReferenceInstance>'
    instances = re.findall(pattern, xml_content, re.DOTALL)
    
    for instance in instances:
        # Extract fields
        surface_match = re.search(r'<SurfaceForm>\s*(.*?)\s*</SurfaceForm>', instance, re.DOTALL)
        offset_match = re.search(r'<Offset>\s*(\d+)\s*</Offset>', instance)
        length_match = re.search(r'<Length>\s*(\d+)\s*</Length>', instance)
        annotation_match = re.search(r'<ChosenAnnotation>\s*(.*?)\s*</ChosenAnnotation>', instance, re.DOTALL)
        
        if surface_match and offset_match and length_match and annotation_match:
            surface_form = surface_match.group(1).strip()
            offset = int(offset_match.group(1))
            length = int(length_match.group(1))
            wiki_url = annotation_match.group(1).strip()
            
            # Extract entity name from Wikipedia URL
            if 'wikipedia.org/wiki/' in wiki_url:
                entity = wiki_url.split('/wiki/')[-1]
            else:
                entity = wiki_url
            
            entity_mentions.append({
                'surface_form': surface_form,
                'offset': offset,
                'length': length,
                'entity': entity,
                'wiki_url': wiki_url
            })
    
    return text, entity_mentions

def create_opensearch_format(documents_data):
    """Convert AQUAINT data to OpenSearch experiment format."""
    queries = []
    judgments = []
    documents = []
    
    query_id = 1
    
    for doc_name, doc_data in documents_data.items():
        text = doc_data['text']
        mentions = doc_data['mentions']
        
        # Store document
        documents.append({
            'id': doc_name,
            'text': text
        })
        
        # Create queries and judgments from entity mentions
        for mention in mentions:
            # Use the surface form as the query
            query_text = mention['surface_form']
            entity = mention['entity']
            
            # Create query
            queries.append({
                'query_id': f"aquaint_{query_id:03d}",
                'query': query_text,
                'document_context': doc_name,
                'entity': entity,
                'offset': mention['offset']
            })
            
            # Create judgment (relevance: 1.0 for the document containing the mention)
            judgments.append({
                'query_id': f"aquaint_{query_id:03d}",
                'doc_id': doc_name,
                'relevance': 1.0,
                'entity': entity
            })
            
            query_id += 1
    
    return queries, judgments, documents

def save_results(queries, judgments, documents):
    """Save extracted data to JSON files."""
    # Save queries
    queries_path = Path('results/aquaint_queries.json')
    queries_path.parent.mkdir(exist_ok=True)
    with open(queries_path, 'w', encoding='utf-8') as f:
        json.dump(queries, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(queries)} queries to {queries_path}")
    
    # Save judgments
    judgments_path = Path('results/aquaint_judgments.json')
    with open(judgments_path, 'w', encoding='utf-8') as f:
        json.dump(judgments, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(judgments)} judgments to {judgments_path}")
    
    # Save documents for reference
    docs_path = Path('results/aquaint_documents.json')
    with open(docs_path, 'w', encoding='utf-8') as f:
        json.dump(documents, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(documents)} documents to {docs_path}")
    
    return queries_path, judgments_path

def create_experiment_config(queries_path, judgments_path):
    """Create OpenSearch experiment configuration."""
    config = {
        "experiment_name": "aquaint_entity_linking",
        "dataset_name": "aquaint",
        "index_name": "aquaint_index",
        "embedding_model_id": "YOUR_MODEL_ID_HERE",
        "queries_file": str(queries_path),
        "judgments_file": str(judgments_path),
        "corpus_file": "results/aquaint_documents.json",
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
    
    config_path = Path('configs/experiment_aquaint.json')
    config_path.parent.mkdir(exist_ok=True)
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2)
    print(f"\nCreated experiment config: {config_path}")

def main():
    print("Extracting AQUAINT entity linking data...")
    
    # Paths to AQUAINT data
    base_path = Path('datasets/WikificationACL2011Data/AQUAINT')
    raw_texts_path = base_path / 'RawTexts'
    annotations_path = base_path / 'Problems'
    
    if not raw_texts_path.exists() or not annotations_path.exists():
        print(f"Error: AQUAINT data not found at {base_path}")
        return
    
    # Process all documents
    documents_data = {}
    
    # Get all HTML files (excluding .svn)
    html_files = [f for f in raw_texts_path.glob('*.htm') if not f.name.startswith('.')]
    
    print(f"Found {len(html_files)} documents to process")
    
    for html_file in html_files:
        doc_name = html_file.stem
        raw_path = raw_texts_path / html_file.name
        ann_path = annotations_path / html_file.name
        
        if ann_path.exists():
            try:
                text, mentions = parse_aquaint_document(raw_path, ann_path)
                if mentions:  # Only include documents with annotations
                    documents_data[doc_name] = {
                        'text': text,
                        'mentions': mentions
                    }
            except Exception as e:
                print(f"Error processing {doc_name}: {e}")
    
    print(f"Successfully processed {len(documents_data)} documents with annotations")
    
    # Convert to OpenSearch format
    queries, judgments, documents = create_opensearch_format(documents_data)
    
    # Save results
    queries_path, judgments_path = save_results(queries, judgments, documents)
    
    # Create experiment configuration
    create_experiment_config(queries_path, judgments_path)
    
    # Print statistics
    print(f"\n=== Dataset Statistics ===")
    print(f"Total documents: {len(documents)}")
    print(f"Total entity mentions: {len(queries)}")
    print(f"Queries created: {len(queries)}")
    print(f"Judgments created: {len(judgments)}")
    
    # Show example mentions
    print(f"\n=== Example Entity Mentions ===")
    examples_shown = 0
    seen_surfaces = set()
    for query in queries[:50]:  # Check first 50 for variety
        surface = query['query']
        if surface not in seen_surfaces and examples_shown < 10:
            entity = query['entity']
            print(f"'{surface}' -> {entity}")
            seen_surfaces.add(surface)
            examples_shown += 1
    
    # Count unique surface forms
    unique_surfaces = len(set(q['query'] for q in queries))
    print(f"\n=== Ambiguity Analysis ===")
    print(f"Total mentions: {len(queries)}")
    print(f"Unique surface forms: {unique_surfaces}")
    print(f"Average mentions per surface: {len(queries)/unique_surfaces:.1f}")
    
    print(f"\nExpected Results (based on AQUAINT characteristics):")
    print("- Lexical (0.0/1.0): Poor performance on entity mentions")
    print("- Balanced (0.5/0.5): Baseline performance")
    print("- Semantic-heavy (0.9/0.1): Significant improvement (10-15% expected)")
    print("- Pure semantic (1.0/0.0): Potentially best for disambiguation")
    
    print(f"\n=== Setup Complete ===")
    print("1. Queries extracted and saved")
    print("2. Judgments extracted and saved")
    print("3. Experiment configuration created")
    print("\nNext steps:")
    print("1. Update the embedding_model_id in the config")
    print("2. Run the experiment with your OpenSearch pipeline")
    print("3. Compare results with DBPedia, MSNBC, ACE2004, and KORE50")

if __name__ == "__main__":
    main()
