"""
Amazon ESCI Data Loader for BEIR
Loads Amazon Shopping Queries Dataset (ESCI) and converts to BEIR format
"""

from typing import Dict, Tuple
from tqdm.autonotebook import tqdm
import pandas as pd
import json
import os
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Loads Amazon ESCI dataset and converts to BEIR format:
    - corpus: product catalog (from parquet files)
    - queries: search queries  
    - qrels: relevance judgments (E/S/C/I converted to numeric)
    """
    
    def __init__(self, data_folder: str, language: str = "us", 
                 small_version: bool = True):
        """
        Initialize ESCI data loader.
        
        Args:
            data_folder: Path to ESCI dataset folder
            language: Language version (us, es, jp)
            small_version: Use small version of dataset
        """
        self.data_folder = data_folder
        self.language = language
        self.small_version = small_version
        
        self.corpus = {}
        self.queries = {}
        self.qrels = {}
        
        # ESCI label to score mapping (using integers for pytrec_eval)
        self.label_to_score = {
            'E': 3,      # Exact
            'S': 2,      # Substitute
            'C': 1,      # Complement  
            'I': 0       # Irrelevant
        }
    
    def load(self, split: str = "test") -> Tuple[Dict[str, Dict[str, str]], 
                                                  Dict[str, str], 
                                                  Dict[str, Dict[str, int]]]:
        """
        Load ESCI data and convert to BEIR format.
        
        Args:
            split: Data split to load (train/test)
            
        Returns:
            Tuple of (corpus, queries, qrels)
        """
        logger.info(f"Loading Amazon ESCI dataset ({self.language}, {split})")
        
        # Load product catalog
        if not self.corpus:
            logger.info("Loading product catalog...")
            self._load_corpus()
            logger.info(f"Loaded {len(self.corpus)} products")
            if self.corpus:
                logger.info(f"Product example: {list(self.corpus.values())[0]}")
        
        # Load queries and relevance judgments
        if not self.queries:
            logger.info(f"Loading queries for {split} split...")
            self._load_queries_and_qrels(split)
            logger.info(f"Loaded {len(self.queries)} queries")
            if self.queries:
                logger.info(f"Query example: {list(self.queries.values())[0]}")
        
        return self.corpus, self.queries, self.qrels
    
    def _load_corpus(self):
        """Load product catalog from parquet files."""
        # Determine file name based on version
        if self.small_version:
            filenames = [
                f"shopping_queries_dataset_products_{self.language}_small.parquet",
                "shopping_queries_dataset_products.parquet"  # GitHub name
            ]
        else:
            filenames = [
                f"shopping_queries_dataset_products_{self.language}.parquet",
                "shopping_queries_dataset_products.parquet"  # GitHub name
            ]
        
        # Try each filename
        filepath = None
        for filename in filenames:
            temp_path = os.path.join(self.data_folder, filename)
            if os.path.exists(temp_path):
                filepath = temp_path
                break
        
        if not filepath:
            raise FileNotFoundError(f"Product catalog not found. Tried: {filenames}")
        
        # Load products
        logger.info(f"Reading products from {filename}")
        products_df = pd.read_parquet(filepath)
        
        # Convert to BEIR corpus format
        for _, row in tqdm(products_df.iterrows(), total=len(products_df), 
                          desc="Processing products"):
            product_id = str(row['product_id'])
            
            # Combine title and description for text field
            title = row.get('product_title', '')
            brand = row.get('product_brand', '')
            color = row.get('product_color', '')
            description = row.get('product_description', '')
            
            # Build text representation
            text_parts = []
            if title:
                text_parts.append(title)
            if brand and brand != 'null':
                text_parts.append(f"Brand: {brand}")
            if color and color != 'null':
                text_parts.append(f"Color: {color}")
            if description and len(description) > 10:  # Skip very short descriptions
                text_parts.append(description)
            
            text = " ".join(text_parts)
            
            # Store in corpus with metadata
            self.corpus[product_id] = {
                "text": text,
                "title": title,
                "metadata": {
                    "product_id": product_id,
                    "brand": brand,
                    "color": color,
                    "locale": row.get('product_locale', self.language)
                }
            }
    
    def _load_queries_and_qrels(self, split: str):
        """Load queries and relevance judgments."""
        # Determine file names to try
        if self.small_version:
            filenames = [
                f"shopping_queries_dataset_examples_{self.language}_small.parquet",
                "shopping_queries_dataset_examples.parquet"  # GitHub name
            ]
        else:
            filenames = [
                f"shopping_queries_dataset_examples_{self.language}.parquet",
                "shopping_queries_dataset_examples.parquet"  # GitHub name
            ]
        
        # Try each filename
        filepath = None
        for filename in filenames:
            temp_path = os.path.join(self.data_folder, filename)
            if os.path.exists(temp_path):
                filepath = temp_path
                break
        
        if not filepath:
            raise FileNotFoundError(f"Query file not found. Tried: {filenames}")
        
        # Load query data
        logger.info(f"Reading queries from {filename}")
        queries_df = pd.read_parquet(filepath)
        
        # Filter by split
        queries_df = queries_df[queries_df['split'] == split]
        logger.info(f"Found {len(queries_df)} examples for {split} split")
        
        # Process queries and build qrels
        for _, row in tqdm(queries_df.iterrows(), total=len(queries_df),
                          desc="Processing queries"):
            query_id = str(row['query_id'])
            query_text = row['query']
            product_id = str(row['product_id'])
            label = row['esci_label']
            
            # Store query
            if query_id not in self.queries:
                self.queries[query_id] = query_text
            
            # Convert label to score and store in qrels
            score = self.label_to_score.get(label, 0.0)
            
            if query_id not in self.qrels:
                self.qrels[query_id] = {}
            
            # Only include non-zero relevance scores
            if score > 0:
                self.qrels[query_id][product_id] = score
        
        # Log label distribution
        label_counts = queries_df['esci_label'].value_counts()
        logger.info("Label distribution:")
        for label, count in label_counts.items():
            logger.info(f"  {label}: {count} ({count/len(queries_df)*100:.1f}%)")


# Convenience function for compatibility with BEIR
def load_esci(data_folder: str, language: str = "us", 
              small_version: bool = True, split: str = "test"):
    """
    Load Amazon ESCI dataset.
    
    Args:
        data_folder: Path to ESCI dataset
        language: Language version (us, es, jp)
        small_version: Use small version
        split: Data split (train/test)
        
    Returns:
        Tuple of (corpus, queries, qrels)
    """
    loader = DataLoader(data_folder, language, small_version)
    return loader.load(split)
