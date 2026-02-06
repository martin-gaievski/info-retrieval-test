#!/usr/bin/env python3
"""
Hybrid Query Field Auto-Detector

Automatically detects optimal field configuration for hybrid search queries
based on index mapping analysis with minimal user input.

Usage:
    python dynamic_hybrid/hybrid_field_detector.py \
        --host <opensearch_host> --port 80 --index <index_name>
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import requests


class Confidence(Enum):
    """Detection confidence levels."""
    HIGH = "HIGH"       # Unique field or strong naming correlation
    MEDIUM = "MEDIUM"   # Pattern match
    LOW = "LOW"         # Fallback/guess
    NONE = "NONE"       # Not detected


@dataclass
class FieldContentStats:
    """Statistics about field content from sampling."""
    field_name: str
    avg_length: float = 0.0
    sample_count: int = 0
    contains_fields: List[str] = field(default_factory=list)  # Fields whose content is contained in this field
    is_concatenated: bool = False


@dataclass
class DetectedHybridConfig:
    """Result of hybrid field auto-detection."""
    neural_field: Optional[str] = None
    neural_confidence: Confidence = Confidence.NONE
    
    lexical_fields: List[str] = field(default_factory=list)
    lexical_confidence: Dict[str, Confidence] = field(default_factory=dict)
    
    title_field: Optional[str] = None
    title_confidence: Confidence = Confidence.NONE
    
    body_field: Optional[str] = None
    body_confidence: Confidence = Confidence.NONE
    
    suggested_boosts: Dict[str, float] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    
    # Field redundancy detection
    concatenated_fields: List[str] = field(default_factory=list)  # Fields detected as concatenations
    field_stats: Dict[str, FieldContentStats] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            "neural_field": self.neural_field,
            "neural_confidence": self.neural_confidence.value,
            "lexical_fields": self.lexical_fields,
            "lexical_confidence": {k: v.value for k, v in self.lexical_confidence.items()},
            "title_field": self.title_field,
            "title_confidence": self.title_confidence.value,
            "body_field": self.body_field,
            "body_confidence": self.body_confidence.value,
            "suggested_boosts": self.suggested_boosts,
            "warnings": self.warnings
        }
    
    def generate_hybrid_query(self, query_text: str, model_id: str, size: int = 100) -> Dict:
        """Generate a hybrid query based on detected configuration."""
        if not self.neural_field:
            raise ValueError("No neural field detected - cannot generate hybrid query")
        
        if not self.lexical_fields:
            raise ValueError("No lexical fields detected - cannot generate hybrid query")
        
        # Build lexical fields with boosts
        lexical_fields_with_boost = []
        for field_name in self.lexical_fields:
            boost = self.suggested_boosts.get(field_name, 1.0)
            if boost != 1.0:
                lexical_fields_with_boost.append(f"{field_name}^{boost}")
            else:
                lexical_fields_with_boost.append(field_name)
        
        return {
            "size": size,
            "query": {
                "hybrid": {
                    "queries": [
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": lexical_fields_with_boost,
                                "type": "best_fields"
                            }
                        },
                        {
                            "neural": {
                                self.neural_field: {
                                    "query_text": query_text,
                                    "model_id": model_id,
                                    "k": size
                                }
                            }
                        }
                    ]
                }
            }
        }


class HybridFieldDetector:
    """
    Auto-detects optimal fields for hybrid search based on index mapping.
    
    Detection Strategy:
    1. Neural field: Find the unique knn_vector field (deterministic)
    2. Primary lexical field: Look for naming correlation with neural field
    3. Title field: Pattern match against known title patterns
    4. Body field: Pattern match against known body/content patterns
    """
    
    # Patterns for title field detection
    TITLE_PATTERNS = [
        r"^title[_\-]?.*",       # title, title_key, title-field
        r".*[_\-]?title$",       # product_title, doc_title
        r"^name$",               # name (common for entity indices)
        r"^headline[_\-]?.*",    # headline, headline_text
        r"^subject[_\-]?.*",     # subject, subject_line
        r".*[_\-]?name$",        # product_name
    ]
    
    # Patterns for body/content field detection
    BODY_PATTERNS = [
        r"^text[_\-]?.*",        # text, text_key
        r".*[_\-]?text$",        # passage_text, full_text
        r"^content[_\-]?.*",     # content, content_field
        r".*[_\-]?content$",     # page_content
        r"^body[_\-]?.*",        # body, body_text
        r".*[_\-]?body$",        # message_body
        r"^description[_\-]?.*", # description
        r".*[_\-]?description$", # product_description
        r"^abstract[_\-]?.*",    # abstract (academic)
        r".*[_\-]?abstract$",    # document_abstract
        r"^passage[_\-]?.*",     # passage, passage_text
        r".*[_\-]?passage$",     # doc_passage
        r"^summary[_\-]?.*",     # summary
        r"^info[_\-]?.*",        # info, info_text
        r".*[_\-]?info$",        # product_info
    ]
    
    # Common neural field naming patterns
    NEURAL_PATTERNS = [
        r".*embedding.*",        # passage_embedding, text_embedding
        r".*vector.*",           # content_vector, doc_vector
        r".*dense.*",            # dense_vector
        r".*knn.*",              # knn_field
    ]
    
    def __init__(self, host: str, port: int = 80, sample_docs: int = 10):
        """Initialize detector with OpenSearch connection."""
        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()
        self.sample_docs = sample_docs  # Number of docs to sample for content analysis
    
    def detect(self, index_name: str, enable_sampling: bool = True) -> DetectedHybridConfig:
        """
        Detect optimal hybrid field configuration for an index.
        
        Args:
            index_name: Name of the OpenSearch index
            enable_sampling: Whether to sample documents to detect concatenated fields
            
        Returns:
            DetectedHybridConfig with detected fields
        """
        # Fetch mapping
        mapping = self._get_mapping(index_name)
        if not mapping:
            config = DetectedHybridConfig()
            config.warnings.append(f"Failed to retrieve mapping for index '{index_name}'")
            return config
        
        # Parse field properties
        properties = self._extract_properties(mapping, index_name)
        
        # Classify fields by type
        knn_fields = []
        text_fields = []
        
        for field_name, field_def in properties.items():
            field_type = field_def.get("type", "")
            if field_type == "knn_vector":
                knn_fields.append((field_name, field_def))
            elif field_type == "text":
                text_fields.append(field_name)
        
        # Run detection
        config = DetectedHybridConfig()
        
        # 1. Detect neural field (should be unique)
        self._detect_neural_field(config, knn_fields)
        
        # 2. Sample documents to detect concatenated fields (if enabled)
        if enable_sampling and len(text_fields) > 1:
            self._detect_concatenated_fields(config, index_name, text_fields)
        
        # 3. Detect lexical fields (excluding concatenated ones)
        non_concatenated_fields = [f for f in text_fields if f not in config.concatenated_fields]
        self._detect_lexical_fields(config, non_concatenated_fields, text_fields)
        
        # 4. Compute suggested boosts
        self._compute_boosts(config)
        
        return config
    
    def _sample_documents(self, index_name: str, text_fields: List[str]) -> List[Dict]:
        """Sample random documents from the index."""
        try:
            # Request only the text fields we care about
            source_fields = text_fields
            query = {
                "size": self.sample_docs,
                "query": {"function_score": {"query": {"match_all": {}}, "random_score": {}}},
                "_source": source_fields
            }
            response = self.session.post(
                f"{self.base_url}/{index_name}/_search",
                json=query,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 200:
                hits = response.json().get("hits", {}).get("hits", [])
                return [hit.get("_source", {}) for hit in hits]
            return []
        except Exception as e:
            print(f"Error sampling documents: {e}")
            return []
    
    def _detect_concatenated_fields(
        self,
        config: DetectedHybridConfig,
        index_name: str,
        text_fields: List[str]
    ):
        """
        Detect fields that are concatenations of other fields.
        
        A field is considered concatenated if it contains the content of 
        one or more other fields across multiple sampled documents.
        """
        docs = self._sample_documents(index_name, text_fields)
        if not docs:
            config.warnings.append("Could not sample documents - skipping concatenation detection")
            return
        
        # Compute field stats and detect containment
        field_lengths: Dict[str, List[int]] = {f: [] for f in text_fields}
        containment_counts: Dict[str, Dict[str, int]] = {f: {g: 0 for g in text_fields if g != f} for f in text_fields}
        
        for doc in docs:
            for field_a in text_fields:
                content_a = doc.get(field_a, "") or ""
                if content_a:
                    field_lengths[field_a].append(len(content_a))
                    
                    # Check if field_a contains other fields
                    for field_b in text_fields:
                        if field_a == field_b:
                            continue
                        content_b = doc.get(field_b, "") or ""
                        if content_b and len(content_b) > 10:  # Minimum meaningful length
                            # Check if content_b is contained in content_a
                            if content_b in content_a:
                                containment_counts[field_a][field_b] += 1
        
        # Analyze results
        threshold = max(1, len(docs) * 0.7)  # 70% of docs must show containment
        
        for field_a in text_fields:
            contained_fields = []
            for field_b, count in containment_counts[field_a].items():
                if count >= threshold:
                    contained_fields.append(field_b)
            
            if contained_fields:
                # field_a is a concatenation of contained_fields
                config.concatenated_fields.append(field_a)
                avg_len = sum(field_lengths[field_a]) / max(1, len(field_lengths[field_a]))
                
                stats = FieldContentStats(
                    field_name=field_a,
                    avg_length=avg_len,
                    sample_count=len(docs),
                    contains_fields=contained_fields,
                    is_concatenated=True
                )
                config.field_stats[field_a] = stats
                
                config.warnings.append(
                    f"Field '{field_a}' detected as concatenation of {contained_fields}. "
                    f"Excluding from lexical search to avoid double-counting."
                )
    
    def _get_mapping(self, index_name: str) -> Optional[Dict]:
        """Fetch index mapping from OpenSearch."""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}/_mapping")
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            print(f"Error fetching mapping: {e}")
            return None
    
    def _extract_properties(self, mapping: Dict, index_name: str) -> Dict:
        """Extract field properties from mapping response."""
        # Handle both single-index and multi-index responses
        if index_name in mapping:
            return mapping[index_name].get("mappings", {}).get("properties", {})
        else:
            # May be returned without index wrapper
            return mapping.get("mappings", {}).get("properties", {})
    
    def _detect_neural_field(
        self, 
        config: DetectedHybridConfig, 
        knn_fields: List[Tuple[str, Dict]]
    ):
        """Detect the neural (knn_vector) field."""
        if len(knn_fields) == 0:
            config.warnings.append("No knn_vector fields found - neural search not available")
            config.neural_confidence = Confidence.NONE
            return
        
        if len(knn_fields) == 1:
            # Unique knn_vector field - high confidence
            config.neural_field = knn_fields[0][0]
            config.neural_confidence = Confidence.HIGH
            return
        
        # Multiple knn_vector fields - need to disambiguate
        # Prefer fields with "embedding" in name
        for field_name, _ in knn_fields:
            if any(re.match(p, field_name, re.IGNORECASE) for p in self.NEURAL_PATTERNS):
                config.neural_field = field_name
                config.neural_confidence = Confidence.MEDIUM
                config.warnings.append(
                    f"Multiple knn_vector fields found: {[f[0] for f in knn_fields]}. "
                    f"Selected '{field_name}' based on naming pattern."
                )
                return
        
        # Fallback to first one
        config.neural_field = knn_fields[0][0]
        config.neural_confidence = Confidence.LOW
        config.warnings.append(
            f"Multiple knn_vector fields found: {[f[0] for f in knn_fields]}. "
            f"Selected first one '{knn_fields[0][0]}'."
        )
    
    def _detect_lexical_fields(
        self,
        config: DetectedHybridConfig,
        text_fields: List[str],
        all_text_fields: Optional[List[str]] = None
    ):
        """
        Detect lexical search fields (title, body, primary).
        
        Args:
            config: Detection configuration to update
            text_fields: Text fields to consider (excludes concatenated fields)
            all_text_fields: All original text fields (for pattern matching fallback)
        """
        all_fields = all_text_fields or text_fields
        
        if not text_fields:
            config.warnings.append("No text fields found - lexical search not available")
            return
        
        # Strategy 1: Find primary field by naming correlation with neural field
        # Check against all fields first, but prefer non-concatenated
        primary_field = self._find_correlated_field(config.neural_field, text_fields)
        if not primary_field and all_fields:
            # Fallback: check if correlated field was in concatenated list
            correlated = self._find_correlated_field(config.neural_field, all_fields)
            if correlated and correlated in config.concatenated_fields:
                # The correlated field is concatenated - warn and use its components
                config.warnings.append(
                    f"Primary correlated field '{correlated}' is a concatenated field. "
                    f"Using its component fields instead."
                )
        
        # Strategy 2: Find title field by pattern
        title_field = self._find_field_by_pattern(text_fields, self.TITLE_PATTERNS)
        
        # Strategy 3: Find body field by pattern
        body_field = self._find_field_by_pattern(text_fields, self.BODY_PATTERNS)
        
        # Set title field if found
        if title_field:
            config.title_field = title_field
            config.title_confidence = Confidence.MEDIUM
        
        # Set body field if found
        if body_field and body_field != title_field:
            config.body_field = body_field
            config.body_confidence = Confidence.MEDIUM
        
        # Build lexical fields list with priorities
        if primary_field:
            config.lexical_fields.append(primary_field)
            config.lexical_confidence[primary_field] = Confidence.HIGH
            
            # Add title if different from primary
            if title_field and title_field != primary_field:
                config.lexical_fields.append(title_field)
                config.lexical_confidence[title_field] = Confidence.MEDIUM
            
            # Add body if different from primary
            if body_field and body_field != primary_field and body_field != title_field:
                config.lexical_fields.append(body_field)
                config.lexical_confidence[body_field] = Confidence.MEDIUM
        else:
            # No primary correlation - use title + body
            if title_field:
                config.lexical_fields.append(title_field)
                config.lexical_confidence[title_field] = Confidence.MEDIUM
            
            if body_field and body_field != title_field:
                config.lexical_fields.append(body_field)
                config.lexical_confidence[body_field] = Confidence.MEDIUM
            
            # Fallback: add first text field
            if not config.lexical_fields and text_fields:
                config.lexical_fields.append(text_fields[0])
                config.lexical_confidence[text_fields[0]] = Confidence.LOW
                config.warnings.append(
                    f"Could not determine primary lexical field. Using '{text_fields[0]}'."
                )
    
    def _find_correlated_field(
        self, 
        neural_field: Optional[str], 
        text_fields: List[str]
    ) -> Optional[str]:
        """
        Find text field with naming correlation to neural field.
        
        Example: "passage_embedding" -> "passage_text"
        """
        if not neural_field:
            return None
        
        # Extract prefix from neural field name
        # passage_embedding -> passage
        # content_vector -> content
        prefixes = []
        
        # Try common neural suffixes
        for suffix in ["_embedding", "_vector", "_dense", "_knn", "embedding", "vector"]:
            if neural_field.endswith(suffix):
                prefix = neural_field[:-len(suffix)].rstrip("_")
                if prefix:
                    prefixes.append(prefix)
        
        # Also try the first part if underscore-separated
        if "_" in neural_field:
            prefixes.append(neural_field.split("_")[0])
        
        # Look for matching text fields
        for prefix in prefixes:
            for text_field in text_fields:
                # Check if text field starts with same prefix
                if text_field.startswith(prefix + "_") or text_field == prefix + "_text":
                    return text_field
                # Check for exact match like "passage" vs "passage_text"
                if text_field == prefix:
                    return text_field
        
        return None
    
    def _find_field_by_pattern(
        self, 
        fields: List[str], 
        patterns: List[str]
    ) -> Optional[str]:
        """Find first field matching any of the patterns."""
        for field_name in fields:
            for pattern in patterns:
                if re.match(pattern, field_name, re.IGNORECASE):
                    return field_name
        return None
    
    def _compute_boosts(self, config: DetectedHybridConfig):
        """Compute suggested boost values for lexical fields."""
        # Default boost strategy:
        # - Title fields get boost of 2.0 (they're usually more important for relevance)
        # - Body fields get boost of 1.0 (default)
        # - Correlated primary fields get boost of 1.0
        
        for field_name in config.lexical_fields:
            if field_name == config.title_field:
                config.suggested_boosts[field_name] = 2.0
            else:
                config.suggested_boosts[field_name] = 1.0


def print_detection_report(config: DetectedHybridConfig, index_name: str):
    """Print a formatted detection report."""
    print("\n" + "="*70)
    print(f"HYBRID FIELD AUTO-DETECTION REPORT: {index_name}")
    print("="*70)
    
    # Neural field
    print("\n[1] NEURAL FIELD (knn_vector)")
    print("-"*50)
    if config.neural_field:
        print(f"    Field: {config.neural_field}")
        print(f"    Confidence: {config.neural_confidence.value}")
    else:
        print("    NOT DETECTED")
    
    # Lexical fields
    print("\n[2] LEXICAL FIELDS (text)")
    print("-"*50)
    if config.lexical_fields:
        for field_name in config.lexical_fields:
            conf = config.lexical_confidence.get(field_name, Confidence.NONE)
            boost = config.suggested_boosts.get(field_name, 1.0)
            boost_str = f" (boost: {boost})" if boost != 1.0 else ""
            print(f"    • {field_name} [{conf.value}]{boost_str}")
    else:
        print("    NOT DETECTED")
    
    # Title/Body breakdown
    print("\n[3] SEMANTIC BREAKDOWN")
    print("-"*50)
    if config.title_field:
        print(f"    Title field: {config.title_field} [{config.title_confidence.value}]")
    else:
        print("    Title field: NOT DETECTED")
    
    if config.body_field:
        print(f"    Body field: {config.body_field} [{config.body_confidence.value}]")
    else:
        print("    Body field: NOT DETECTED")
    
    # Warnings
    if config.warnings:
        print("\n[4] WARNINGS")
        print("-"*50)
        for warning in config.warnings:
            print(f"    ⚠ {warning}")
    
    # Generated query preview
    if config.neural_field and config.lexical_fields:
        print("\n[5] GENERATED HYBRID QUERY PREVIEW")
        print("-"*50)
        try:
            sample_query = config.generate_hybrid_query(
                query_text="<QUERY_TEXT>",
                model_id="<MODEL_ID>",
                size=100
            )
            print(json.dumps(sample_query, indent=2))
        except Exception as e:
            print(f"    Error generating query: {e}")
    
    print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(
        description="Auto-detect hybrid search field configuration"
    )
    parser.add_argument("--host", required=True, help="OpenSearch host")
    parser.add_argument("--port", type=int, default=80, help="OpenSearch port")
    parser.add_argument("--index", required=True, help="Index name")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    detector = HybridFieldDetector(args.host, args.port)
    config = detector.detect(args.index)
    
    if args.json:
        print(json.dumps(config.to_dict(), indent=2))
    else:
        print_detection_report(config, args.index)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
