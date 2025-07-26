"""
Feature extraction for dynamic hybrid search weight optimization - TUNED VERSION.
Includes FiQA-specific optimizations and more aggressive neural preferences.
"""

import re
from typing import Dict, List
from enum import Enum
import numpy as np


class Domain(Enum):
    ECOMMERCE = "ecommerce"
    QA_CONVERSATIONAL = "qa_conversational"
    MEDICAL_SCIENTIFIC = "medical_scientific"
    LEGAL = "legal"
    TECHNICAL = "technical"
    GENERAL = "general"
    FINANCIAL = "financial"  # New domain for FiQA


class QueryFeatureExtractor:
    """Basic query feature extraction"""
    
    def extract_features(self, query_text: str) -> Dict[str, float]:
        """Extract basic features from query text"""
        features = {}
        
        # Basic features
        features['query_length'] = len(query_text)
        features['token_count'] = len(query_text.split())
        features['has_numbers'] = 1.0 if bool(re.search(r'\d', query_text)) else 0.0
        features['has_special_chars'] = 1.0 if bool(re.search(r'[^a-zA-Z0-9\s]', query_text)) else 0.0
        
        return features


class DomainAwareFeatureExtractor(QueryFeatureExtractor):
    """Domain-specific feature extraction with FiQA optimizations"""
    
    # Pattern definitions
    CURRENCY_PATTERN = re.compile(r'[$€£¥₹]\d+')
    SIZE_PATTERN = re.compile(r'\b(XS|S|M|L|XL|XXL|\d+GB|\d+TB|\d+MB)\b', re.IGNORECASE)
    SKU_PATTERN = re.compile(r'\b[A-Z0-9]{6,}\b')
    
    QUESTION_PATTERN = re.compile(r'^(what|how|why|when|where|who|which|can|should|would|could)\b', re.IGNORECASE)
    
    MEDICAL_ACRONYM_PATTERN = re.compile(r'\b[A-Z]{2,5}\b')
    DOSAGE_PATTERN = re.compile(r'\b\d+\s?(mg|ml|mcg|iu|units?)\b', re.IGNORECASE)
    
    CITATION_PATTERN = re.compile(r'\b\d+\s+[A-Z]\.\s*\d+[a-z]?\s+\d+\b')
    SECTION_PATTERN = re.compile(r'§\s*\d+|section\s+\d+', re.IGNORECASE)
    
    # Financial patterns (NEW)
    TICKER_PATTERN = re.compile(r'\b[A-Z]{2,5}\b')
    PRICE_PATTERN = re.compile(r'\$[\d,]+\.?\d*|\d+\.?\d*%')
    
    def __init__(self, domain: Domain = Domain.GENERAL):
        self.domain = domain
    
    def extract_features(self, query_text: str) -> Dict[str, float]:
        """Extract domain-specific features"""
        # Start with basic features
        features = super().extract_features(query_text)
        
        # Add domain-specific features
        if self.domain == Domain.ECOMMERCE:
            features.update(self._extract_ecommerce_features(query_text))
        elif self.domain == Domain.QA_CONVERSATIONAL:
            features.update(self._extract_qa_features(query_text))
        elif self.domain == Domain.MEDICAL_SCIENTIFIC:
            features.update(self._extract_medical_features(query_text))
        elif self.domain == Domain.LEGAL:
            features.update(self._extract_legal_features(query_text))
        elif self.domain == Domain.TECHNICAL:
            features.update(self._extract_technical_features(query_text))
        elif self.domain == Domain.FINANCIAL:
            features.update(self._extract_financial_features(query_text))
        
        return features
    
    def _extract_ecommerce_features(self, text: str) -> Dict[str, float]:
        features = {}
        features['has_currency'] = 1.0 if self.CURRENCY_PATTERN.search(text) else 0.0
        features['has_size_terms'] = 1.0 if self.SIZE_PATTERN.search(text) else 0.0
        features['has_sku_pattern'] = 1.0 if self.SKU_PATTERN.search(text) else 0.0
        features['contains_brand_keywords'] = 1.0 if self._contains_brand_keywords(text) else 0.0
        features['is_product_search'] = 1.0 if self._is_product_search(text) else 0.0
        return features
    
    def _extract_qa_features(self, text: str) -> Dict[str, float]:
        features = {}
        features['is_question'] = 1.0 if text.strip().endswith('?') else 0.0
        features['has_question_word'] = 1.0 if self.QUESTION_PATTERN.match(text) else 0.0
        features['sentence_count'] = float(len(re.split(r'[.!?]', text)))
        features['conversational_score'] = self._calculate_conversational_score(text)
        features['subjectivity_indicator'] = 1.0 if self._has_subjective_terms(text) else 0.0
        return features
    
    def _extract_medical_features(self, text: str) -> Dict[str, float]:
        features = {}
        features['medical_acronym_count'] = float(len(self.MEDICAL_ACRONYM_PATTERN.findall(text)))
        features['has_dosage'] = 1.0 if self.DOSAGE_PATTERN.search(text) else 0.0
        features['has_symptom_keywords'] = 1.0 if self._has_symptom_keywords(text) else 0.0
        features['has_treatment_keywords'] = 1.0 if self._has_treatment_keywords(text) else 0.0
        features['clinical_term_density'] = self._calculate_clinical_term_density(text)
        return features
    
    def _extract_legal_features(self, text: str) -> Dict[str, float]:
        features = {}
        features['has_citation'] = 1.0 if self.CITATION_PATTERN.search(text) else 0.0
        features['has_section_ref'] = 1.0 if self.SECTION_PATTERN.search(text) else 0.0
        features['legal_entity_count'] = float(self._count_legal_entities(text))
        features['has_date_range'] = 1.0 if self._has_date_range(text) else 0.0
        features['formality_score'] = self._calculate_formality_score(text)
        return features
    
    def _extract_technical_features(self, text: str) -> Dict[str, float]:
        features = {}
        features['has_code_pattern'] = 1.0 if self._has_code_pattern(text) else 0.0
        features['technical_term_ratio'] = self._calculate_technical_term_ratio(text)
        features['has_version_number'] = 1.0 if self._has_version_number(text) else 0.0
        features['camelcase_count'] = float(self._count_camelcase_words(text))
        features['has_error_pattern'] = 1.0 if self._has_error_pattern(text) else 0.0
        return features
    
    def _extract_financial_features(self, text: str) -> Dict[str, float]:
        """Extract FiQA-specific financial features"""
        features = {}
        
        # Detect stock tickers (but exclude common words)
        text_upper = text.upper()
        common_words = {'I', 'A', 'THE', 'AND', 'OR', 'IF', 'IN', 'ON', 'AT', 'TO', 'FOR'}
        potential_tickers = self.TICKER_PATTERN.findall(text)
        tickers = [t for t in potential_tickers if t not in common_words]
        
        features['has_ticker'] = 1.0 if tickers else 0.0
        features['ticker_count'] = float(len(tickers))
        
        # Detect financial numbers (prices, percentages)
        features['has_financial_numbers'] = 1.0 if self.PRICE_PATTERN.search(text) else 0.0
        
        # Detect financial keywords
        fin_keywords = ['invest', 'stock', '401k', 'ira', 'retirement', 'dividend', 
                       'portfolio', 'market', 'trading', 'fund', 'bond', 'etf',
                       'mutual fund', 'index fund', 'capital gains', 'tax']
        text_lower = text.lower()
        features['financial_keyword_count'] = float(sum(1 for kw in fin_keywords if kw in text_lower))
        
        # Detect question intent
        advice_words = ['should', 'best', 'recommend', 'advice', 'strategy', 'how to']
        features['seeks_advice'] = 1.0 if any(word in text_lower for word in advice_words) else 0.0
        
        # Detect specific vs general queries
        # Note: token_count is not available here, we'll calculate it
        token_count = len(text.split())
        features['is_specific_lookup'] = 1.0 if (features['ticker_count'] > 0 and token_count <= 5) else 0.0
        
        return features
    
    # Helper methods (unchanged from original)
    def _contains_brand_keywords(self, text: str) -> bool:
        lower = text.lower()
        return any(brand in lower for brand in ['apple', 'samsung', 'nike', 'amazon', 'brand'])
    
    def _is_product_search(self, text: str) -> bool:
        lower = text.lower()
        return any(term in lower for term in ['buy', 'price', 'cheap', 'best', 'review'])
    
    def _calculate_conversational_score(self, text: str) -> float:
        lower = text.lower()
        score = 0.0
        if any(pron in lower for pron in ['i ', 'my ', 'me ']):
            score += 0.3
        if any(word in lower for word in ['please', 'thanks']):
            score += 0.2
        if any(word in lower for word in ['anyone', 'someone']):
            score += 0.2
        if any(word in lower for word in ['help', 'need']):
            score += 0.3
        return min(1.0, score)
    
    def _has_subjective_terms(self, text: str) -> bool:
        lower = text.lower()
        return any(term in lower for term in ['think', 'believe', 'opinion', 'feel', 'seems'])
    
    def _has_symptom_keywords(self, text: str) -> bool:
        lower = text.lower()
        return any(term in lower for term in ['pain', 'symptom', 'fever', 'ache', 'discomfort'])
    
    def _has_treatment_keywords(self, text: str) -> bool:
        lower = text.lower()
        return any(term in lower for term in ['treatment', 'therapy', 'medication', 'drug', 'cure'])
    
    def _calculate_clinical_term_density(self, text: str) -> float:
        clinical_terms = ['diagnosis', 'prognosis', 'etiology', 'pathogenesis', 
                         'clinical', 'patient', 'therapeutic', 'intervention']
        lower = text.lower()
        count = sum(1 for term in clinical_terms if term in lower)
        return count / max(1, len(text.split()))
    
    def _count_legal_entities(self, text: str) -> int:
        lower = text.lower()
        entities = ['plaintiff', 'defendant', 'court', 'judge', 'attorney', 'counsel']
        return sum(1 for entity in entities if entity in lower)
    
    def _has_date_range(self, text: str) -> bool:
        return bool(re.search(r'\b\d{4}\s*[-–]\s*\d{4}\b', text) or 
                   ('between' in text.lower() and re.search(r'\d{4}', text)))
    
    def _calculate_formality_score(self, text: str) -> float:
        lower = text.lower()
        score = 0.5  # baseline
        
        # Formal indicators
        if any(term in lower for term in ['pursuant', 'whereas', 'herein']):
            score += 0.2
        if any(term in lower for term in ['shall', 'thereof']):
            score += 0.1
        
        # Informal indicators
        if any(term in lower for term in ['gonna', 'wanna', '!']):
            score -= 0.2
        
        return max(0.0, min(1.0, score))
    
    def _has_code_pattern(self, text: str) -> bool:
        return any(pattern in text for pattern in ['()', '{}', '[]', '->', '::'])
    
    def _calculate_technical_term_ratio(self, text: str) -> float:
        tech_terms = ['api', 'sdk', 'framework', 'library', 'function', 'method',
                     'class', 'interface', 'debug', 'compile', 'runtime']
        lower = text.lower()
        count = sum(1 for term in tech_terms if term in lower)
        return count / max(1, len(text.split()))
    
    def _has_version_number(self, text: str) -> bool:
        return bool(re.search(r'\bv?\d+\.\d+(\.\d+)?\b', text))
    
    def _count_camelcase_words(self, text: str) -> int:
        camelcase_pattern = re.compile(r'\b[a-z]+[A-Z][a-zA-Z]*\b')
        return len(camelcase_pattern.findall(text))
    
    def _has_error_pattern(self, text: str) -> bool:
        lower = text.lower()
        return (any(term in lower for term in ['error', 'exception', 'failed']) or
                bool(re.search(r'\b\d{3,4}\b', text)))  # HTTP error codes


def get_domain_for_dataset(dataset_name: str) -> Domain:
    """Map BEIR datasets to appropriate domains - TUNED FOR FIQA"""
    domain_mapping = {
        'esci': Domain.ECOMMERCE,  # Added ESCI mapping
        'trec-covid': Domain.MEDICAL_SCIENTIFIC,
        'bioasq': Domain.MEDICAL_SCIENTIFIC,
        'nfcorpus': Domain.MEDICAL_SCIENTIFIC,
        'scifact': Domain.MEDICAL_SCIENTIFIC,
        'fiqa': Domain.FINANCIAL,  # Changed from GENERAL to FINANCIAL
        'arguana': Domain.GENERAL,
        'webis-touche2020': Domain.GENERAL,
        'quora': Domain.QA_CONVERSATIONAL,
        'cqadupstack': Domain.TECHNICAL,
        'msmarco': Domain.GENERAL,
        'nq': Domain.QA_CONVERSATIONAL,
        'hotpotqa': Domain.QA_CONVERSATIONAL,
        'fever': Domain.GENERAL,
        'climate-fever': Domain.MEDICAL_SCIENTIFIC,
        'dbpedia-entity': Domain.GENERAL,
        'scidocs': Domain.MEDICAL_SCIENTIFIC,
        'robust04': Domain.GENERAL,
        'trec-news': Domain.GENERAL,
        'signal1m': Domain.GENERAL
    }
    return domain_mapping.get(dataset_name, Domain.GENERAL)
