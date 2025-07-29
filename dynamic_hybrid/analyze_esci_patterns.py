"""
Analyze ESCI dataset to extract data-driven features and patterns.
This script:
1. Analyzes queries with different optimal weights
2. Extracts common patterns from the data
3. Builds vocabulary from actual products
4. Creates data-driven features
"""

import os
import re
import json
import pandas as pd
import numpy as np
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import LatentDirichletAllocation
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk

# Download NLTK data if needed
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')


class ESCIDataAnalyzer:
    """Analyze ESCI data to extract patterns and features"""
    
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.brand_vocabulary = set()
        self.category_vocabulary = set()
        self.product_terms = set()
        self.query_patterns = defaultdict(list)
        
    def analyze_training_data(self, training_csv_path: str) -> Dict:
        """Analyze training data to find patterns for different weight classes"""
        print(f"Loading training data from {training_csv_path}")
        df = pd.read_csv(training_csv_path)
        
        # Group queries by their optimal weights
        weight_groups = {
            'lexical_heavy': df[df['best_neural_weight'] <= 0.2],  # 0.9/0.1, 0.8/0.2
            'lexical_moderate': df[(df['best_neural_weight'] > 0.2) & (df['best_neural_weight'] <= 0.4)],  # 0.7/0.3, 0.6/0.4
            'balanced': df[(df['best_neural_weight'] > 0.4) & (df['best_neural_weight'] <= 0.6)],  # 0.5/0.5
            'neural_moderate': df[(df['best_neural_weight'] > 0.6) & (df['best_neural_weight'] <= 0.8)],  # 0.4/0.6, 0.3/0.7
            'neural_heavy': df[df['best_neural_weight'] > 0.8]  # 0.2/0.8, 0.1/0.9
        }
        
        patterns = {}
        for group_name, group_df in weight_groups.items():
            if len(group_df) > 0:
                print(f"\nAnalyzing {group_name} group ({len(group_df)} queries)")
                patterns[group_name] = self._analyze_query_group(group_df)
        
        return patterns
    
    def _analyze_query_group(self, group_df: pd.DataFrame) -> Dict:
        """Analyze a group of queries with similar optimal weights"""
        queries = group_df['query_text'].tolist()
        
        # Extract common terms using TF-IDF
        tfidf = TfidfVectorizer(
            max_features=50,
            ngram_range=(1, 3),
            stop_words='english'
        )
        
        try:
            tfidf_matrix = tfidf.fit_transform(queries)
            feature_names = tfidf.get_feature_names_out()
            
            # Get top terms for this group
            tfidf_scores = tfidf_matrix.sum(axis=0).A1
            top_indices = tfidf_scores.argsort()[-20:][::-1]
            top_terms = [feature_names[i] for i in top_indices]
        except:
            top_terms = []
        
        # Extract common patterns
        patterns = {
            'top_terms': top_terms,
            'avg_length': np.mean([len(q.split()) for q in queries]),
            'common_start_words': self._get_common_start_words(queries),
            'common_end_words': self._get_common_end_words(queries),
            'has_numbers_ratio': np.mean([bool(re.search(r'\d', q)) for q in queries]),
            'query_templates': self._extract_query_templates(queries[:100])  # Sample for efficiency
        }
        
        return patterns
    
    def _get_common_start_words(self, queries: List[str], top_n: int = 10) -> List[Tuple[str, int]]:
        """Get most common starting words/phrases"""
        start_words = []
        for query in queries:
            words = query.lower().split()
            if words:
                start_words.append(words[0])
                if len(words) > 1:
                    start_words.append(' '.join(words[:2]))
        
        return Counter(start_words).most_common(top_n)
    
    def _get_common_end_words(self, queries: List[str], top_n: int = 10) -> List[Tuple[str, int]]:
        """Get most common ending words"""
        end_words = []
        for query in queries:
            words = query.lower().split()
            if words:
                end_words.append(words[-1])
        
        return Counter(end_words).most_common(top_n)
    
    def _extract_query_templates(self, queries: List[str]) -> List[str]:
        """Extract common query templates by replacing specific terms with placeholders"""
        templates = []
        
        for query in queries:
            # Replace numbers with <NUM>
            template = re.sub(r'\b\d+\b', '<NUM>', query)
            # Replace potential brand/model names with <BRAND>
            template = re.sub(r'\b[A-Z][A-Za-z]+\b', '<BRAND>', template)
            # Replace prices with <PRICE>
            template = re.sub(r'\$\d+', '<PRICE>', template)
            
            templates.append(template.lower())
        
        # Get most common templates
        template_counts = Counter(templates)
        return [t for t, count in template_counts.most_common(10) if count > 1]
    
    def analyze_product_corpus(self, corpus_path: str) -> Dict:
        """Analyze product corpus to extract vocabulary"""
        print("\nAnalyzing product corpus...")
        
        # Load ESCI product data
        import pandas as pd
        products_df = pd.read_parquet(corpus_path)
        
        # Extract brands from product titles
        brands = []
        categories = []
        all_terms = []
        
        for _, row in products_df.iterrows():
            title = str(row.get('product_title', ''))
            
            # Extract potential brand names (capitalized words at start)
            words = title.split()
            if words and words[0][0].isupper():
                brands.append(words[0])
            
            # Extract all terms
            all_terms.extend([w.lower() for w in words if len(w) > 2])
        
        # Get most common brands and terms
        brand_counts = Counter(brands)
        term_counts = Counter(all_terms)
        
        self.brand_vocabulary = set([b for b, c in brand_counts.most_common(100) if c > 5])
        self.product_terms = set([t for t, c in term_counts.most_common(500) if c > 10])
        
        return {
            'top_brands': brand_counts.most_common(20),
            'vocabulary_size': len(self.product_terms),
            'brand_count': len(self.brand_vocabulary)
        }
    
    def create_pattern_based_features(self, patterns: Dict) -> Dict:
        """Create feature extraction rules based on learned patterns"""
        feature_rules = {}
        
        # Lexical-heavy patterns (0.9/0.1)
        if 'lexical_heavy' in patterns:
            lex_patterns = patterns['lexical_heavy']
            feature_rules['lexical_indicators'] = {
                'terms': lex_patterns['top_terms'][:10],
                'start_patterns': [p[0] for p in lex_patterns['common_start_words'][:5]],
                'templates': lex_patterns['query_templates'][:5]
            }
        
        # Neural-heavy patterns (0.1/0.9)
        if 'neural_heavy' in patterns:
            neural_patterns = patterns['neural_heavy']
            feature_rules['neural_indicators'] = {
                'terms': neural_patterns['top_terms'][:10],
                'start_patterns': [p[0] for p in neural_patterns['common_start_words'][:5]],
                'templates': neural_patterns['query_templates'][:5]
            }
        
        return feature_rules
    
    def save_analysis(self, output_path: str, patterns: Dict, corpus_analysis: Dict, feature_rules: Dict):
        """Save analysis results"""
        analysis_data = {
            'patterns': patterns,
            'corpus_analysis': corpus_analysis,
            'feature_rules': feature_rules,
            'brand_vocabulary': list(self.brand_vocabulary),
            'product_terms': list(self.product_terms)[:200]  # Top 200 terms
        }
        
        with open(output_path, 'wb') as f:
            pickle.dump(analysis_data, f)
        
        print(f"\nAnalysis saved to {output_path}")
        
        # Also save human-readable summary
        summary_path = output_path.replace('.pkl', '_summary.txt')
        with open(summary_path, 'w') as f:
            f.write("ESCI Data Analysis Summary\n")
            f.write("="*50 + "\n\n")
            
            for group_name, group_patterns in patterns.items():
                f.write(f"\n{group_name.upper()} (prefer {group_name})\n")
                f.write("-"*30 + "\n")
                f.write(f"Average query length: {group_patterns['avg_length']:.1f} words\n")
                f.write(f"Has numbers ratio: {group_patterns['has_numbers_ratio']:.2%}\n")
                
                f.write("\nTop terms:\n")
                for term in group_patterns['top_terms'][:10]:
                    f.write(f"  - {term}\n")
                
                f.write("\nCommon start patterns:\n")
                for pattern, count in group_patterns['common_start_words'][:5]:
                    f.write(f"  - '{pattern}' ({count} times)\n")
                
                f.write("\nQuery templates:\n")
                for template in group_patterns['query_templates'][:5]:
                    f.write(f"  - {template}\n")
            
            if corpus_analysis:
                f.write("\n\nCORPUS ANALYSIS\n")
                f.write("-"*30 + "\n")
                f.write(f"Brand vocabulary size: {corpus_analysis.get('brand_count', 0)}\n")
                f.write(f"Product term vocabulary: {corpus_analysis.get('vocabulary_size', 0)}\n")
                
                if 'top_brands' in corpus_analysis:
                    f.write("\nTop brands:\n")
                    for brand, count in corpus_analysis['top_brands'][:10]:
                        f.write(f"  - {brand}: {count}\n")
            else:
                f.write("\n\nCORPUS ANALYSIS\n")
                f.write("-"*30 + "\n")
                f.write("No product corpus analyzed (file not found)\n")
        
        print(f"Summary saved to {summary_path}")


def main():
    """Analyze ESCI data to extract patterns"""
    analyzer = ESCIDataAnalyzer()
    
    # Paths
    training_data_path = "esci_training_data_enhanced.csv"
    corpus_path = "esci_data/shopping_queries_dataset_examples.parquet"
    output_path = "dynamic_hybrid/esci_data_analysis.pkl"
    
    # Check if training data exists
    if not os.path.exists(training_data_path):
        print(f"Training data not found at {training_data_path}")
        print("Please run the training script first to generate training data with optimal weights")
        return
    
    # Analyze training data patterns
    patterns = analyzer.analyze_training_data(training_data_path)
    
    # Analyze product corpus if available
    corpus_analysis = {}
    if os.path.exists(corpus_path):
        corpus_analysis = analyzer.analyze_product_corpus(corpus_path)
    else:
        print(f"Product corpus not found at {corpus_path}")
    
    # Create feature rules based on patterns
    feature_rules = analyzer.create_pattern_based_features(patterns)
    
    # Save analysis
    analyzer.save_analysis(output_path, patterns, corpus_analysis, feature_rules)
    
    print("\nAnalysis complete! Use the extracted patterns to create data-driven features.")


if __name__ == "__main__":
    main()
