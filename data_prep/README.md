# Query and Judgment Extraction Scripts for OpenSearch Relevance Workbench

This folder contains Python scripts to extract and prepare queries and relevance judgments from various Information Retrieval (IR) datasets for use with OpenSearch's Search Relevance Workbench and hybrid search optimization experiments.

## Purpose
These scripts convert dataset-specific formats into the standardized JSON format required by OpenSearch's Search Relevance Workbench, enabling systematic evaluation of search quality metrics (NDCG, MAP, Precision@k).

## Datasets Supported

### BEIR Benchmark Datasets
- **Arguana** - Argument retrieval dataset
- **Climate-FEVER** - Climate change fact verification
- **CQADupStack** - Stack Exchange community Q&A (with Android subdomain support)
- **DBPedia** - Entity search from Wikipedia/DBPedia
- **FEVER** - Fact extraction and verification
- **NQ (Natural Questions)** - Google's question answering dataset
- **Quora** - Duplicate question detection
- **Touche2020** - TREC conversational assistance track

### E-commerce Dataset
- **ESCI** - Amazon shopping queries dataset

### Entity Linking Datasets
- **ACE2004** - Named entity recognition and linking
- **AQUAINT** - News corpus for entity linking
- **KORE50** - Entity linking benchmark
- **MSNBC** - News articles for entity disambiguation

## Script Organization

Most datasets have separate scripts for:
- `extract_[dataset]_queries.py` - Extracts search queries
- `extract_[dataset]_judgments.py` - Extracts relevance judgments/ratings

Some datasets have combined scripts:
- `extract_[dataset]_queries_judgments.py` - Extracts both queries and judgments

Special format scripts:
- `extract_[dataset]_opensearch_format.py` - Additional OpenSearch-specific formatting

## Output Format

All scripts generate JSON files compatible with OpenSearch Search Relevance Workbench:

### Queries Format
```json
{
  "name": "dataset_queries",
  "description": "Description of dataset",
  "querySetQueries": [
    {"queryText": "search query text"},
    ...
  ]
}
```

### Judgments Format
```json
{
  "name": "dataset_judgments",
  "description": "Description of judgments",
  "judgmentRatings": [
    {
      "query": "search query text",
      "ratings": [
        {"docId": "document_id", "rating": "relevance_score"},
        ...
      ]
    }
  ]
}
```

## Usage

Run any script from the project root directory:
```bash
python data_prep/extract_[dataset]_queries.py
python data_prep/extract_[dataset]_judgments.py
```

Output files are saved to the `results/` directory.

## Special Instructions

### CQADupStack Dataset
CQADupStack has multiple subdomains (android, gaming, etc.). See `CQADUPSTACK_USAGE_GUIDE.md` in the project root for specific instructions on handling this multi-domain dataset.

## Dependencies
- Python 3.x
- JSON library (standard)
- Dataset files should be extracted in `datasets/` directory
