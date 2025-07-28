# ESCI Dataset Setup

The Amazon ESCI (Shopping Queries) dataset requires special setup due to its parquet file format. This guide covers downloading, ingesting, and evaluating ESCI data for dynamic hybrid search.

## Quick Start

1. **Download ESCI data** (see below)
2. **Setup OpenSearch** with neural models
3. **Ingest data** using our ESCI ingestion script
4. **Run evaluation** with dynamic hybrid search

## Download ESCI Data

**Note**: Due to network restrictions, ESCI dataset files must be downloaded manually.

### Option 1: Small Version (Recommended for Testing)

Download these files manually from your browser:
- Products (small): https://esci-data.s3.amazonaws.com/esci-data/shopping_queries_dataset_products_us_small.parquet
- Examples (small): https://esci-data.s3.amazonaws.com/esci-data/shopping_queries_dataset_examples_us_small.parquet

### Option 2: Full Version (Production)

Download these files manually from GitHub:
- Products (full): https://github.com/amazon-science/esci-data/raw/main/shopping_queries_dataset/shopping_queries_dataset_products.parquet
- Examples (full): https://github.com/amazon-science/esci-data/raw/main/shopping_queries_dataset/shopping_queries_dataset_examples.parquet

### Copy Files to Server

After downloading to your local machine, create the data directory and copy files:

```bash
# On remote server, create directory
mkdir -p /path/to/your/project/esci_data

# From your local machine, copy the files
scp /local/path/shopping_queries_dataset_*.parquet remote-host:/path/to/your/project/esci_data/

# Example for specific hosts:
scp /Users/user/dev/ir/info-retrieval-test/esci_data/*.parquet \
    dev-dsk:/home/user/dev/ir/info-retrieval-test/esci_data/
```

## Setup OpenSearch

1. **Start OpenSearch and setup ML models**:
   ```bash
   ./setup_opensearch_for_poc.sh
   ```
   
   Note the **model ID** from the output - you'll need it for ingestion.

## Data Ingestion

### Using the ESCI Ingestion Script (Recommended)

Our optimized script provides better field mapping and error handling:

```bash
# Basic ingestion with small dataset
python esci_ingestion.py -m YOUR_MODEL_ID -d esci_data

# Full dataset ingestion
python esci_ingestion.py -m YOUR_MODEL_ID -d esci_data --full-dataset

# Test search functionality only
python esci_ingestion.py -m YOUR_MODEL_ID --test-only

# Limit documents for testing
python esci_ingestion.py -m YOUR_MODEL_ID -d esci_data -n 5000
```

#### Script Options:
- `-m, --model-id`: ML model ID (required)
- `-d, --data-folder`: Path to ESCI data folder (default: esci_data)
- `-i, --index`: Index name (default: esci-products)
- `-h, --host`: OpenSearch host (default: localhost)
- `-p, --port`: OpenSearch port (default: 9200)
- `-t, --test-only`: Only test search, don't ingest data  
- `-f, --full-dataset`: Use full dataset instead of small version
- `-n, --max-docs`: Maximum documents to ingest
- `--help`: Show detailed help

### Alternative: Using BEIR Data Ingestor

```bash
# Standard BEIR approach (requires copying data to expected location)
python -m beir.hybrid.data_ingestor \
    --dataset esci \
    --index esci-products \
    --model sentence-transformers/all-MiniLM-L6-v2
```

## Verify Ingestion

Check that data was ingested correctly:

```bash
# Check document count
curl "localhost:9200/esci-products/_count?pretty"

# Test search functionality
python esci_ingestion.py -m YOUR_MODEL_ID --test-only
```

## Field Mapping

The ESCI ingestion script uses the following field structure:

- **`product_title`**: Main text field for BM25 search
- **`title_embedding`**: Vector field for neural search (auto-generated)
- **`product_id`**: Unique product identifier
- **`product_brand`, `product_color`, `product_description`**: Additional product attributes

## Running ESCI Evaluation

Once data is ingested, run dynamic hybrid search evaluation:

```bash
./run_esci_exhaustive_comparison.sh 500
```

## Troubleshooting

### Common Issues:

1. **No documents ingested**: Check that parquet files are in the correct folder
2. **Timeout errors**: Ensure OpenSearch has sufficient memory for embeddings
3. **Search returns 0 results**: Verify index exists and has documents

### Debug Commands:

```bash
# Check index exists
curl "localhost:9200/_cat/indices?v"

# Check index mapping
curl "localhost:9200/esci-products/_mapping?pretty"

# Test sample query
curl -X POST "localhost:9200/esci-products/_search" -H 'Content-Type: application/json' -d '{
  "query": {"match": {"product_title": "Electronics"}},
  "size": 3
}'
```

## Dataset Information

- **Small version**: ~1,000 products (for quick testing)
- **Full version**: ~100,000+ products (production evaluation)
- **Format**: Parquet files with product catalog and query examples
- **Fields**: product_title, product_brand, product_color, product_description, etc.
- **Query types**: E (Exact), S (Substitute), C (Complement), I (Irrelevant)

## Important Notes

- ESCI uses specialized field mappings optimized for e-commerce search
- The ingestion script automatically handles parquet file parsing
- Neural embeddings are generated automatically via ML pipeline
- Supports both small-scale testing and full dataset evaluation
