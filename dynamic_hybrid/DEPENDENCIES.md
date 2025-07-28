# External Dependencies

This project requires the following files from the BEIR framework:

## Required BEIR modules:
- `beir/hybrid/data_ingestor.py` - For ingesting BEIR datasets into OpenSearch
- `beir/datasets/data_loader.py` - Base data loader functionality
- `beir/datasets/data_loader_esci.py` - ESCI-specific data loader

## Installation:
```bash
pip install beir
```

Or copy the required files from:
https://github.com/beir-cellar/beir

## ESCI Dataset Setup

The Amazon ESCI dataset requires special setup with parquet files.

### Quick Setup:
```bash
# Download ESCI sample data
mkdir -p esci_data
cd esci_data
wget https://esci-data.s3.amazonaws.com/esci-data/shopping_queries_dataset_products_us_small.parquet
wget https://esci-data.s3.amazonaws.com/esci-data/shopping_queries_dataset_examples_us_small.parquet
cd ..

# Setup OpenSearch (note the model_id from output)
./setup_opensearch_for_poc.sh

# Ingest using our optimized ESCI script
python esci_ingestion.py -m YOUR_MODEL_ID -d esci_data
```

### Advanced Options:
```bash
# Use full dataset instead of small version
python esci_ingestion.py -m YOUR_MODEL_ID -d esci_data --full-dataset

# Test search functionality only
python esci_ingestion.py -m YOUR_MODEL_ID --test-only

# Limit documents for testing
python esci_ingestion.py -m YOUR_MODEL_ID -d esci_data -n 5000

# Get help
python esci_ingestion.py --help
```

**Why use the ESCI ingestion script?**
- Proper field mapping (`product_title` → `title_embedding`)
- Handles parquet files natively
- Built-in search functionality testing
- Optimized for e-commerce product data

See **ESCI_SETUP.md** for complete setup instructions and troubleshooting.
