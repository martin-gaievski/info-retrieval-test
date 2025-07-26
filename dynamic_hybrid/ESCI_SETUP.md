# ESCI Dataset Setup

The Amazon ESCI (Shopping Queries) dataset requires special setup due to its parquet file format.

## Download ESCI Data

1. Download the ESCI parquet files from Amazon:
   ```bash
   mkdir -p datasets/esci
   cd datasets/esci
   
   # Download query data
   wget https://github.com/amazon-science/esci-data/raw/main/shopping_queries_dataset/shopping_queries_dataset_examples.parquet
   
   # Download product data
   wget https://github.com/amazon-science/esci-data/raw/main/shopping_queries_dataset/shopping_queries_dataset_products.parquet
   ```

2. Alternative: Use the US small version (recommended for testing):
   ```bash
   # These are smaller files for faster testing
   wget https://esci-data.s3.amazonaws.com/esci-data/shopping_queries_dataset_products_us_small.parquet
   wget https://esci-data.s3.amazonaws.com/esci-data/shopping_queries_dataset_examples_us_small.parquet
   ```

## Directory Structure

After downloading, your directory should look like:
```
datasets/
└── esci/
    ├── shopping_queries_dataset_examples.parquet
    └── shopping_queries_dataset_products.parquet
```

Or for the small version:
```
datasets/
└── esci/
    ├── shopping_queries_dataset_examples_us_small.parquet
    └── shopping_queries_dataset_products_us_small.parquet
```

## Data Ingestion

To ingest ESCI data into OpenSearch:

```bash
# Using the BEIR hybrid data ingestor (requires beir package)
python -m beir.hybrid.data_ingestor \
    --dataset esci \
    --index esci-products \
    --model sentence-transformers/all-MiniLM-L6-v2
```

## Running ESCI Evaluation

Once data is ingested:

```bash
./run_esci_exhaustive_comparison.sh 500
```

## Important Notes

- ESCI uses different field names than standard BEIR datasets
- The `data_loader_esci.py` module handles these differences
- Total dataset size: ~1.8M products (small version: ~100K)
- Requires pandas for parquet file reading
