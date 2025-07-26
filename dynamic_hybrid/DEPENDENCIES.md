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
See **ESCI_SETUP.md** for detailed instructions on:
- Downloading ESCI parquet files
- Setting up the correct directory structure
- Ingesting data into OpenSearch
