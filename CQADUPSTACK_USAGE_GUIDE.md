# CQADupStack Usage Guide - Correct Command Patterns

## For Separate Indices (RECOMMENDED)

To use individual indices for each subset, include a dash "-" in the index name:

### Ingest Android subset into its own index:
```bash
python3 test_opensearch_dupstack.py \
  -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip \
  -h localhost \
  -p 9200 \
  -i cqadupstack- \
  -f android \
  -o ingest
```
**Creates index:** `cqadupstack-android`

### Ingest all subsets into separate indices:
```bash
python3 test_opensearch_dupstack.py \
  -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip \
  -h localhost \
  -p 9200 \
  -i cqadupstack- \
  -f all \
  -o ingest
```
**Creates indices:** 
- `cqadupstack-android`
- `cqadupstack-gaming`
- `cqadupstack-physics`
- ... (one for each subset)

## Your Command Analysis

Your current command:
```bash
python3 test_opensearch_dupstack.py -d cqadupstack -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip -h localhost -p 9200 -i cqadupstack -f android -o ingest
```

**Issue:** `-i cqadupstack` (no dash) → Would use single index "cqadupstack"

**Corrected command for separate indices:**
```bash
python3 test_opensearch_dupstack.py \
  -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip \
  -h localhost \
  -p 9200 \
  -i cqadupstack- \
  -f android \
  -o ingest
```

Note: The `-d` parameter is not needed as the dataset name is automatically extracted from the URL.

## How It Works

The script automatically detects the indexing strategy based on the `-i` parameter:

1. **With dash** (`-i cqadupstack-`):
   - Creates subset-specific indices
   - Appends subset name to base index name
   - Result: `cqadupstack-android`, `cqadupstack-gaming`, etc.

2. **Without dash** (`-i cqadupstack`):
   - Uses single index for all subsets
   - All documents go into one index
   - Result: `cqadupstack` (not recommended)

## Complete Examples for Each Subset

```bash
# Android
python3 test_opensearch_dupstack.py -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip -h localhost -p 9200 -i cqadupstack- -f android -o ingest

# Gaming
python3 test_opensearch_dupstack.py -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip -h localhost -p 9200 -i cqadupstack- -f gaming -o ingest

# Physics
python3 test_opensearch_dupstack.py -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip -h localhost -p 9200 -i cqadupstack- -f physics -o ingest

# ... and so on for other subsets
```

## Evaluation with Separate Indices

When evaluating, use the same index pattern:
```bash
python3 test_opensearch_dupstack.py \
  -u https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip \
  -h localhost \
  -p 9200 \
  -i cqadupstack- \
  -f android \
  -m <model_id> \
  -o evaluate
```

This ensures queries search only within their domain-specific index.

## Query and Judgment Extraction for OpenSearch Relevance Workbench

To extract queries and judgments for use with OpenSearch's Search Relevance Workbench, use the scripts in `prep_optimizer_experiment_data/`:

### Extract queries and judgments for all subsets:
```bash
# Extract queries for all CQADupStack subsets
python prep_optimizer_experiment_data/extract_cqadupstack_queries.py

# Extract judgments for all CQADupStack subsets  
python prep_optimizer_experiment_data/extract_cqadupstack_judgments.py
```

### Extract for specific subset (e.g., Android):
```bash
# Extract Android-specific queries
python prep_optimizer_experiment_data/extract_cqadupstack_android_queries.py

# Extract Android-specific judgments
python prep_optimizer_experiment_data/extract_cqadupstack_android_judgments.py
```

### Output Files
The scripts generate JSON files in the `results/` directory:
- `cqadupstack_android_queries.json` - Queries for Android subset
- `cqadupstack_android_judgments.json` - Relevance judgments for Android subset
- `cqadupstack_gaming_queries.json` - Queries for Gaming subset
- `cqadupstack_gaming_judgments.json` - Relevance judgments for Gaming subset
- ... (similar files for other subsets)

### Available Subsets
CQADupStack contains 12 Stack Exchange communities:
- android
- english
- gaming
- gis
- mathematica
- physics
- programmers
- stats
- tex
- unix
- webmasters
- wordpress
