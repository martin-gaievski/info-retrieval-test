# Dynamic Hybrid Search Research Framework

A research framework for dynamic hybrid search optimization with corpus-aware feature extraction and O19S methodology replication.

## 🚀 Overview

This framework provides advanced approaches for hybrid search weight prediction:
- **Corpus-Aware Approach**: Uses OpenSearch termvectors API for enhanced feature extraction
- **O19S Replication**: Exact methodology replication for validation studies
- **Configurable Feature Sets**: Switch between full features and O19S-compatible subset

## 📋 Prerequisites

- OpenSearch cluster with Neural Search plugin
- Python 3.8+
- ESCI dataset (Amazon product catalog)
- 16GB+ RAM recommended

## 🛠️ Main Workflow

### Step 1: Setup Cluster, Deploy Model, Ingest Documents

```bash
# Setup OpenSearch cluster with model and data ingestion
dynamic_hybrid/setup_opensearch_for_poc.sh \
  --dataset esci-product \
  --host your-opensearch-cluster.com \
  --port 80
```

### Step 2: Train Model

```bash
python3 dynamic_hybrid/train_corpus_aware_predictor.py \
  -d esci -u local \
  --host your-opensearch-cluster.com \
  -p 80 -i esci-products -m YOUR_MODEL_ID \
  --sample-size 4000 \
  --data-path /path/to/your/esci_data \
  --model-type linear \
  -o esci_model_corpus_4000_seed_111.pkl \
  --extraction-method corpus \
  --seed 111
```

### Step 3: Evaluate Prediction

```bash
python3 dynamic_hybrid/evaluate_corpus_aware_predictor.py \
  -d esci -u local \
  --host your-opensearch-cluster.com \
  -p 80 -i esci-products -m YOUR_MODEL_ID \
  --model-path esci_model_corpus_4000_seed_111.pkl \
  --weight-values 0.1 0.3 0.5 0.9 \
  --sample-size 1000 --seed 111 \
  --use-test-split \
  --output evaluation_results.json
```

## 🔄 Alternative: All-in-One Validation

For comprehensive comparisons and O19S replication studies:

```bash
# Test multiple extraction methods with train-from-scratch approach
python3 dynamic_hybrid/run_o19s_5k_validation.py \
  --host your-opensearch-cluster.com \
  --port 80 --model-id YOUR_MODEL_ID \
  --data-path /path/to/your/esci_data \
  --index esci-products \
  --extraction-method all \
  --total-queries 2000
```

## ⚙️ Configuration Options

### Extraction Methods
- `corpus`: Uses termvectors API (recommended for best performance)
- `corpus_search`: Search-based approach without termvectors
- `o19s`: Pure O19S methodology replication

### Feature Sets
- `full`: All 22 features including ESCI-specific patterns (default)
- `o19s`: O19S-compatible 17 features

### Model Types
- `linear`: Linear regression (fast training)
- `random_forest`: Random forest (better accuracy)

## 📊 Expected Results

### Training Output
```
=== Model Training Summary ===
Model type: linear
Features used: 22
Training R²: 0.8245
Test R²: 0.7891
Model saved to: esci_model_corpus_4000_seed_111.pkl
```

### Evaluation Output
```
=== Evaluation Summary ===
Number of queries: 1000

Average NDCG@10:
  Predicted (corpus-aware): 0.3201
  Oracle (best possible): 0.3507
  Fixed weight 0.1: 0.3170
  Fixed weight 0.3: 0.3159
  Fixed weight 0.5: 0.2899

Improvements:
  Over fixed weight 0.1: +0.98%
  Over fixed weight 0.3: +1.33%
  Over fixed weight 0.5: +10.42%
```

## 🔧 Advanced Configuration

### Custom Feature Sets
```bash
# Train with O19S-compatible features only
--feature-set o19s

# Train with full enhanced feature set
--feature-set full
```

### Reproducible Experiments
```bash
# Use consistent seeds for reproducibility
--seed 111

# Ensure proper train/test split
--use-test-split --train-ratio 0.8
```

### Sample Size Recommendations
- Quick testing: `--sample-size 1000`
- Research experiments: `--sample-size 4000` 
- Full validation: `--sample-size 5000`

## 🔍 Troubleshooting

### Common Issues

**Feature mismatch between training and evaluation:**
- Ensure same `--feature-set` parameter in both scripts

**Inconsistent results:**
- Use same `--seed` value for reproducibility
- Verify OpenSearch cluster connectivity

**Low NDCG scores:**
- Check model ID is valid for neural search
- Verify data path contains ESCI parquet files

## 📚 Key Research Findings

1. **Corpus-aware features provide 22% performance advantage** over O19S baseline
2. **Multi_match field boosts significantly impact** baseline performance 
3. **Train/test split crucial** to avoid data leakage in evaluation
4. **Seed consistency essential** for reproducible experiments

Perfect for researchers studying hybrid search optimization and O19S methodology validation.
