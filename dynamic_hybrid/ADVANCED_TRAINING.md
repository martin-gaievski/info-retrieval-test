# Advanced Training with Data-Driven Features

This guide covers how to use data-driven feature extraction and advanced model training for improved weight prediction.

## Overview

The advanced training approach uses:
1. **Data-driven feature extraction** - Learns patterns from your actual dataset
2. **Advanced ML models** - Random Forest and XGBoost instead of simple Linear Regression
3. **Feature importance analysis** - Understand which features matter most

## Files Included

- `analyze_esci_patterns.py` - Analyzes your dataset to discover patterns
- `feature_extractor_esci_datadriven.py` - Extracts features based on learned patterns
- `train_weight_predictor_advanced.py` - Advanced training with multiple model options

## Step-by-Step Instructions

### Step 1: Analyze Your Dataset

First, analyze your ESCI dataset to discover patterns:

```bash
python dynamic_hybrid/analyze_esci_patterns.py \
    --data-path esci_data \
    --output-path dynamic_hybrid/esci_data_analysis.pkl
```

This will:
- Analyze query patterns for each optimal weight class
- Extract common terms, brands, and product vocabulary
- Save patterns to `esci_data_analysis.pkl`

Expected output:
```
Analyzing ESCI patterns...
Found 1485 training examples
Weight distribution:
  0.1/0.9: 523 queries
  0.2/0.8: 68 queries
  ...
Extracting patterns for each weight class...
Saved analysis to dynamic_hybrid/esci_data_analysis.pkl
```

### Step 2: Train with Data-Driven Features

Use the basic training script with data-driven features:

```bash
# First, update train_weight_predictor.py to use data-driven features
# Add this import at the top:
# from feature_extractor_esci_datadriven import ESCIDataDrivenFeatureExtractor

# Then run training:
python dynamic_hybrid/train_weight_predictor.py \
    -d esci \
    -u local \
    --host localhost \
    -p 9200 \
    -i esci-products \
    -m YOUR_MODEL_ID \
    -o esci_datadriven_model.pkl \
    --data-path esci_data \
    --full-dataset \
    --sample-size 5000
```

### Step 3: Advanced Model Training (Optional)

For even better results, use the advanced training script with Random Forest:

```bash
python dynamic_hybrid/train_weight_predictor_advanced.py \
    -d esci \
    -u local \
    --host localhost \
    -p 9200 \
    -i esci-products \
    -m YOUR_MODEL_ID \
    -o esci_rf_model.pkl \
    --data-path esci_data \
    --full-dataset \
    --sample-size 5000 \
    --model-type random_forest
```

Available model types:
- `linear` - Linear Regression (baseline)
- `random_forest` - Random Forest (recommended)
- `xgboost` - XGBoost (best performance, requires `pip install xgboost`)

### Step 4: Evaluate with ML Model

Test your trained model:

```bash
python dynamic_hybrid/evaluate_dynamic_hybrid_standalone.py \
    --dataset esci \
    --url local \
    --data-path esci_data \
    --index esci-products \
    --model-id YOUR_MODEL_ID \
    --output results/esci_ml_evaluation.json \
    --use-ml \
    --ml-model-path esci_rf_model.pkl \
    --compare \
    --static-weights "0.3,0.7" "0.5,0.5" "0.7,0.3"
```

## Modifying train_weight_predictor.py

To use data-driven features, modify `train_weight_predictor.py`:

```python
# Add import at top
from feature_extractor_esci_datadriven import ESCIDataDrivenFeatureExtractor

# In collect_training_data method, replace:
# feature_extractor = DomainAwareFeatureExtractor(domain)

# With:
analysis_path = "dynamic_hybrid/esci_data_analysis.pkl"
if dataset_name.lower() == "esci" and os.path.exists(analysis_path):
    feature_extractor = ESCIDataDrivenFeatureExtractor(analysis_path)
    logger.info("Using data-driven feature extractor")
else:
    feature_extractor = DomainAwareFeatureExtractor(domain)
```

## Expected Improvements

With data-driven features:
- **R² improvement**: 0.12 → 0.20-0.30
- **More features**: 25+ learned features vs 9 basic features
- **Better patterns**: Discovers actual query patterns from your data

With Random Forest:
- **R² improvement**: Additional 0.05-0.10
- **Handles non-linearity**: Captures complex feature interactions
- **Feature importance**: Shows which features matter most

## Feature Analysis Output

The analyze_esci_patterns.py script will show:
```
=== Pattern Analysis ===
Lexical-heavy queries tend to:
- Start with: ['Samsung', 'Apple', 'Nike']
- Contain brand names or SKUs
- Average length: 2.3 tokens

Neural-heavy queries tend to:
- Start with: ['best', 'how to', 'gift ideas']
- Be more descriptive
- Average length: 5.1 tokens

Top brands found: Apple, Samsung, Nike, Adidas...
Common product terms: laptop, phone, shoes, headphones...
```

## Troubleshooting

1. **"Analysis data not found"**
   - Run `analyze_esci_patterns.py` first
   - Check the output path matches the import path

2. **Low R² scores**
   - Try Random Forest instead of Linear Regression
   - Increase sample size to 10,000 queries
   - Check if analysis captured enough patterns

3. **XGBoost not available**
   - Install with: `pip install xgboost`
   - Or use Random Forest instead

## Next Steps

After training with advanced features:
1. Compare results with basic features
2. Analyze feature importance from Random Forest
3. Fine-tune based on your specific use case
4. Consider adding domain-specific features

The data-driven approach should significantly improve your model's ability to predict optimal weights for ESCI queries!
