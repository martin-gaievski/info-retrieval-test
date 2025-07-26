#!/bin/bash

# Run exhaustive comparison of all weight combinations for ESCI
# This tests the ML model against static weight combinations
#
# Usage: 
#   ./run_esci_exhaustive_comparison.sh [number_of_queries] [weight_pairs...]
#
# Examples:
#   # Test all combinations (default)
#   ./run_esci_exhaustive_comparison.sh 500
#   
#   # Test specific weight pairs only
#   ./run_esci_exhaustive_comparison.sh 500 "0.6,0.4" "0.7,0.3" "0.8,0.2"
#
# Default: 500 queries, all weight combinations

# Default number of queries if not provided
DEFAULT_QUERIES=500

# Get number of queries from first argument
NUM_QUERIES=${1:-$DEFAULT_QUERIES}

# Shift to get remaining arguments (weight pairs)
shift

echo "======================================"
echo "ESCI Weight Comparison"
echo "======================================"
echo "Usage: $0 [number_of_queries] [weight_pairs...]"
echo "Using $NUM_QUERIES queries for evaluation"

# Check if specific weight pairs were provided
if [ $# -gt 0 ]; then
    echo "Testing specific weight combinations: $@"
    WEIGHT_ARGS="--static-weights $@"
else
    echo "Testing ALL weight combinations (0.1 step)"
    WEIGHT_ARGS=""
fi
echo "======================================"

# Check if the model file exists
if [ ! -f "esci_rf_optimized.pkl" ]; then
    echo "Warning: esci_rf_optimized.pkl not found. Copying from esci_weight_predictor_model.pkl..."
    cp esci_weight_predictor_model.pkl esci_rf_optimized.pkl
fi

# Check if feature extractor exists
if ! grep -q "ESCIOptimizedFeatureExtractor" dynamic_hybrid/evaluate_dynamic_hybrid_standalone.py; then
    echo "Note: Using standard feature extractor. For best results, update the script to use ESCIOptimizedFeatureExtractor"
fi

# Run the evaluation with specified combinations
python dynamic_hybrid/evaluate_dynamic_hybrid_standalone.py \
    --dataset esci \
    --url esci \
    --host localhost \
    --port 9200 \
    --index esci-products \
    --model-id pCEwOZgBtmNHHhH7ZtEE \
    --output results/esci_exhaustive_comparison_ml.json \
    --use-ml \
    --compare \
    --max-queries $NUM_QUERIES \
    --weight-step 0.1 \
    $WEIGHT_ARGS

echo "Evaluation complete. Results saved to results/esci_exhaustive_comparison_ml.json"

# Extract and display the best static weight
echo ""
echo "=== Finding Best Static Weight ==="
python -c "
import json
with open('results/esci_exhaustive_comparison_ml.json', 'r') as f:
    data = json.load(f)
    
# Find best static weight
best_static = None
best_ndcg = 0
for comp in data['comparisons']:
    ndcg = comp['static_results']['average_metrics']['ndcg@10']
    if ndcg > best_ndcg:
        best_ndcg = ndcg
        best_static = comp['static_weights']

dynamic_ndcg = data['dynamic']['average_metrics']['ndcg@10']

print(f'Best static weight: {best_static} (NDCG@10: {best_ndcg:.4f})')
print(f'Dynamic ML weight: NDCG@10: {dynamic_ndcg:.4f}')
print(f'Dynamic vs Best Static: {((dynamic_ndcg - best_ndcg) / best_ndcg * 100):+.1f}%')
"
