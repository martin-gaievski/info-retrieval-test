#!/bin/bash

# Example script to evaluate dynamic hybrid search on SciDocs dataset
# This demonstrates how to use the POC with your existing BEIR setup
# Run this script from the parent directory: /Users/gaievski/dev/ir/info-retrieval-test

# Parse command line arguments
USE_ML=false
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--model-id)
            MODEL_ID="$2"
            shift 2
            ;;
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -i|--index)
            INDEX_NAME="$2"
            shift 2
            ;;
        --use-ml)
            USE_ML=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -m, --model-id MODEL_ID    ML model ID (required, or set MODEL_ID env var)"
            echo "  -h, --host HOST            OpenSearch host (default: localhost)"
            echo "  -p, --port PORT            OpenSearch port (default: 9200)"
            echo "  -i, --index INDEX          Index name (default: my-nlp-index-1)"
            echo "  --use-ml                   Use ML model for weight prediction (default: heuristics)"
            echo "  --help                     Show this help message"
            echo ""
            echo "Example:"
            echo "  $0 --model-id xn0ddpQBU-oejfwL-9TW"
            echo "  $0 --model-id xn0ddpQBU-oejfwL-9TW --use-ml"
            echo "  MODEL_ID=xn0ddpQBU-oejfwL-9TW $0"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Configuration with defaults
DATASET="scidocs"
DATASET_URL="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scidocs.zip"
HOST="${HOST:-localhost}"
PORT="${PORT:-9200}"
INDEX_NAME="${INDEX_NAME:-my-nlp-index-1}"

# Check if MODEL_ID is set (from environment or command line)
if [ -z "$MODEL_ID" ]; then
    echo "ERROR: MODEL_ID is not set!"
    echo ""
    echo "Please provide the model ID either via:"
    echo "  1. Command line: $0 --model-id <your-model-id>"
    echo "  2. Environment variable: MODEL_ID=<your-model-id> $0"
    echo ""
    echo "To find available models, run:"
    echo "  curl localhost:9200/_plugins/_ml/models | jq '.models[].model_id'"
    exit 1
fi

# Output directory
OUTPUT_DIR="results"
mkdir -p ${OUTPUT_DIR}

echo "=== Configuration ==="
echo "Dataset: ${DATASET}"
echo "Host: ${HOST}:${PORT}"
echo "Index: ${INDEX_NAME}"
echo "Model ID: ${MODEL_ID}"
echo "Use ML: ${USE_ML}"
echo "Output: ${OUTPUT_DIR}"
echo ""

echo "=== Dynamic Hybrid Search Evaluation for SciDocs ==="
echo ""

# 1. Evaluate with dynamic weights only
echo "Step 1: Evaluating with dynamic weights..."
EVAL_CMD="python dynamic_hybrid/evaluate_dynamic_hybrid_standalone.py \
    --dataset ${DATASET} \
    --url ${DATASET_URL} \
    --host ${HOST} \
    --port ${PORT} \
    --index ${INDEX_NAME} \
    --model-id ${MODEL_ID} \
    --output ${OUTPUT_DIR}/${DATASET}_dynamic_results.json"

if [ "$USE_ML" = true ]; then
    EVAL_CMD="${EVAL_CMD} --use-ml"
fi

eval ${EVAL_CMD}

echo ""
echo "Step 2: Comparing dynamic vs static weights..."
# 2. Compare with multiple static weight configurations
COMPARE_CMD="python dynamic_hybrid/evaluate_dynamic_hybrid_standalone.py \
    --dataset ${DATASET} \
    --url ${DATASET_URL} \
    --host ${HOST} \
    --port ${PORT} \
    --index ${INDEX_NAME} \
    --model-id ${MODEL_ID} \
    --output ${OUTPUT_DIR}/${DATASET}_comparison_results.json \
    --compare \
    --static-weights \"0.5,0.5\" \"0.6,0.4\" \"0.7,0.3\" \"0.4,0.6\" \"0.3,0.7\" \"0.1,0.9\" \"0.9,0.1\""

if [ "$USE_ML" = true ]; then
    COMPARE_CMD="${COMPARE_CMD} --use-ml"
fi

eval ${COMPARE_CMD}

echo ""
echo "=== Results Summary ==="

# Extract and display key metrics
if [ -f "${OUTPUT_DIR}/${DATASET}_comparison_results.json" ]; then
    echo ""
    echo "Dynamic vs Static Weight Comparison:"
    python -c "
import json
with open('${OUTPUT_DIR}/${DATASET}_comparison_results.json', 'r') as f:
    data = json.load(f)
    
    # Print dynamic results
    print(f\"\\nDynamic Weights Results:\")
    print(f\"  Domain: {data['dynamic']['domain']}\")
    print(f\"  NDCG@10: {data['dynamic']['average_metrics']['ndcg@10']:.4f}\")
    print(f\"  MAP@10: {data['dynamic']['average_metrics']['map@10']:.4f}\")
    print(f\"  Weight distribution: {data['dynamic']['weight_distribution']}\")
    
    # Print comparisons
    print(f\"\\nComparison with Static Weights:\")
    for comp in data['comparisons']:
        weights = comp['static_weights']
        improvements = comp['improvements']
        print(f\"\\n  Static weights {weights}:\")
        for metric, info in improvements.items():
            print(f\"    {metric}: {info['dynamic']:.4f} vs {info['static']:.4f} ({info['improvement_pct']:+.1f}%)\")
"
fi

echo ""
echo "Detailed results saved to:"
echo "  - ${OUTPUT_DIR}/${DATASET}_dynamic_results.json"
echo "  - ${OUTPUT_DIR}/${DATASET}_comparison_results.json"
echo ""
echo "=== Evaluation Complete ==="
