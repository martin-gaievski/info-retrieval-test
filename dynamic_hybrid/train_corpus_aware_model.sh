#!/bin/bash

# Train corpus-aware weight predictor model for dynamic hybrid search
# This implements the approach with query string features and corpus-based features

# Default values
DATASET="esci"
URL="local"
DATA_PATH="esci_data"
HOST="localhost"
PORT="9200"
INDEX="esci_products"
MODEL_ID=""
OUTPUT="corpus_aware_weight_predictor.pkl"
SAMPLE_SIZE="500"
MODEL_TYPE="random_forest"
TRAINING_DATA_FILE="corpus_aware_training_data.csv"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--dataset)
      DATASET="$2"
      shift 2
      ;;
    -u|--url)
      URL="$2"
      shift 2
      ;;
    --data-path)
      DATA_PATH="$2"
      shift 2
      ;;
    --host)
      HOST="$2"
      shift 2
      ;;
    -p|--port)
      PORT="$2"
      shift 2
      ;;
    -i|--index)
      INDEX="$2"
      shift 2
      ;;
    -m|--model-id)
      MODEL_ID="$2"
      shift 2
      ;;
    -o|--output)
      OUTPUT="$2"
      shift 2
      ;;
    --sample-size)
      SAMPLE_SIZE="$2"
      shift 2
      ;;
    --model-type)
      MODEL_TYPE="$2"
      shift 2
      ;;
    --no-cache)
      NO_CACHE="--no-cache"
      shift
      ;;
    --include-result-features)
      INCLUDE_RESULT="--include-result-features"
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  -d, --dataset DATASET      Dataset name (default: esci)"
      echo "  -u, --url URL              Dataset URL (default: local)"
      echo "  --data-path PATH           Path to dataset files (default: esci_data)"
      echo "  --host HOST                OpenSearch host (default: localhost)"
      echo "  -p, --port PORT            OpenSearch port (default: 9200)"
      echo "  -i, --index INDEX          Index name (default: esci_products)"
      echo "  -m, --model-id ID          Neural model ID (required)"
      echo "  -o, --output FILE          Output model file (default: corpus_aware_weight_predictor.pkl)"
      echo "  --sample-size SIZE         Number of queries to sample (default: 500)"
      echo "  --model-type TYPE          Model type: random_forest, gradient_boosting, linear, ridge (default: random_forest)"
      echo "  --no-cache                 Disable term statistics caching"
      echo "  --include-result-features  Include search result features in training"
      echo "  -h, --help                 Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if model ID is provided
if [ -z "$MODEL_ID" ]; then
  echo "Error: Neural model ID is required. Use -m or --model-id to specify."
  exit 1
fi

# Create output directory if needed
OUTPUT_DIR=$(dirname "$OUTPUT")
if [ ! -d "$OUTPUT_DIR" ] && [ "$OUTPUT_DIR" != "." ]; then
  mkdir -p "$OUTPUT_DIR"
fi

echo "Training corpus-aware weight predictor model..."
echo "Dataset: $DATASET"
echo "Index: $INDEX"
echo "Model ID: $MODEL_ID"
echo "Sample size: $SAMPLE_SIZE"
echo "Model type: $MODEL_TYPE"
echo "Output: $OUTPUT"
echo ""

# Run the training script
python dynamic_hybrid/train_corpus_aware_predictor.py \
  --dataset "$DATASET" \
  --url "$URL" \
  --data-path "$DATA_PATH" \
  --host "$HOST" \
  --port "$PORT" \
  --index "$INDEX" \
  --model-id "$MODEL_ID" \
  --output "$OUTPUT" \
  --sample-size "$SAMPLE_SIZE" \
  --model-type "$MODEL_TYPE" \
  --training-data-file "$TRAINING_DATA_FILE" \
  $NO_CACHE \
  $INCLUDE_RESULT

# Check if training was successful
if [ $? -eq 0 ]; then
  echo ""
  echo "Training completed successfully!"
  echo "Model saved to: $OUTPUT"
  echo "Training data saved to: $TRAINING_DATA_FILE"
  
  # Provide next steps
  echo ""
  echo "Next steps:"
  echo "1. Evaluate the model:"
  echo "   python dynamic_hybrid/evaluate_corpus_aware_predictor.py \\"
  echo "     --dataset $DATASET --url $URL --index $INDEX \\"
  echo "     --model-id $MODEL_ID --model-path $OUTPUT"
  echo ""
  echo "2. Use the model for predictions:"
  echo "   # The model can be loaded and used to predict optimal weights"
  echo "   # based on query features including corpus statistics"
else
  echo ""
  echo "Training failed. Please check the error messages above."
  exit 1
fi
