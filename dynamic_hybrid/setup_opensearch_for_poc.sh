#!/bin/bash
# Quick setup script for Dynamic Hybrid Search POC prerequisites

# Parse command line arguments
DATASET_NAME=""
HOST="localhost"
PORT="9200"
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--dataset)
            DATASET_NAME="$2"
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
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -d, --dataset DATASET    Dataset name (e.g., scifact, scidocs, esci-product)"
            echo "  -h, --host HOST          OpenSearch host (default: localhost)"
            echo "  -p, --port PORT          OpenSearch port (default: 9200)"
            echo "  --help                   Show this help message"
            echo ""
            echo "Example:"
            echo "  $0 --dataset scifact --host myserver.com --port 9201"
            echo "  $0 --dataset esci-product    # Uses special ESCI ingestion script"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Map dataset names to URLs
declare -A DATASET_URLS
DATASET_URLS["scifact"]="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip"
DATASET_URLS["scidocs"]="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scidocs.zip"
DATASET_URLS["trec-covid"]="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/trec-covid.zip"
DATASET_URLS["nfcorpus"]="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/nfcorpus.zip"
DATASET_URLS["fiqa"]="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/fiqa.zip"
DATASET_URLS["arguana"]="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/arguana.zip"

echo "=== OpenSearch Setup for Dynamic Hybrid Search POC ==="
echo "OpenSearch Host: $HOST"
echo "OpenSearch Port: $PORT"
if [ -n "$DATASET_NAME" ]; then
    echo "Dataset: $DATASET_NAME"
fi
echo ""


# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
MAJOR='\033[0;34m'
RESET='\033[0m' # No Color

# 1. Check if OpenSearch is running
echo "Checking OpenSearch status..."
if curl -s $HOST:$PORT > /dev/null 2>&1; then
    echo -e "${GREEN}✓ OpenSearch is running${NC}"
    
    # Get cluster info
    CLUSTER_INFO=$(curl -s $HOST:$PORT)
    VERSION=$(echo $CLUSTER_INFO | grep -o '"number" : "[^"]*"' | cut -d'"' -f4)
    echo "  Version: $VERSION"
else
    echo -e "${RED}✗ OpenSearch is not running on $HOST:$PORT${NC}"
    echo ""
    echo "Please start OpenSearch first. You can use Docker:"
    echo "  docker run -d --name opensearch -p $PORT:$PORT -p 9300:9300 \\"
    echo "    -e \"discovery.type=single-node\" \\"
    echo "    -e \"plugins.security.disabled=true\" \\"
    echo "    opensearchproject/opensearch:2.11.0"
    exit 1
fi

# 2. Check neural-search plugin
echo ""
echo "Checking neural-search plugin..."
if curl -s $HOST:$PORT/_cat/plugins 2>/dev/null | grep -q neural-search; then
    echo -e "${GREEN}✓ Neural-search plugin is installed${NC}"
else
    echo -e "${YELLOW}⚠ Neural-search plugin not found${NC}"
    echo "  You may need to install it manually"
fi

# 3. Check if index exists
echo ""
echo "Checking index 'my-nlp-index-1'..."
if curl -s -o /dev/null -w "%{http_code}" $HOST:$PORT/my-nlp-index-1 2>/dev/null | grep -q 200; then
    echo -e "${GREEN}✓ Index already exists${NC}"
    
    # Get document count
    DOC_COUNT=$(curl -s $HOST:$PORT/my-nlp-index-1/_count 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2)
    echo "  Document count: $DOC_COUNT"
    
    if [ "$DOC_COUNT" -eq "0" ]; then
        echo -e "${YELLOW}  ⚠ Index is empty - you need to ingest data${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Index does not exist${NC}"
    echo "Creating index with hybrid mappings..."
    
    # Create index
    RESPONSE=$(curl -s -X PUT "$HOST:$PORT/my-nlp-index-1" \
    -H "Content-Type: application/json" \
    -d '{
      "settings": {
        "number_of_shards": 12,
        "number_of_replicas": 0,
        "index.knn": true,
        "default_pipeline": "embeddings-pipeline"
      },
      "mappings": {
        "properties": {
          "text": {
            "type": "text",
            "analyzer": "standard"
          },
          "title_embedding": {
            "type": "knn_vector",
            "dimension": 384,
            "method": {
              "name": "hnsw",
              "space_type": "l2",
              "engine": "lucene",
              "parameters": {
                "ef_construction": 128,
                "m": 24
              }
            }
          },
          "product_title": {
            "type": "text"
          }
        }
      }
    }')
    
    if echo "$RESPONSE" | grep -q '"acknowledged":true'; then
        echo -e "${GREEN}✓ Index created successfully${NC}"
    else
        echo -e "${RED}✗ Failed to create index${NC}"
        echo "Response: $RESPONSE"
    fi
fi

echo -e "${MAJOR}Configuring the ML Commons plugin.${RESET}"
curl -s -X PUT "http://$HOST:$PORT/_cluster/settings" -H 'Content-Type: application/json' --data-binary '{
  "persistent": {
        "plugins": {
            "ml_commons": {
                "only_run_on_ml_node": "false",
                "model_access_control_enabled": "true",
                "native_memory_threshold": "99"
            }
        }
    }
}'

echo
echo -e "${MAJOR}Lookup or Register a model group.${RESET}"
response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/model_groups/_search" \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "query": {
      "bool": {
        "must": [
          {
            "terms": {
              "name": [
                "neural_search_model_group"
              ]
            }
          }
        ]
      }
    }
  }')

# Extract the model_group_id from the JSON response
model_group_id=$(echo "$response" | jq -r '.hits.hits[0]._id')

# Check if model_group_id is blank or "null"
if [ -z "$model_group_id" ] || [ "$model_group_id" = "null" ]; then
  echo "No existing model group found, creating a new one..."
  response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/model_groups/_register" \
    -H 'Content-Type: application/json' \
    --data-binary '{
      "name": "neural_search_model_group",
      "description": "A model group for neural search models"
    }')

  # Extract the model_group_id from the JSON response
  model_group_id=$(echo "$response" | jq -r '.model_group_id')
  echo "Created Model Group with id: $model_group_id"
else
  echo "Using existing Model Group with id: $model_group_id"
fi

echo -e "${MAJOR}Registering a model in the model group.${RESET}"
response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/models/_register" \
  -H 'Content-Type: application/json' \
  --data-binary "{
     \"name\": \"huggingface/sentence-transformers/all-MiniLM-L6-v2\",
     \"version\": \"1.0.1\",
     \"model_group_id\": \"$model_group_id\",
     \"model_format\": \"TORCH_SCRIPT\"
  }")

# Extract the task_id from the JSON response
task_id=$(echo "$response" | jq -r '.task_id')

# Use the extracted task_id
echo "Created Model, get status with task id: $task_id"


echo -e "${MAJOR}Waiting for the model to be registered.${RESET}"
max_attempts=10
attempts=0

# Wait for task to be COMPLETED
while [[ "$(curl -s $HOST:$PORT/_plugins/_ml/tasks/$task_id | jq -r '.state')" != "COMPLETED" && $attempts -lt $max_attempts ]]; do
    echo "Waiting for task to complete... attempt $((attempts + 1))/$max_attempts"
    sleep 5
    attempts=$((attempts + 1))
done

if [[ $attempts -ge $max_attempts ]]; then
    echo "Limit of attempts reached. Something went wrong with registering the model. Check OpenSearch logs."
    exit 1
else
    response=$(curl -s $HOST:$PORT/_plugins/_ml/tasks/$task_id)
    model_id=$(echo "$response" | jq -r '.model_id')
    echo "Task completed successfully! Model registered with id: $model_id"
fi

echo -e "${MAJOR}Deploying the model.${RESET}"
response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/models/$model_id/_deploy")

# Extract the task_id from the JSON response
deploy_task_id=$(echo "$response" | jq -r '.task_id')

echo "Model deployment started, get status with task id: $deploy_task_id"

echo -e "${MAJOR}Waiting for the model to be deployed.${RESET}"
# Reset attempts
attempts=0

while [[ "$(curl -s $HOST:$PORT/_plugins/_ml/tasks/$deploy_task_id | jq -r '.state')" != "COMPLETED" && $attempts -lt $max_attempts ]]; do
    echo "Waiting for deployment task to complete... attempt $((attempts + 1))/$max_attempts"
    sleep 5
    attempts=$((attempts + 1))
done

if [[ $attempts -ge $max_attempts ]]; then
    echo "Limit of attempts reached. Something went wrong with deploying the model. Check OpenSearch logs."
else
    response=$(curl -s $HOST:$PORT/_plugins/_ml/tasks/$deploy_task_id)
    echo "Deployment task completed successfully!"
fi

# Check state of deployed model
attempts=0
while [[ "$(curl -s $HOST:$PORT/_plugins/_ml/models/$model_id | jq -r '.model_state')" != "DEPLOYED" && $attempts -lt $max_attempts ]]; do
    echo "Waiting for task to complete... attempt $((attempts + 1))/$max_attempts"
    sleep 5
    attempts=$((attempts + 1))
done

if [[ $attempts -ge $max_attempts ]]; then
    echo "Limit of attempts reached. Something went wrong with deploying the model. Check OpenSearch logs."
else
    echo "Task completed successfully! Model deployment successful with model id: $model_id"
fi

echo -e "${MAJOR}Creating an ingest pipeline for embedding generation during index time.${RESET}"
curl -s -X PUT "http://$HOST:$PORT/_ingest/pipeline/embeddings-pipeline" \
  -H 'Content-Type: application/json' \
  --data-binary "{
     \"description\": \"A text embedding pipeline\",
       \"processors\": [
         {
          \"text_embedding\": {
          \"model_id\": \"$model_id\",
          \"field_map\": {
            \"product_title\": \"title_embedding\"
          }
        }
      }
    ]
  }"

echo ""
echo -e "${GREEN}✓ Ingest pipeline created successfully${NC}"

# The model has been deployed, so we can use it now
echo ""
echo -e "${GREEN}✓ Model setup complete!${NC}"
echo "  Model ID: $model_id"
echo "  Model Group ID: $model_group_id"

# 5. Test hybrid query capability
echo ""
echo "Testing hybrid query..."

# Use the model we just deployed
if [ -n "$model_id" ]; then
    TEST_RESPONSE=$(curl -s -X POST "$HOST:$PORT/my-nlp-index-1/_search" \
    -H "Content-Type: application/json" \
    -d "{
      \"size\": 1,
      \"query\": {
        \"hybrid\": {
          \"queries\": [
            {\"match\": {\"passage_text\": \"test\"}},
            {\"neural\": {\"title_embedding\": {\"query_text\": \"test\", \"model_id\": \"$model_id\", \"k\": 1}}}
          ]
        }
      }
    }" 2>/dev/null)
    
    if echo "$TEST_RESPONSE" | grep -q '"hits"'; then
        echo -e "${GREEN}✓ Hybrid query is working${NC}"
    else
        echo -e "${RED}✗ Hybrid query failed${NC}"
        echo "Response: $TEST_RESPONSE"
    fi
else
    echo -e "${YELLOW}⚠ Cannot test hybrid query without a deployed model${NC}"
fi

# Summary
echo ""
echo "=== Setup Summary ==="
echo ""

# Check all prerequisites
READY=true

if ! curl -s $HOST:$PORT > /dev/null 2>&1; then
    echo -e "${RED}✗ OpenSearch not running${NC}"
    READY=false
else
    echo -e "${GREEN}✓ OpenSearch running${NC}"
fi

if curl -s $HOST:$PORT/_cat/plugins 2>/dev/null | grep -q neural-search; then
    echo -e "${GREEN}✓ Neural-search plugin installed${NC}"
else
    echo -e "${YELLOW}⚠ Neural-search plugin not verified${NC}"
fi

if curl -s -o /dev/null -w "%{http_code}" $HOST:$PORT/my-nlp-index-1 2>/dev/null | grep -q 200; then
    echo -e "${GREEN}✓ Index exists${NC}"
else
    echo -e "${RED}✗ Index missing${NC}"
    READY=false
fi

if [ -n "$model_id" ]; then
    echo -e "${GREEN}✓ ML model available ($model_id)${NC}"
else
    echo -e "${RED}✗ No ML model deployed${NC}"
    READY=false
fi

DOC_COUNT=$(curl -s $HOST:$PORT/my-nlp-index-1/_count 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2)
if [ -n "$DOC_COUNT" ] && [ "$DOC_COUNT" -gt "0" ]; then
    echo -e "${GREEN}✓ Index has data ($DOC_COUNT documents)${NC}"
else
    echo -e "${YELLOW}⚠ Index is empty - ingest data before running evaluation${NC}"
fi

echo ""
if [ "$READY" = true ] && [ -n "$model_id" ]; then
    echo -e "${GREEN}✅ Prerequisites are met!${NC}"
    echo ""
    
    # Ingest dataset if index is empty and dataset is specified
    if [ -z "$DOC_COUNT" ] || [ "$DOC_COUNT" -eq "0" ]; then
        if [ -n "$DATASET_NAME" ]; then
            # Special handling for esci-product dataset
            if [ "$DATASET_NAME" = "esci-product" ]; then
                echo -e "${MAJOR}Ingesting ESCI product dataset...${RESET}"
                echo "Running: python3 dynamic_hybrid/esci_ingestion.py -m $model_id -h $HOST -p $PORT --full-dataset"
                
                python3 dynamic_hybrid/esci_ingestion.py -m "$model_id" -h "$HOST" -p "$PORT" --full-dataset
                
                if [ $? -eq 0 ]; then
                    echo -e "${GREEN}✓ ESCI dataset ingestion completed${NC}"
                    
                    # Check document count again
                    DOC_COUNT=$(curl -s $HOST:$PORT/my-nlp-index-1/_count 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2)
                    echo "  Document count after ingestion: $DOC_COUNT"
                else
                    echo -e "${RED}✗ ESCI dataset ingestion failed${NC}"
                    echo "Please check the error messages above"
                    exit 1
                fi
            else
                # Check if dataset URL exists
                if [ -z "${DATASET_URLS[$DATASET_NAME]}" ]; then
                    echo -e "${RED}✗ Unknown dataset: $DATASET_NAME${NC}"
                    echo "Supported datasets: ${!DATASET_URLS[@]} esci-product"
                    exit 1
                fi
                
                DATASET_URL="${DATASET_URLS[$DATASET_NAME]}"
                echo -e "${MAJOR}Ingesting $DATASET_NAME dataset...${RESET}"
                echo "Running: python3 test_opensearch_3.py -d $DATASET_NAME -u $DATASET_URL -h $HOST -p $PORT -i my-nlp-index-1 -o ingest"
                
                python3 test_opensearch_3.py \
                    -d "$DATASET_NAME" \
                    -u "$DATASET_URL" \
                    -h $HOST \
                    -p $PORT \
                    -i my-nlp-index-1 \
                    -o ingest
                
                if [ $? -eq 0 ]; then
                    echo -e "${GREEN}✓ Dataset ingestion completed${NC}"
                    
                    # Check document count again
                    DOC_COUNT=$(curl -s $HOST:$PORT/my-nlp-index-1/_count 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2)
                    echo "  Document count after ingestion: $DOC_COUNT"
                else
                    echo -e "${RED}✗ Dataset ingestion failed${NC}"
                    echo "Please check the error messages above"
                    exit 1
                fi
            fi
        else
            echo -e "${YELLOW}⚠ No dataset specified. Use -d/--dataset to ingest data.${NC}"
        fi
    fi
    
    echo ""
    echo "Next steps:"
    echo "1. Run the evaluation from the parent directory:"
    if [ -n "$DATASET_NAME" ]; then
        echo "   ./dynamic_hybrid/run_${DATASET_NAME}_evaluation.sh --model-id $model_id"
    else
        echo "   ./dynamic_hybrid/run_<dataset>_evaluation.sh --model-id $model_id"
    fi
else
    echo -e "${RED}❌ Some prerequisites are missing${NC}"
    echo ""
    echo "Please address the issues above before running the evaluation."
fi

echo ""
echo "For detailed setup instructions, see SETUP_GUIDE.md"
