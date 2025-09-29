#!/bin/bash

# OpenSearch setup script for One-Click Hybrid Search POC
# This script prepares OpenSearch for the WANDS dataset hybrid search optimization

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments
HOST="localhost"
PORT="9200"
INDEX_NAME="wands_products"
SAMPLE_SIZE="100"
INGEST_ONLY="false"

while [[ $# -gt 0 ]]; do
    case $1 in
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
        -s|--sample-size)
            SAMPLE_SIZE="$2"
            shift 2
            ;;
    --ingest-only)
        INGEST_ONLY="true"
        shift
        ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -h, --host HOST          OpenSearch host (default: localhost)"
            echo "  -p, --port PORT          OpenSearch port (default: 9200)"
            echo "  -i, --index INDEX        Index name (default: wands_products)"
            echo "  -s, --sample-size SIZE   Number of products to index (default: 100)"
            echo "  --ingest-only            Skip resource creation, only ingest documents"
            echo "  --help                   Show this help message"
            echo ""
            echo "Examples:"
            echo "  # First run - create all resources and ingest documents:"
            echo "  $0 --host localhost --port 9200 --sample-size 500"
            echo ""
            echo "  # Re-run only document ingestion (skip resource creation):"
            echo "  $0 --ingest-only --sample-size 1000"
            echo ""
            echo "This script will:"
            echo "  1. Check OpenSearch connectivity"
            echo "  2. Set up ML Commons plugin (skip with --ingest-only)"
            echo "  3. Deploy a sentence transformer model (skip with --ingest-only)"
            echo "  4. Create an index for WANDS products (skip with --ingest-only)"
            echo "  5. Index sample products from WANDS dataset"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "============================================================"
echo "     One-Click Hybrid Search POC - OpenSearch Setup"
echo "============================================================"
echo ""
echo "Configuration:"
echo "  OpenSearch Host: $HOST"
echo "  OpenSearch Port: $PORT"
echo "  Index Name: $INDEX_NAME"
echo "  Sample Size: $SAMPLE_SIZE products"
if [ "$INGEST_ONLY" = "true" ]; then
    echo "  Mode: INGEST ONLY (skipping resource creation)"
else
    echo "  Mode: FULL SETUP (creating resources and ingesting)"
fi
echo ""

# Function to check command availability
check_command() {
    if ! command -v $1 &> /dev/null; then
        echo -e "${RED}✗ $1 is not installed${NC}"
        echo "  Please install $1 to continue"
        exit 1
    fi
}

# Check required commands
echo "Checking prerequisites..."
check_command curl
check_command jq
check_command python3

# 1. Check if OpenSearch is running
echo ""
echo "Checking OpenSearch connection..."
if curl -s $HOST:$PORT > /dev/null 2>&1; then
    echo -e "${GREEN}✓ OpenSearch is running${NC}"
    
    # Get cluster info
    CLUSTER_INFO=$(curl -s $HOST:$PORT)
    VERSION=$(echo $CLUSTER_INFO | jq -r '.version.number' 2>/dev/null || echo "unknown")
    echo "  Version: $VERSION"
else
    echo -e "${RED}✗ OpenSearch is not running on $HOST:$PORT${NC}"
    echo ""
    echo "To start OpenSearch with Docker:"
    echo ""
    echo "docker run -d --name opensearch \\"
    echo "  -p 9200:9200 -p 9600:9600 \\"
    echo "  -e \"discovery.type=single-node\" \\"
    echo "  -e \"plugins.security.disabled=true\" \\"
    echo "  opensearchproject/opensearch:latest"
    echo ""
    exit 1
fi

# 2. Check neural-search plugin
echo ""
echo "Checking neural-search plugin..."
if curl -s $HOST:$PORT/_cat/plugins 2>/dev/null | grep -q neural-search; then
    echo -e "${GREEN}✓ Neural-search plugin is installed${NC}"
else
    echo -e "${YELLOW}⚠ Neural-search plugin not found${NC}"
    echo "  The POC will work with keyword search only"
    echo "  To enable vector search, install the neural-search plugin"
fi

# Skip resource creation if --ingest-only flag is set
if [ "$INGEST_ONLY" = "true" ]; then
    echo ""
    echo -e "${BLUE}Skipping resource creation (--ingest-only mode)${NC}"
    echo "  - Skipping ML Commons configuration"
    echo "  - Skipping model deployment"
    echo "  - Skipping pipeline creation"
    echo ""
else

# 3. Configure ML Commons plugin (if available)
echo ""
echo "Configuring ML Commons plugin..."
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
}' > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ ML Commons configured${NC}"
else
    echo -e "${YELLOW}⚠ ML Commons configuration skipped${NC}"
fi

# 4. Set up sentence transformer model (optional, for vector search)
echo ""
echo "Setting up sentence transformer model..."

# Check if model group exists
response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/model_groups/_search" \
  -H 'Content-Type: application/json' \
  --data-binary '{
    "query": {
      "bool": {
        "must": [
          {
            "terms": {
              "name": ["one_click_hybrid_model_group"]
            }
          }
        ]
      }
    }
  }' 2>/dev/null)

model_group_id=$(echo "$response" | jq -r '.hits.hits[0]._id' 2>/dev/null)

# Create model group if it doesn't exist
if [ -z "$model_group_id" ] || [ "$model_group_id" = "null" ]; then
    echo "  Creating model group..."
    response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/model_groups/_register" \
      -H 'Content-Type: application/json' \
      --data-binary '{
        "name": "one_click_hybrid_model_group",
        "description": "Model group for One-Click Hybrid Search POC"
      }' 2>/dev/null)
    
    model_group_id=$(echo "$response" | jq -r '.model_group_id' 2>/dev/null)
    
    if [ -n "$model_group_id" ] && [ "$model_group_id" != "null" ]; then
        echo -e "${GREEN}  ✓ Model group created: $model_group_id${NC}"
    else
        echo -e "${YELLOW}  ⚠ Could not create model group (ML Commons may not be available)${NC}"
    fi
else
    echo -e "${GREEN}  ✓ Using existing model group: $model_group_id${NC}"
fi

# Register and deploy model if model group was created
if [ -n "$model_group_id" ] && [ "$model_group_id" != "null" ]; then
    echo "  Registering sentence transformer model..."
    response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/models/_register" \
      -H 'Content-Type: application/json' \
      --data-binary "{
         \"name\": \"huggingface/sentence-transformers/all-MiniLM-L12-v2\",
         \"version\": \"1.0.2\",
         \"model_group_id\": \"$model_group_id\",
         \"model_format\": \"TORCH_SCRIPT\"
      }" 2>/dev/null)
    
    task_id=$(echo "$response" | jq -r '.task_id' 2>/dev/null)
    
    if [ -n "$task_id" ] && [ "$task_id" != "null" ]; then
        echo "  Waiting for model registration..."
        
        # Wait for registration to complete
        max_attempts=20
        attempts=0
        while [[ $attempts -lt $max_attempts ]]; do
            state=$(curl -s $HOST:$PORT/_plugins/_ml/tasks/$task_id 2>/dev/null | jq -r '.state' 2>/dev/null)
            if [ "$state" = "COMPLETED" ]; then
                break
            fi
            sleep 2
            attempts=$((attempts + 1))
        done
        
        if [ "$state" = "COMPLETED" ]; then
            model_id=$(curl -s $HOST:$PORT/_plugins/_ml/tasks/$task_id | jq -r '.model_id')
            echo -e "${GREEN}  ✓ Model registered: $model_id${NC}"
            
            # Deploy the model
            echo "  Deploying model..."
            response=$(curl -s -X POST "http://$HOST:$PORT/_plugins/_ml/models/$model_id/_deploy" 2>/dev/null)
            deploy_task_id=$(echo "$response" | jq -r '.task_id' 2>/dev/null)
            
            if [ -n "$deploy_task_id" ] && [ "$deploy_task_id" != "null" ]; then
                # Wait for deployment
                attempts=0
                while [[ $attempts -lt $max_attempts ]]; do
                    state=$(curl -s $HOST:$PORT/_plugins/_ml/tasks/$deploy_task_id 2>/dev/null | jq -r '.state' 2>/dev/null)
                    if [ "$state" = "COMPLETED" ]; then
                        echo -e "${GREEN}  ✓ Model deployed successfully${NC}"
                        break
                    fi
                    sleep 2
                    attempts=$((attempts + 1))
                done
            fi
        fi
    else
        echo -e "${YELLOW}  ⚠ Model registration skipped (ML Commons not available)${NC}"
    fi
fi

# 6. Create ingest pipeline (if model is available)
if [ -n "$model_id" ] && [ "$model_id" != "null" ]; then
    echo ""
    echo "Creating ingest pipeline..."
    curl -s -X PUT "http://$HOST:$PORT/_ingest/pipeline/embeddings-pipeline" \
      -H 'Content-Type: application/json' \
      --data-binary "{
        \"description\": \"Embedding pipeline for WANDS products\",
        \"processors\": [
          {
            \"text_embedding\": {
              \"model_id\": \"$model_id\",
              \"field_map\": {
                \"product_search\": \"product_embedding\"
              }
            }
          }
        ]
      }" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Ingest pipeline created${NC}"
    fi
fi

fi # End of resource creation section (skipped if --ingest-only)

# 5. Create index for WANDS products (in both modes if needed)
echo ""
echo "Checking index '$INDEX_NAME'..."

# Check if index exists
if curl -s -o /dev/null -w "%{http_code}" $HOST:$PORT/$INDEX_NAME 2>/dev/null | grep -q 200; then
    echo -e "${GREEN}✓ Index already exists${NC}"
    
    # Only ask to recreate in non-ingest-only mode
    if [ "$INGEST_ONLY" != "true" ]; then
        echo "  Do you want to delete and recreate it? (y/n)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            curl -s -X DELETE "$HOST:$PORT/$INDEX_NAME" > /dev/null 2>&1
            echo "  Index deleted"
        fi
    fi
fi

# Create index with hybrid search mappings if it doesn't exist
if ! curl -s -o /dev/null -w "%{http_code}" $HOST:$PORT/$INDEX_NAME 2>/dev/null | grep -q 200; then
    echo "Creating index '$INDEX_NAME'..."
    RESPONSE=$(curl -s -X PUT "$HOST:$PORT/$INDEX_NAME" \
    -H "Content-Type: application/json" \
    -d '{
      "settings": {
        "number_of_shards": 4,
        "number_of_replicas": 0,
        "index.knn": true,
        "default_pipeline": "embeddings-pipeline",
        "analysis": {
          "analyzer": {
            "standard_analyzer": {
              "type": "standard",
              "stopwords": "_english_"
            }
          }
        }
      },
      "mappings": {
        "properties": {
          "product_id": {
            "type": "keyword"
          },
          "product_name": {
            "type": "text",
            "analyzer": "standard_analyzer",
            "fields": {
              "keyword": {
                "type": "keyword"
              }
            }
          },
          "product_class": {
            "type": "text",
            "analyzer": "standard_analyzer",
            "fields": {
              "keyword": {
                "type": "keyword"
              }
            }
          },
          "product_description": {
            "type": "text",
            "analyzer": "standard_analyzer"
          },
          "product_search": {
            "type": "text",
            "analyzer": "standard_analyzer"
          },
          "product_features": {
            "type": "text",
            "analyzer": "standard_analyzer"
          },
          "product_embedding": {
            "type": "knn_vector",
            "dimension": 384,
            "method": {
              "name": "hnsw",
              "space_type": "l2",
              "engine": "lucene"
            }
          }
        }
      }
    }')
    
    if echo "$RESPONSE" | grep -q '"acknowledged":true'; then
        echo -e "${GREEN}✓ Index created successfully${NC}"
    else
        echo -e "${RED}✗ Failed to create index${NC}"
        echo "Response: $RESPONSE"
        exit 1
    fi
fi

# 7. Index sample documents from WANDS
echo ""
echo "Preparing to index WANDS products..."

# Check if WANDS dataset exists
WANDS_PATH="/Users/gaievski/dev/datasets/WANDS"
if [ ! -d "$WANDS_PATH" ]; then
    echo -e "${YELLOW}⚠ WANDS dataset not found at $WANDS_PATH${NC}"
    echo "  Please download the WANDS dataset first"
    echo "  Visit: https://github.com/wayfair/WANDS"
else
    echo "  Loading products from WANDS dataset..."
    
    # Get the directory where this script is located
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    
    # Check if the index_wands_documents.py script exists
    if [ ! -f "$SCRIPT_DIR/index_wands_documents.py" ]; then
        echo -e "${RED}✗ index_wands_documents.py not found in $SCRIPT_DIR${NC}"
        echo "  Please ensure index_wands_documents.py is in the same directory as this script"
        exit 1
    fi
    
    # Run the indexing script
    python3 "$SCRIPT_DIR/index_wands_documents.py" \
        --host "$HOST" \
        --port "$PORT" \
        --index "$INDEX_NAME" \
        --sample-size "$SAMPLE_SIZE" \
        --wands-path "$WANDS_PATH"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ WANDS products indexed successfully${NC}"
        
        # Check document count
        DOC_COUNT=$(curl -s $HOST:$PORT/$INDEX_NAME/_count 2>/dev/null | jq -r '.count' 2>/dev/null || echo "0")
        echo "  Total documents in index: $DOC_COUNT"
    else
        echo -e "${RED}✗ Failed to index WANDS products${NC}"
        echo "  Run the indexing script directly for more details:"
        echo "  python3 $SCRIPT_DIR/index_wands_documents.py --help"
    fi
fi

# 8. Test search capability
echo ""
echo "Testing search capability..."

# Test keyword search
TEST_RESPONSE=$(curl -s -X POST "$HOST:$PORT/$INDEX_NAME/_search" \
-H "Content-Type: application/json" \
-d '{
  "size": 1,
  "query": {
    "multi_match": {
      "query": "laptop",
      "fields": ["product_name^3", "product_class^2", "product_description"]
    }
  }
}' 2>/dev/null)

if echo "$TEST_RESPONSE" | grep -q '"hits"'; then
    echo -e "${GREEN}✓ Keyword search is working${NC}"
else
    echo -e "${YELLOW}⚠ Keyword search test failed${NC}"
fi

# Test hybrid search if model is available
if [ -n "$model_id" ] && [ "$model_id" != "null" ]; then
    TEST_RESPONSE=$(curl -s -X POST "$HOST:$PORT/$INDEX_NAME/_search" \
    -H "Content-Type: application/json" \
    -d "{
      \"size\": 1,
      \"query\": {
        \"hybrid\": {
          \"queries\": [
            {\"multi_match\": {\"query\": \"laptop\", \"fields\": [\"product_name\", \"product_description\"]}},
            {\"neural\": {\"embedding\": {\"query_text\": \"laptop computer\", \"model_id\": \"$model_id\", \"k\": 5}}}
          ]
        }
      }
    }" 2>/dev/null)
    
    if echo "$TEST_RESPONSE" | grep -q '"hits"'; then
        echo -e "${GREEN}✓ Hybrid search is working${NC}"
    else
        echo -e "${YELLOW}⚠ Hybrid search not available${NC}"
    fi
fi

# 9. Summary
echo ""
echo "============================================================"
echo "                    Setup Complete!"
echo "============================================================"
echo ""
echo "Index Information:"
echo "  Name: $INDEX_NAME"
echo "  Documents: $DOC_COUNT"
if [ -n "$model_id" ] && [ "$model_id" != "null" ]; then
    echo "  Model ID: $model_id"
    echo "  Vector Search: Enabled"
else
    echo "  Vector Search: Not available"
fi
