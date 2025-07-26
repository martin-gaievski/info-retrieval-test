# Quick Start Guide

Get dynamic hybrid search running in 15 minutes!

## 1. Prerequisites Check

```bash
# Check Python version (need 3.8+)
python --version

# Check Docker is running
docker ps

# Check available memory (need 16GB+)
free -h  # Linux
# or
sysctl hw.memsize  # macOS
```

## 2. Clone and Setup

```bash
# Clone repository
git clone https://github.com/opensearch-project/dynamic-hybrid-search.git
cd dynamic-hybrid-search

# Install dependencies
pip install -r requirements.txt
```

## 3. Start OpenSearch

```bash
# Launch OpenSearch with neural search
./setup_opensearch_for_poc.sh

# Wait for cluster to be ready (check http://localhost:9200)
curl -X GET "localhost:9200/_cluster/health?pretty"
```

## 4. Quick Test with SciFact Dataset

SciFact is small (5K docs) and perfect for testing:

```bash
# Ingest data and create neural model
python -m beir.hybrid.data_ingestor \
    --dataset scifact \
    --index scifact-index \
    --model sentence-transformers/all-MiniLM-L6-v2

# Note the model_id from output (looks like: pCEwOZgBtmNHHhH7ZtEE)
```

## 5. Run Evaluation

```bash
# Simple evaluation
python evaluate_dynamic_hybrid_standalone.py \
    --dataset scifact \
    --url https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip \
    --index scifact-index \
    --model-id YOUR_MODEL_ID \
    --output results/scifact_quickstart.json

# With comparison to static weights
python evaluate_dynamic_hybrid_standalone.py \
    --dataset scifact \
    --url https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip \
    --index scifact-index \
    --model-id YOUR_MODEL_ID \
    --output results/scifact_comparison.json \
    --compare \
    --static-weights "0.6,0.4" "0.7,0.3" "0.5,0.5"
```

## 6. View Results

```bash
# Check performance metrics
cat results/scifact_quickstart.json | jq '.average_metrics'

# Example output:
# {
#   "ndcg@10": 0.6543,
#   "map@10": 0.6234,
#   "recall@10": 0.8012,
#   "precision@10": 0.0834
# }
```

## 🎉 Success!

You've just run dynamic hybrid search! The system automatically adjusted search weights per query to optimize performance.

## Next Steps

1. **Try larger datasets**: FiQA (financial), SciDocs (scientific), NFCorpus (medical)
2. **Train ML models**: Use `train_weight_predictor.py` with `--use-ml` flag
3. **Exhaustive comparison**: Compare against all possible static weights
4. **Production setup**: Configure multi-node OpenSearch cluster

## Troubleshooting

### OpenSearch won't start
- Check Docker memory settings (need 4GB+ for Docker)
- Ensure ports 9200, 9600 are free
- Try `docker system prune` to clean up

### Model upload fails
- Ensure ML nodes enabled in OpenSearch
- Check cluster has enough heap memory
- Verify neural-search plugin installed

### Low performance scores
- Ensure correct model_id used
- Check index has documents (min 1000 recommended)
- Verify both lexical and neural fields indexed

## Common Commands

```bash
# Check OpenSearch status
curl localhost:9200/_cat/health?v

# List indices
curl localhost:9200/_cat/indices?v

# Check ML models
curl localhost:9200/_plugins/_ml/models/_search?pretty

# Delete an index (careful!)
curl -X DELETE localhost:9200/scifact-index
