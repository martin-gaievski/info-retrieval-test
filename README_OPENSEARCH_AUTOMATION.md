# OpenSearch IR Experiment Automation Framework

## Overview

This automation framework streamlines the setup and execution of Information Retrieval (IR) experiments using OpenSearch. It handles the complete lifecycle of experiments including:

- OpenSearch index creation with custom fields and KNN settings
- ML model deployment (local and remote)
- Ingest pipeline configuration for automated embedding generation
- Flexible resource reuse across multiple experiments
- Integration with BEIR evaluation framework for metrics calculation

## Architecture

The framework consists of three main components:

1. **opensearch_experiment_automation.py** - Core automation for OpenSearch resource management
2. **run_ir_experiment.py** - Integrated experiment runner combining automation with BEIR evaluation
3. **Configuration files** - JSON-based experiment configurations for flexibility

## Installation

### Prerequisites

```bash
# Install OpenSearch Python client
pip install opensearch-py

# Ensure BEIR framework is installed (already in your environment)
# pip install beir
```

### OpenSearch Setup

Ensure OpenSearch is running with the required plugins:
- ML Commons plugin for model management
- KNN plugin for vector search

## Configuration

### Basic Configuration Structure

```json
{
  "name": "experiment_name",
  "opensearch": {
    "host": "localhost",
    "port": 9200,
    "use_ssl": false,
    "verify_certs": false,
    "timeout": 30
  },
  "index": {
    "name": "index_name",
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "knn_enabled": true,
    "vector_dimension": 768,
    "vector_method": "hnsw",
    "vector_space_type": "l2"
  },
  "model": {
    "name": "model_name",
    "model_type": "LOCAL_TEXT_EMBEDDING",
    "model_url": "https://...",
    "deploy_on_create": true
  },
  "pipeline": {
    "name": "pipeline_name",
    "input_field": "passage_text",
    "output_field": "passage_embedding"
  }
}
```

### Resource Reuse Configuration

For subsequent experiments reusing existing resources:

```json
{
  "name": "second_experiment",
  "opensearch": {...},
  "index": {...},
  "reuse_model": true,
  "reuse_model_id": "MODEL_ID_FROM_FIRST_EXPERIMENT",
  "reuse_pipeline": true,
  "reuse_pipeline_name": "existing_pipeline_name",
  "skip_model_deployment": true,
  "skip_pipeline_creation": true
}
```

## Usage Examples

### 1. Setup OpenSearch Resources Only

```bash
# First experiment - create all resources
python opensearch_experiment_automation.py \
  --config configs/experiment_first_dataset.json

# Second experiment - reuse model and pipeline
python opensearch_experiment_automation.py \
  --config configs/experiment_second_dataset.json
```

### 2. Run Complete IR Experiment

```bash
# Run full experiment with arguana dataset
python run_ir_experiment.py \
  --config configs/experiment_first_dataset.json \
  --dataset arguana \
  --methods bm25,neural,hybrid \
  --k-values 5,10,100 \
  --output results/arguana_results.json

# Run with existing infrastructure (skip setup)
python run_ir_experiment.py \
  --config configs/experiment_first_dataset.json \
  --dataset fiqa \
  --skip-setup \
  --skip-ingest \
  --methods hybrid \
  --output results/fiqa_results.json
```

### 3. Multi-Dataset Experiments

```bash
# First dataset - full setup
python run_ir_experiment.py \
  --config configs/experiment_first_dataset.json \
  --dataset nfcorpus \
  --output results/nfcorpus_results.json

# Second dataset - reuse model and pipeline
python run_ir_experiment.py \
  --config configs/experiment_second_dataset.json \
  --dataset trec-covid \
  --output results/trec_covid_results.json

# Third dataset - only create new index
python run_ir_experiment.py \
  --config configs/experiment_third_dataset.json \
  --dataset arguana \
  --output results/arguana_results.json
```

### 4. Cleanup Resources

```bash
# Clean up specific resources
python opensearch_experiment_automation.py \
  --config configs/experiment_first_dataset.json \
  --cleanup \
  --cleanup-index \
  --cleanup-pipeline

# Clean up all resources after experiment
python run_ir_experiment.py \
  --config configs/experiment_first_dataset.json \
  --dataset nfcorpus \
  --cleanup \
  --cleanup-all
```

## Command Line Options

### opensearch_experiment_automation.py

| Option | Description |
|--------|-------------|
| `--config` | Path to experiment configuration JSON file (required) |
| `--cleanup` | Clean up resources after experiment |
| `--cleanup-index` | Delete index during cleanup |
| `--cleanup-model` | Delete model during cleanup |
| `--cleanup-pipeline` | Delete pipeline during cleanup |
| `--verbose` | Enable verbose logging |

### run_ir_experiment.py

| Option | Description | Default |
|--------|-------------|---------|
| `--config` | Path to experiment configuration JSON file (required) | - |
| `--dataset` | Dataset name (e.g., nfcorpus, trec-covid) (required) | - |
| `--data-path` | Path to dataset (will download if not provided) | Auto-download |
| `--methods` | Comma-separated list of search methods | bm25,neural,hybrid |
| `--k-values` | Comma-separated list of k values for evaluation | 5,10,100 |
| `--num-runs` | Number of runs for timing evaluation | 1 |
| `--skip-setup` | Skip infrastructure setup | False |
| `--skip-ingest` | Skip data ingestion | False |
| `--output` | Output file for results | experiment_results.json |
| `--cleanup` | Clean up resources after experiment | False |
| `--cleanup-all` | Delete all resources during cleanup | False |
| `--verbose` | Enable verbose logging | False |

## Supported Models

### Local Text Embedding Models

The framework supports various sentence-transformer models from the OpenSearch model repository:

- **MS MARCO DistilBERT**: Good balance of speed and accuracy
  ```json
  "model_url": "https://artifacts.opensearch.org/models/ml-models/huggingface/sentence-transformers/msmarco-distilbert-base-tas-b/1.0.2/torch_script/sentence-transformers_msmarco-distilbert-base-tas-b-1.0.2-torch_script.zip"
  ```

- **All-MiniLM-L6**: Faster, smaller model
  ```json
  "model_url": "https://artifacts.opensearch.org/models/ml-models/huggingface/sentence-transformers/all-MiniLM-L6-v2/1.0.2/torch_script/sentence-transformers_all-MiniLM-L6-v2-1.0.2-torch_script.zip"
  ```

### Remote Models (Future Support)

The framework is designed to support remote models via connectors:
- OpenAI embeddings
- Cohere embeddings
- Custom API endpoints

## Evaluation Metrics

The framework calculates standard IR metrics:
- **NDCG** (Normalized Discounted Cumulative Gain) @ k
- **MAP** (Mean Average Precision) @ k
- **Recall** @ k
- **Precision** @ k

## Best Practices

### 1. Resource Management

- **First Experiment**: Create all resources (index, model, pipeline)
- **Subsequent Experiments**: Reuse model and pipeline, only create new indices
- **Model Deployment**: Deploy once, reuse across experiments
- **Pipeline Reuse**: Same pipeline can serve multiple indices

### 2. Performance Optimization

- **Batch Size**: Adjust ingestion batch size based on document size
- **Warmup Queries**: Run warmup queries before evaluation (built-in)
- **Multiple Runs**: Use `--num-runs` > 1 for timing measurements

### 3. Experiment Organization

```
configs/
├── base_config.json           # Base configuration
├── dataset1_config.json       # First dataset
├── dataset2_config.json       # Second dataset (reuse model)
└── dataset3_config.json       # Third dataset (reuse model)

results/
├── dataset1_results.json
├── dataset2_results.json
└── dataset3_results.json
```

## Troubleshooting

### Common Issues

1. **Model Registration Fails**
   - Ensure ML Commons plugin is installed
   - Check model URL is accessible
   - Verify sufficient memory for model loading

2. **Pipeline Creation Fails**
   - Ensure model is deployed before creating pipeline
   - Check model_id is correct
   - Verify field mappings match index schema

3. **Index Already Exists**
   - Use different index name or delete existing
   - Set `skip_index_creation: true` to use existing

4. **Timeout Errors**
   - Increase timeout in configuration
   - Check OpenSearch cluster health
   - Ensure sufficient resources

### Debug Mode

Enable verbose logging for detailed troubleshooting:
```bash
python run_ir_experiment.py --config config.json --dataset nfcorpus --verbose
```

## Example Workflow

### Complete Multi-Dataset Experiment

```bash
# 1. First dataset - full setup (creates model and pipeline)
python run_ir_experiment.py \
  --config configs/experiment_first_dataset.json \
  --dataset nfcorpus \
  --methods bm25,neural,hybrid \
  --output results/nfcorpus_results.json

# Note the model_id from output, e.g., "model_id": "abc123"

# 2. Update second config with model_id
# Edit configs/experiment_second_dataset.json:
# "reuse_model_id": "abc123"

# 3. Second dataset - reuse model
python run_ir_experiment.py \
  --config configs/experiment_second_dataset.json \
  --dataset trec-covid \
  --methods bm25,neural,hybrid \
  --output results/trec_covid_results.json

# 4. Third dataset - reuse model  
python run_ir_experiment.py \
  --config configs/experiment_third_dataset.json \
  --dataset arguana \
  --methods bm25,neural,hybrid \
  --output results/arguana_results.json

# 5. Compare results
python compare_results.py \
  results/nfcorpus_results.json \
  results/trec_covid_results.json \
  results/arguana_results.json
```

## Integration with Existing Code

The automation framework integrates seamlessly with your existing BEIR evaluation code:

```python
from opensearch_experiment_automation import OpenSearchExperimentManager
from beir.hybrid.search import RetrievalOpenSearch

# Setup infrastructure
config = load_config_from_file("config.json")
manager = OpenSearchExperimentManager(config)
results = manager.setup_experiment()

# Use with existing BEIR code
os_retrieval = RetrievalOpenSearch(
    endpoint=config.opensearch.host,
    port=str(config.opensearch.port),
    index_name=config.index.name,
    model_id=results['model_id'],
    search_method='hybrid',
    pipeline_name=results['pipeline']
)
```

## Advanced Features

### Custom Field Mappings

Define custom fields in configuration:

```json
"fields": {
  "passage_text": {"type": "text"},
  "title": {"type": "text", "analyzer": "english"},
  "metadata": {"type": "object"},
  "passage_embedding": {
    "type": "knn_vector",
    "dimension": 768,
    "method": {
      "name": "hnsw",
      "space_type": "cosinesimil",
      "engine": "nmslib",
      "parameters": {
        "ef_construction": 512,
        "m": 16
      }
    }
  }
}
```

### Multiple Pipelines

Configure different pipelines for different processing:

```json
"pipeline": {
  "name": "multilingual_pipeline",
  "model_id": "multilingual_model_id",
  "input_field": "text",
  "output_field": "text_embedding"
}
```

## Contributing

To extend the framework:

1. Add new model types in `ModelType` enum
2. Implement remote connector support in `ModelConfig`
3. Add new evaluation metrics in `IRExperimentRunner`
4. Create custom ingest processors in `IngestPipelineConfig`

## License

This framework is built on top of the BEIR evaluation framework and follows the same licensing terms.
