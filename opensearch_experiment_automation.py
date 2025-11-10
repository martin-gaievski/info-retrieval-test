#!/usr/bin/env python3
"""
OpenSearch Experiment Automation Framework for Information Retrieval

This framework automates the setup and management of OpenSearch resources for IR experiments:
- Index creation with custom fields and settings
- ML model deployment (local and remote)  
- Ingest pipeline configuration
- Flexible resource reuse across experiments
"""

import json
import logging
import argparse
import time
import sys
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
from opensearchpy import OpenSearch, RequestsHttpConnection, exceptions

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class ModelType(Enum):
    """Supported model types"""
    LOCAL_TEXT_EMBEDDING = "text_embedding"
    REMOTE_TEXT_EMBEDDING = "remote_text_embedding"
    REMOTE_LLM = "remote_llm"


@dataclass
class OpenSearchConfig:
    """OpenSearch connection configuration"""
    host: str = "localhost"
    port: int = 9200
    use_ssl: bool = False
    verify_certs: bool = False
    timeout: int = 30
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class IndexConfig:
    """Index configuration"""
    name: str
    number_of_shards: int = 1
    number_of_replicas: int = 0
    knn_enabled: bool = True
    vector_dimension: int = 768
    vector_method: str = "hnsw"
    vector_space_type: str = "l2"
    default_pipeline: Optional[str] = None
    fields: Dict[str, Dict] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set default fields if not provided"""
        if not self.fields:
            self.fields = {
                "passage_text": {"type": "text"},
                "text_key": {"type": "text"},
                "title_key": {"type": "text"},
                "passage_embedding": {
                    "type": "knn_vector",
                    "dimension": self.vector_dimension,
                    "method": {
                        "name": self.vector_method,
                        "space_type": self.vector_space_type,
                        "engine": "lucene"
                    }
                }
            }


@dataclass
class ModelConfig:
    """ML Model configuration"""
    name: str
    model_type: ModelType
    model_format: str = "TORCH_SCRIPT"
    model_group_id: Optional[str] = None
    description: str = "Text embedding model for IR experiments"
    version: str = "1.0"
    
    # For local models
    model_url: Optional[str] = None
    model_config_url: Optional[str] = None
    
    # For remote models
    connector_id: Optional[str] = None
    
    # Common settings
    deploy_on_create: bool = True
    
    def get_local_model_body(self) -> Dict:
        """Get request body for local model registration"""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "model_format": self.model_format,
            "model_group_id": self.model_group_id,
            "model_content_hash_value": "e13b74006290a9d0f58c1376f9629d4ebc05a0f9385f40db837452b167ae9021",
            "model_config": {
                "model_type": self.model_type.value,
                "embedding_dimension": 768,
                "framework_type": "sentence_transformers"
            },
            "url": self.model_url
        }


@dataclass
class IngestPipelineConfig:
    """Ingest pipeline configuration"""
    name: str
    model_id: str
    input_field: str = "passage_text"
    output_field: str = "passage_embedding"
    description: str = "Neural ingest pipeline for text embeddings"


@dataclass
class ExperimentConfig:
    """Complete experiment configuration"""
    name: str
    opensearch: OpenSearchConfig
    index: IndexConfig
    model: Optional[ModelConfig] = None
    pipeline: Optional[IngestPipelineConfig] = None
    reuse_model: bool = False
    reuse_model_id: Optional[str] = None
    reuse_pipeline: bool = False
    reuse_pipeline_name: Optional[str] = None
    skip_index_creation: bool = False
    skip_model_deployment: bool = False
    skip_pipeline_creation: bool = False


class OpenSearchExperimentManager:
    """Manages OpenSearch resources for IR experiments"""
    
    def __init__(self, config: ExperimentConfig):
        self.config = config
        self.client = self._create_client()
        self.model_id = config.reuse_model_id
        self.pipeline_name = config.reuse_pipeline_name
        
    def _create_client(self) -> OpenSearch:
        """Create OpenSearch client"""
        auth = None
        if self.config.opensearch.username and self.config.opensearch.password:
            auth = (self.config.opensearch.username, self.config.opensearch.password)
            
        return OpenSearch(
            hosts=[{
                'host': self.config.opensearch.host,
                'port': self.config.opensearch.port
            }],
            http_auth=auth,
            use_ssl=self.config.opensearch.use_ssl,
            verify_certs=self.config.opensearch.verify_certs,
            connection_class=RequestsHttpConnection,
            timeout=self.config.opensearch.timeout
        )
    
    def check_cluster_health(self) -> bool:
        """Check if OpenSearch cluster is healthy"""
        try:
            health = self.client.cluster.health()
            logger.info(f"Cluster health: {health['status']}")
            return health['status'] in ['green', 'yellow']
        except Exception as e:
            logger.error(f"Failed to check cluster health: {e}")
            return False
    
    def create_ml_model_group(self) -> str:
        """Create ML model group"""
        try:
            body = {
                "name": f"model_group_{self.config.name}",
                "description": f"Model group for experiment {self.config.name}"
            }
            response = self.client.transport.perform_request(
                "POST",
                "/_plugins/_ml/model_groups/_register",
                body=body
            )
            model_group_id = response['model_group_id']
            logger.info(f"Created model group: {model_group_id}")
            return model_group_id
        except Exception as e:
            logger.error(f"Failed to create model group: {e}")
            raise
    
    def register_local_model(self, model_config: ModelConfig) -> str:
        """Register a local ML model"""
        try:
            # Create model group if not provided
            if not model_config.model_group_id:
                model_config.model_group_id = self.create_ml_model_group()
            
            # Register model
            body = model_config.get_local_model_body()
            response = self.client.transport.perform_request(
                "POST",
                "/_plugins/_ml/models/_register",
                body=body
            )
            
            task_id = response['task_id']
            logger.info(f"Model registration task started: {task_id}")
            
            # Wait for registration to complete
            model_id = self._wait_for_task_completion(task_id)
            logger.info(f"Model registered successfully: {model_id}")
            
            return model_id
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise
    
    def _wait_for_task_completion(self, task_id: str, max_retries: int = 30) -> str:
        """Wait for ML task to complete and return model_id"""
        for i in range(max_retries):
            try:
                response = self.client.transport.perform_request(
                    "GET",
                    f"/_plugins/_ml/tasks/{task_id}"
                )
                
                if response['state'] == 'COMPLETED':
                    return response['model_id']
                elif response['state'] == 'FAILED':
                    raise Exception(f"Task failed: {response.get('error', 'Unknown error')}")
                
                time.sleep(2)
            except Exception as e:
                if i == max_retries - 1:
                    raise Exception(f"Task monitoring failed: {e}")
                time.sleep(2)
        
        raise Exception("Task timeout")
    
    def deploy_model(self, model_id: str) -> bool:
        """Deploy ML model"""
        try:
            response = self.client.transport.perform_request(
                "POST",
                f"/_plugins/_ml/models/{model_id}/_deploy"
            )
            
            task_id = response['task_id']
            logger.info(f"Model deployment task started: {task_id}")
            
            # Wait for deployment
            self._wait_for_deployment(task_id)
            logger.info(f"Model deployed successfully: {model_id}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to deploy model: {e}")
            raise
    
    def _wait_for_deployment(self, task_id: str, max_retries: int = 30):
        """Wait for model deployment to complete"""
        for i in range(max_retries):
            try:
                response = self.client.transport.perform_request(
                    "GET",
                    f"/_plugins/_ml/tasks/{task_id}"
                )
                
                if response['state'] == 'COMPLETED':
                    return
                elif response['state'] == 'FAILED':
                    raise Exception(f"Deployment failed: {response.get('error', 'Unknown error')}")
                
                time.sleep(2)
            except Exception as e:
                if i == max_retries - 1:
                    raise Exception(f"Deployment monitoring failed: {e}")
                time.sleep(2)
    
    def create_index(self, index_config: IndexConfig) -> bool:
        """Create index with specified configuration"""
        try:
            # Check if index exists
            if self.client.indices.exists(index=index_config.name):
                logger.warning(f"Index {index_config.name} already exists")
                return True
            
            # Prepare index body
            body = {
                "settings": {
                    "number_of_shards": index_config.number_of_shards,
                    "number_of_replicas": index_config.number_of_replicas,
                    "index.knn": index_config.knn_enabled
                },
                "mappings": {
                    "properties": index_config.fields
                }
            }
            
            # Add default pipeline if specified
            if index_config.default_pipeline:
                body["settings"]["default_pipeline"] = index_config.default_pipeline
            
            # Create index
            response = self.client.indices.create(index=index_config.name, body=body)
            logger.info(f"Index created: {index_config.name}")
            
            return response['acknowledged']
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            raise
    
    def create_ingest_pipeline(self, pipeline_config: IngestPipelineConfig) -> bool:
        """Create ingest pipeline for text embedding"""
        try:
            body = {
                "description": pipeline_config.description,
                "processors": [
                    {
                        "text_embedding": {
                            "model_id": pipeline_config.model_id,
                            "field_map": {
                                pipeline_config.input_field: pipeline_config.output_field
                            }
                        }
                    }
                ]
            }
            
            response = self.client.ingest.put_pipeline(
                id=pipeline_config.name,
                body=body
            )
            
            logger.info(f"Ingest pipeline created: {pipeline_config.name}")
            return response['acknowledged']
        except Exception as e:
            logger.error(f"Failed to create ingest pipeline: {e}")
            raise
    
    def setup_experiment(self) -> Dict[str, str]:
        """Set up complete experiment environment"""
        results = {
            "experiment_name": self.config.name,
            "status": "success"
        }
        
        try:
            # Check cluster health
            if not self.check_cluster_health():
                raise Exception("Cluster is not healthy")
            
            # Create or reuse index
            if not self.config.skip_index_creation:
                self.create_index(self.config.index)
                results["index"] = self.config.index.name
            else:
                logger.info(f"Skipping index creation for {self.config.index.name}")
            
            # Deploy or reuse model
            if not self.config.skip_model_deployment:
                if self.config.reuse_model and self.config.reuse_model_id:
                    self.model_id = self.config.reuse_model_id
                    logger.info(f"Reusing existing model: {self.model_id}")
                else:
                    if self.config.model:
                        self.model_id = self.register_local_model(self.config.model)
                        if self.config.model.deploy_on_create:
                            self.deploy_model(self.model_id)
                results["model_id"] = self.model_id
            else:
                logger.info("Skipping model deployment")
            
            # Create or reuse ingest pipeline
            if not self.config.skip_pipeline_creation:
                if self.config.reuse_pipeline and self.config.reuse_pipeline_name:
                    self.pipeline_name = self.config.reuse_pipeline_name
                    logger.info(f"Reusing existing pipeline: {self.pipeline_name}")
                else:
                    if self.config.pipeline:
                        # Update pipeline with actual model_id
                        self.config.pipeline.model_id = self.model_id
                        self.create_ingest_pipeline(self.config.pipeline)
                        self.pipeline_name = self.config.pipeline.name
                        
                        # Update index with default pipeline
                        if self.config.index.default_pipeline != self.pipeline_name:
                            self._update_index_default_pipeline(
                                self.config.index.name, 
                                self.pipeline_name
                            )
                results["pipeline"] = self.pipeline_name
            else:
                logger.info("Skipping pipeline creation")
            
            logger.info(f"Experiment setup completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Experiment setup failed: {e}")
            results["status"] = "failed"
            results["error"] = str(e)
            return results
    
    def _update_index_default_pipeline(self, index_name: str, pipeline_name: str):
        """Update index settings to use default pipeline"""
        try:
            self.client.indices.put_settings(
                index=index_name,
                body={
                    "default_pipeline": pipeline_name
                }
            )
            logger.info(f"Updated index {index_name} with default pipeline {pipeline_name}")
        except Exception as e:
            logger.warning(f"Failed to update index default pipeline: {e}")
    
    def cleanup_resources(self, delete_index: bool = False, 
                         delete_model: bool = False,
                         delete_pipeline: bool = False):
        """Clean up experiment resources"""
        try:
            if delete_pipeline and self.pipeline_name:
                self.client.ingest.delete_pipeline(id=self.pipeline_name)
                logger.info(f"Deleted pipeline: {self.pipeline_name}")
            
            if delete_model and self.model_id:
                # Undeploy model first
                self.client.transport.perform_request(
                    "POST",
                    f"/_plugins/_ml/models/{self.model_id}/_undeploy"
                )
                time.sleep(2)
                # Delete model
                self.client.transport.perform_request(
                    "DELETE",
                    f"/_plugins/_ml/models/{self.model_id}"
                )
                logger.info(f"Deleted model: {self.model_id}")
            
            if delete_index and self.config.index.name:
                self.client.indices.delete(index=self.config.index.name)
                logger.info(f"Deleted index: {self.config.index.name}")
                
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")


def load_config_from_file(config_file: str) -> ExperimentConfig:
    """Load experiment configuration from JSON file"""
    with open(config_file, 'r') as f:
        config_dict = json.load(f)
    
    # Parse nested configurations
    opensearch_config = OpenSearchConfig(**config_dict.get('opensearch', {}))
    index_config = IndexConfig(**config_dict.get('index', {}))
    
    model_config = None
    if 'model' in config_dict:
        model_dict = config_dict['model']
        if 'model_type' in model_dict:
            model_dict['model_type'] = ModelType[model_dict['model_type']]
        model_config = ModelConfig(**model_dict)
    
    pipeline_config = None
    if 'pipeline' in config_dict:
        pipeline_config = IngestPipelineConfig(**config_dict['pipeline'])
    
    return ExperimentConfig(
        name=config_dict['name'],
        opensearch=opensearch_config,
        index=index_config,
        model=model_config,
        pipeline=pipeline_config,
        reuse_model=config_dict.get('reuse_model', False),
        reuse_model_id=config_dict.get('reuse_model_id'),
        reuse_pipeline=config_dict.get('reuse_pipeline', False),
        reuse_pipeline_name=config_dict.get('reuse_pipeline_name'),
        skip_index_creation=config_dict.get('skip_index_creation', False),
        skip_model_deployment=config_dict.get('skip_model_deployment', False),
        skip_pipeline_creation=config_dict.get('skip_pipeline_creation', False)
    )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='OpenSearch Experiment Automation for Information Retrieval'
    )
    parser.add_argument(
        '--config', 
        type=str, 
        required=True,
        help='Path to experiment configuration JSON file'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Clean up resources after experiment'
    )
    parser.add_argument(
        '--cleanup-index',
        action='store_true',
        help='Delete index during cleanup'
    )
    parser.add_argument(
        '--cleanup-model',
        action='store_true',
        help='Delete model during cleanup'
    )
    parser.add_argument(
        '--cleanup-pipeline',
        action='store_true',
        help='Delete pipeline during cleanup'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    
    # Load configuration
    try:
        config = load_config_from_file(args.config)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)
    
    # Create experiment manager
    manager = OpenSearchExperimentManager(config)
    
    # Setup experiment
    results = manager.setup_experiment()
    
    # Print results
    print(json.dumps(results, indent=2))
    
    # Cleanup if requested
    if args.cleanup:
        manager.cleanup_resources(
            delete_index=args.cleanup_index,
            delete_model=args.cleanup_model,
            delete_pipeline=args.cleanup_pipeline
        )
    
    sys.exit(0 if results['status'] == 'success' else 1)


if __name__ == "__main__":
    main()
