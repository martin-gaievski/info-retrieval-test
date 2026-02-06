"""
LLM Judge for Inline Relevancy Scoring.

This module provides LLM-based relevancy judgment using:
1. OpenSearch ML-Commons remote connector (recommended)
2. AWS Bedrock (Claude) direct connection (fallback)

Scoring document relevance to queries with optimized prompts.
"""

import json
import re
import time
import hashlib
import requests
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum


class RatingScale(Enum):
    """Available rating scales for relevancy scoring."""
    GRADED_5 = "graded_5"      # 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 (6 levels)
    GRADED_4 = "graded_4"      # 0.0, 0.25, 0.5, 0.75, 1.0 (5 levels)
    BINARY = "binary"          # 0.0, 1.0 (2 levels)


class LLMProvider(Enum):
    """LLM provider options."""
    ML_COMMONS = "ml_commons"  # OpenSearch ML-Commons remote connector
    BEDROCK = "bedrock"        # Direct AWS Bedrock
    MOCK = "mock"              # Mock for testing


@dataclass
class JudgmentResult:
    """Result of a single LLM judgment."""
    doc_id: str
    rating: float
    confidence: Optional[float] = None
    reasoning: Optional[str] = None
    raw_response: Optional[str] = None


@dataclass
class JudgmentBatch:
    """Batch of judgments for a single query."""
    query: str
    judgments: Dict[str, float]  # doc_id -> rating
    total_tokens: int = 0
    latency_ms: float = 0
    errors: List[str] = None


class LLMJudge:
    """
    LLM-based relevancy judge for search evaluation.
    
    Supports:
    - OpenSearch ML-Commons remote connector (calls deployed LLM via OpenSearch)
    - AWS Bedrock (Claude) direct connection
    
    Uses optimized prompts for deterministic relevancy scoring.
    """
    
    # Prompt template for relevancy judgment
    SYSTEM_PROMPT = """You are an expert search relevance evaluator for e-commerce product search. Your task is to rate how relevant each product is to a given search query.

Rating Scale (0.0 to 1.0 with 0.2 increments):
- 1.0: Perfect Match - Product exactly matches what the user is searching for
- 0.8: Excellent - Very relevant product, addresses the search intent well
- 0.6: Good - Relevant product, partially matches the search query
- 0.4: Fair - Some relevance but missing key attributes the user wants
- 0.2: Poor - Marginally related product, mostly off-topic
- 0.0: Not Relevant - Completely unrelated to the search query

Instructions:
1. Evaluate each product independently
2. Consider semantic meaning, not just keyword matches
3. Consider product attributes like brand, color, features in context of the query
4. Output ONLY a JSON object with document IDs as keys and ratings as values
5. Do not include any explanation - only the JSON object"""

    USER_PROMPT_TEMPLATE = """Search Query: {query}

Products to evaluate:
{documents}

Output the relevance ratings as a JSON object:"""

    def __init__(
        self,
        opensearch_url: str = None,
        llm_model_id: str = None,
        provider: LLMProvider = LLMProvider.ML_COMMONS,
        rating_scale: RatingScale = RatingScale.GRADED_5,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        batch_size: int = 20,
        # Bedrock fallback params
        bedrock_model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0",
        bedrock_region: str = "us-west-2",
        debug: bool = False
    ):
        """
        Initialize the LLM judge.
        
        Args:
            opensearch_url: OpenSearch base URL for ML-Commons (e.g., http://host:port)
            llm_model_id: LLM model ID deployed in ML-Commons
            provider: Which LLM provider to use
            rating_scale: Rating scale to use
            temperature: LLM temperature (0 for deterministic)
            max_tokens: Maximum tokens for response
            batch_size: Documents per LLM call
            bedrock_model_id: Bedrock model ID (for BEDROCK provider)
            bedrock_region: AWS region (for BEDROCK provider)
            debug: Enable debug logging
        """
        self.opensearch_url = opensearch_url
        self.llm_model_id = llm_model_id
        self.provider = provider
        self.rating_scale = rating_scale
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.batch_size = batch_size
        self.bedrock_model_id = bedrock_model_id
        self.bedrock_region = bedrock_region
        self._debug = debug
        
        self._bedrock_client = None
        self._session = requests.Session()
        
    @property
    def bedrock_client(self):
        """Lazy initialization of Bedrock client."""
        if self._bedrock_client is None:
            try:
                import boto3
                self._bedrock_client = boto3.client(
                    'bedrock-runtime',
                    region_name=self.bedrock_region
                )
            except ImportError:
                raise ImportError("boto3 required for Bedrock integration. Install with: pip install boto3")
        return self._bedrock_client
    
    def judge_documents(
        self,
        query: str,
        documents: Dict[str, Dict],
        content_field: str = "text",
        title_field: Optional[str] = "title"
    ) -> JudgmentBatch:
        """
        Judge relevancy of documents for a query.
        
        Args:
            query: The search query
            documents: Dict[doc_id] -> {document fields}
            content_field: Field name containing document content
            title_field: Field name containing document title (optional)
            
        Returns:
            JudgmentBatch with ratings for all documents
        """
        all_judgments = {}
        total_tokens = 0
        total_latency = 0
        errors = []
        
        # Process in batches
        doc_ids = list(documents.keys())
        for i in range(0, len(doc_ids), self.batch_size):
            batch_ids = doc_ids[i:i + self.batch_size]
            batch_docs = {did: documents[did] for did in batch_ids}
            
            try:
                batch_result = self._judge_batch(
                    query, batch_docs, content_field, title_field
                )
                all_judgments.update(batch_result.judgments)
                total_tokens += batch_result.total_tokens
                total_latency += batch_result.latency_ms
                if batch_result.errors:
                    errors.extend(batch_result.errors)
            except Exception as e:
                errors.append(f"Batch {i//self.batch_size}: {str(e)}")
                # Assign default score for failed documents
                for did in batch_ids:
                    if did not in all_judgments:
                        all_judgments[did] = 0.0
                        
        return JudgmentBatch(
            query=query,
            judgments=all_judgments,
            total_tokens=total_tokens,
            latency_ms=total_latency,
            errors=errors if errors else None
        )
    
    def _judge_batch(
        self,
        query: str,
        documents: Dict[str, Dict],
        content_field: str,
        title_field: Optional[str]
    ) -> JudgmentBatch:
        """Judge a single batch of documents."""
        
        # Format documents for prompt
        doc_texts = []
        for doc_id, doc in documents.items():
            # Handle None values - doc.get() can return None if key exists with None value
            title = (doc.get(title_field) or "") if title_field else ""
            content = doc.get(content_field) or ""
            
            # Truncate long content
            if content and len(content) > 500:
                content = content[:500] + "..."
                
            if title:
                doc_texts.append(f"[{doc_id}]\nTitle: {title}\nContent: {content}")
            else:
                doc_texts.append(f"[{doc_id}]\nContent: {content}")
                
        documents_text = "\n\n".join(doc_texts)
        
        user_prompt = self.USER_PROMPT_TEMPLATE.format(
            query=query,
            documents=documents_text
        )
        
        # Call LLM
        start_time = time.time()
        response = self._call_llm(user_prompt)
        latency_ms = (time.time() - start_time) * 1000
        
        # Parse response
        judgments, parse_errors = self._parse_response(
            response['content'], 
            list(documents.keys())
        )
        
        return JudgmentBatch(
            query=query,
            judgments=judgments,
            total_tokens=response.get('total_tokens', 0),
            latency_ms=latency_ms,
            errors=parse_errors if parse_errors else None
        )
    
    def _call_llm(self, user_prompt: str) -> Dict:
        """Call LLM based on configured provider."""
        if self.provider == LLMProvider.ML_COMMONS:
            return self._call_ml_commons(user_prompt)
        elif self.provider == LLMProvider.BEDROCK:
            return self._call_bedrock(user_prompt)
        else:
            raise ValueError(f"Unknown provider: {self.provider}")
    
    def _call_ml_commons(self, user_prompt: str) -> Dict:
        """Call LLM via OpenSearch ML-Commons remote connector."""
        
        if not self.opensearch_url or not self.llm_model_id:
            raise ValueError("opensearch_url and llm_model_id required for ML_COMMONS provider")
        
        # ML-Commons predict endpoint
        url = f"{self.opensearch_url}/_plugins/_ml/models/{self.llm_model_id}/_predict"
        
        # Build the prompt for remote connector
        full_prompt = f"{self.SYSTEM_PROMPT}\n\n{user_prompt}"
        
        # Request body for remote connector (Claude via Bedrock connector)
        request_body = {
            "parameters": {
                "messages": [
                    {
                        "role": "user",
                        "content": full_prompt
                    }
                ],
                "max_tokens": self.max_tokens,
                "temperature": self.temperature
            }
        }
        
        try:
            # Add timeout: 120s for connect, 300s for read (LLM can be slow)
            response = self._session.post(url, json=request_body, timeout=(120, 300))
            
            if response.status_code != 200:
                raise Exception(f"ML-Commons error: {response.status_code} - {response.text}")
            
            result = response.json()
            
            # DEBUG: Print raw response to diagnose parsing issues
            if self._debug:
                print(f"\n[DEBUG] ML-Commons raw response: {json.dumps(result, indent=2)[:2000]}")
            
            # Parse ML-Commons response (structure varies by connector)
            content = self._extract_content_from_ml_commons(result)
            
            if self._debug and content:
                print(f"[DEBUG] Extracted content: {content[:500]}...")
            
            return {
                'content': content,
                'total_tokens': 0  # ML-Commons doesn't always return token count
            }
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"ML-Commons request failed: {str(e)}")
    
    def _extract_content_from_ml_commons(self, result: Dict) -> str:
        """Extract content from various ML-Commons response formats."""
        
        inference_results = result.get('inference_results', [])
        if not inference_results:
            return ''
            
        output = inference_results[0].get('output', [])
        if not output:
            return ''
        
        # Try direct fields first
        for field in ['result', 'data', 'text', 'response', 'completion']:
            if output[0].get(field):
                return output[0].get(field)
        
        # dataAsMap format (most common for remote connectors)
        data_as_map = output[0].get('dataAsMap', {})
        if not data_as_map:
            return ''
        
        # Format 1: OpenAI-style (choices[0].message.content)
        # Used by GPT-3.5, GPT-4 via OpenAI-compatible connectors
        choices = data_as_map.get('choices', [])
        if choices and len(choices) > 0:
            message = choices[0].get('message', {})
            content = message.get('content', '')
            if content:
                return content
        
        # Format 2: Claude/Anthropic-style (content[0].text)
        if 'content' in data_as_map:
            content_arr = data_as_map['content']
            if isinstance(content_arr, list) and content_arr:
                if isinstance(content_arr[0], dict):
                    return content_arr[0].get('text', '')
                return str(content_arr[0])
            return str(content_arr)
        
        # Format 3: Simple response field
        for field in ['response', 'completion', 'text', 'message', 'output']:
            if data_as_map.get(field):
                val = data_as_map.get(field)
                if isinstance(val, str):
                    return val
                elif isinstance(val, dict) and 'content' in val:
                    return val['content']
        
        return ''
    
    def _call_bedrock(self, user_prompt: str) -> Dict:
        """Call Bedrock Claude API directly."""
        
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "messages": [
                {
                    "role": "user",
                    "content": f"{self.SYSTEM_PROMPT}\n\n{user_prompt}"
                }
            ]
        }
        
        response = self.bedrock_client.invoke_model(
            modelId=self.bedrock_model_id,
            body=json.dumps(request_body),
            contentType="application/json",
            accept="application/json"
        )
        
        response_body = json.loads(response['body'].read())
        
        return {
            'content': response_body.get('content', [{}])[0].get('text', ''),
            'total_tokens': (
                response_body.get('usage', {}).get('input_tokens', 0) +
                response_body.get('usage', {}).get('output_tokens', 0)
            )
        }
    
    def _parse_response(
        self,
        response_text: str,
        expected_doc_ids: List[str]
    ) -> Tuple[Dict[str, float], List[str]]:
        """Parse LLM response into ratings."""
        
        judgments = {}
        errors = []
        
        # Try to extract JSON from response
        try:
            # Find JSON object in response
            json_match = re.search(r'\{[^{}]*\}', response_text, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())
                
                for doc_id in expected_doc_ids:
                    if doc_id in parsed:
                        rating = float(parsed[doc_id])
                        # Clamp to valid range and round to scale
                        rating = max(0.0, min(1.0, rating))
                        rating = self._round_to_scale(rating)
                        judgments[doc_id] = rating
                    else:
                        errors.append(f"Missing rating for {doc_id}")
                        judgments[doc_id] = 0.0
            else:
                errors.append("No JSON found in response")
                for doc_id in expected_doc_ids:
                    judgments[doc_id] = 0.0
                    
        except json.JSONDecodeError as e:
            errors.append(f"JSON parse error: {str(e)}")
            for doc_id in expected_doc_ids:
                judgments[doc_id] = 0.0
        except Exception as e:
            errors.append(f"Parse error: {str(e)}")
            for doc_id in expected_doc_ids:
                judgments[doc_id] = 0.0
                
        return judgments, errors
    
    def _round_to_scale(self, rating: float) -> float:
        """Round rating to nearest value in scale."""
        
        if self.rating_scale == RatingScale.GRADED_5:
            # 0.0, 0.2, 0.4, 0.6, 0.8, 1.0
            return round(rating * 5) / 5
        elif self.rating_scale == RatingScale.GRADED_4:
            # 0.0, 0.25, 0.5, 0.75, 1.0
            return round(rating * 4) / 4
        elif self.rating_scale == RatingScale.BINARY:
            return 1.0 if rating >= 0.5 else 0.0
        else:
            return rating
    
    def estimate_cost(
        self,
        num_queries: int,
        avg_docs_per_query: int,
        avg_doc_length: int = 300
    ) -> Dict:
        """
        Estimate LLM costs for a judgment run.
        
        Args:
            num_queries: Number of queries to judge
            avg_docs_per_query: Average documents per query
            avg_doc_length: Average document length in characters
            
        Returns:
            Dict with cost estimates
        """
        # Approximate token counts
        tokens_per_char = 0.25  # Rough estimate
        system_tokens = len(self.SYSTEM_PROMPT) * tokens_per_char
        
        tokens_per_doc = avg_doc_length * tokens_per_char + 50  # +50 for formatting
        tokens_per_query = 20  # Query text tokens
        
        # Calculate per-query input tokens
        input_tokens_per_query = (
            system_tokens + 
            tokens_per_query + 
            (tokens_per_doc * avg_docs_per_query)
        )
        
        # Output tokens (JSON with ratings)
        output_tokens_per_query = avg_docs_per_query * 15  # ~15 tokens per rating
        
        # Total tokens
        total_input = input_tokens_per_query * num_queries
        total_output = output_tokens_per_query * num_queries
        
        # Claude Sonnet 3.5 pricing (approximate as of 2024)
        input_cost_per_1k = 0.003
        output_cost_per_1k = 0.015
        
        input_cost = (total_input / 1000) * input_cost_per_1k
        output_cost = (total_output / 1000) * output_cost_per_1k
        
        return {
            "num_queries": num_queries,
            "avg_docs_per_query": avg_docs_per_query,
            "total_input_tokens": int(total_input),
            "total_output_tokens": int(total_output),
            "estimated_input_cost": round(input_cost, 2),
            "estimated_output_cost": round(output_cost, 2),
            "total_estimated_cost": round(input_cost + output_cost, 2),
            "num_api_calls": num_queries * (avg_docs_per_query // self.batch_size + 1)
        }


class MockLLMJudge(LLMJudge):
    """
    Mock LLM judge for testing without actual API calls.
    
    Generates consistent pseudo-random ratings based on doc_id and query.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._call_count = 0
        
    def _call_llm(self, user_prompt: str) -> Dict:
        """Generate mock response without API call."""
        self._call_count += 1
        
        # Extract doc IDs from prompt - look for [B0XXXXX] pattern
        doc_ids = re.findall(r'\[([A-Z0-9]+)\]', user_prompt)
        
        if not doc_ids:
            # Fallback: try any bracketed content
            doc_ids = re.findall(r'\[([^\]]+)\]', user_prompt)
        
        # Generate consistent pseudo-random ratings based on doc_id
        # Use MD5 hash for deterministic results across Python runs
        # (Python's hash() is randomized per session for security)
        ratings = {}
        for doc_id in doc_ids:
            # Deterministic hash using MD5
            hash_bytes = hashlib.md5(doc_id.encode()).digest()
            hash_val = int.from_bytes(hash_bytes[:4], 'big') % 100
            
            if hash_val < 10:
                rating = 0.0
            elif hash_val < 25:
                rating = 0.2
            elif hash_val < 45:
                rating = 0.4
            elif hash_val < 70:
                rating = 0.6
            elif hash_val < 90:
                rating = 0.8
            else:
                rating = 1.0
            ratings[doc_id] = rating
            
        return {
            'content': json.dumps(ratings),
            'total_tokens': 100 * len(doc_ids)
        }
