"""
LLM Judge for Inline Relevancy Scoring.

This module provides LLM-based relevancy judgment using AWS Bedrock (Claude)
or other LLM providers for scoring document relevance to queries.
"""

import json
import re
import time
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum


class RatingScale(Enum):
    """Available rating scales for relevancy scoring."""
    GRADED_5 = "graded_5"      # 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 (6 levels)
    GRADED_4 = "graded_4"      # 0.0, 0.25, 0.5, 0.75, 1.0 (5 levels)
    BINARY = "binary"          # 0.0, 1.0 (2 levels)


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
    
    Supports AWS Bedrock (Claude) and can be extended for other providers.
    Uses optimized prompts for deterministic relevancy scoring.
    """
    
    # Prompt template for relevancy judgment
    SYSTEM_PROMPT = """You are an expert search relevance evaluator. Your task is to rate how relevant each document is to a given search query.

Rating Scale (0.0 to 1.0 with 0.2 increments):
- 1.0: Perfect - Directly answers the query, highly relevant
- 0.8: Excellent - Very relevant, addresses most of the query
- 0.6: Good - Relevant content, partially addresses the query  
- 0.4: Fair - Some relevance but missing key aspects
- 0.2: Poor - Marginally relevant, mostly off-topic
- 0.0: Not Relevant - Completely unrelated to the query

Instructions:
1. Evaluate each document independently
2. Consider semantic meaning, not just keyword matches
3. Output ONLY a JSON object with document IDs as keys and ratings as values
4. Do not include any explanation - only the JSON object"""

    USER_PROMPT_TEMPLATE = """Query: {query}

Documents to evaluate:
{documents}

Output the ratings as a JSON object:"""

    def __init__(
        self,
        model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0",
        region: str = "us-west-2",
        rating_scale: RatingScale = RatingScale.GRADED_5,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        batch_size: int = 20
    ):
        """
        Initialize the LLM judge.
        
        Args:
            model_id: Bedrock model ID for Claude
            region: AWS region for Bedrock
            rating_scale: Rating scale to use
            temperature: LLM temperature (0 for deterministic)
            max_tokens: Maximum tokens for response
            batch_size: Documents per LLM call
        """
        self.model_id = model_id
        self.region = region
        self.rating_scale = rating_scale
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.batch_size = batch_size
        
        self._client = None
        
    @property
    def client(self):
        """Lazy initialization of Bedrock client."""
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client(
                    'bedrock-runtime',
                    region_name=self.region
                )
            except ImportError:
                raise ImportError("boto3 required for Bedrock integration. Install with: pip install boto3")
        return self._client
    
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
            title = doc.get(title_field, "") if title_field else ""
            content = doc.get(content_field, "")
            
            # Truncate long content
            if len(content) > 500:
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
        response = self._call_bedrock(user_prompt)
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
    
    def _call_bedrock(self, user_prompt: str) -> Dict:
        """Call Bedrock Claude API."""
        
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
        
        response = self.client.invoke_model(
            modelId=self.model_id,
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
        
    def _call_bedrock(self, user_prompt: str) -> Dict:
        """Generate mock response without API call."""
        self._call_count += 1
        
        # Extract doc IDs from prompt
        doc_ids = re.findall(r'\[([^\]]+)\]', user_prompt)
        
        # Generate consistent pseudo-random ratings
        ratings = {}
        for doc_id in doc_ids:
            # Use hash for consistency
            hash_val = hash(doc_id) % 100
            if hash_val < 20:
                rating = 0.0
            elif hash_val < 40:
                rating = 0.2
            elif hash_val < 60:
                rating = 0.4
            elif hash_val < 80:
                rating = 0.6
            elif hash_val < 95:
                rating = 0.8
            else:
                rating = 1.0
            ratings[doc_id] = rating
            
        return {
            'content': json.dumps(ratings),
            'total_tokens': 100 * len(doc_ids)
        }
