"""
Output formatting utilities for standardized results.
Provides functions to save and format model outputs consistently.
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd


def create_metadata(
    model_name: str,
    model_type: str,
    dataset: str,
    training_samples: int,
    test_samples: int,
    features: list,
    hyperparameters: Dict[str, Any],
    performance_metrics: Dict[str, float],
    additional_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create standardized metadata dictionary for model.
    
    Args:
        model_name: Name of the model
        model_type: Type of model (ridge, neural, xgboost, etc.)
        dataset: Dataset name
        training_samples: Number of training samples
        test_samples: Number of test samples
        features: List of feature names
        hyperparameters: Model hyperparameters
        performance_metrics: Training/validation metrics
        additional_info: Any additional information
        
    Returns:
        Standardized metadata dictionary
    """
    metadata = {
        "model_name": model_name,
        "model_type": model_type,
        "dataset": dataset,
        "timestamp": datetime.now().isoformat(),
        "data": {
            "training_samples": training_samples,
            "test_samples": test_samples,
            "features": features,
            "num_features": len(features)
        },
        "hyperparameters": hyperparameters,
        "performance": performance_metrics,
        "version": "2.0"  # Version 2.0 indicates refactored standardized format
    }
    
    if additional_info:
        metadata["additional_info"] = additional_info
    
    return metadata


def save_model_outputs(
    model_name: str,
    model_object: Any,
    metadata: Dict[str, Any],
    training_data: Optional[pd.DataFrame] = None,
    output_dir: str = "dynamic_hybrid"
) -> Dict[str, str]:
    """
    Save model and related outputs in standardized format.
    
    Args:
        model_name: Base name for output files
        model_object: The trained model object
        metadata: Model metadata dictionary
        training_data: Optional training data DataFrame
        output_dir: Directory to save outputs
        
    Returns:
        Dictionary of saved file paths
    """
    saved_files = {}
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save model based on type
    model_type = metadata.get("model_type", "unknown")
    
    if model_type == "neural":
        # PyTorch model
        import torch
        model_path = os.path.join(output_dir, f"{model_name}.pth")
        torch.save(model_object, model_path)
        saved_files["model"] = model_path
    else:
        # Sklearn model (Ridge, RandomForest, etc.)
        import pickle
        model_path = os.path.join(output_dir, f"{model_name}.pkl")
        with open(model_path, 'wb') as f:
            pickle.dump(model_object, f)
        saved_files["model"] = model_path
    
    # Save metadata
    metadata_path = os.path.join(output_dir, f"{model_name}_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    saved_files["metadata"] = metadata_path
    
    # Save training data if provided
    if training_data is not None:
        data_path = os.path.join(output_dir, f"{model_name}_training_data.csv")
        training_data.to_csv(data_path, index=False)
        saved_files["training_data"] = data_path
    
    return saved_files


def save_evaluation_results(
    model_name: str,
    results: Dict[str, Any],
    output_dir: str = "dynamic_hybrid"
) -> str:
    """
    Save evaluation results in standardized format.
    
    Args:
        model_name: Model name for file naming
        results: Evaluation results dictionary
        output_dir: Directory to save results
        
    Returns:
        Path to saved results file
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Add timestamp to results
    results["timestamp"] = datetime.now().isoformat()
    results["model_name"] = model_name
    
    # Save results
    results_path = os.path.join(output_dir, f"{model_name}_evaluation.json")
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    return results_path


def format_training_summary(
    model_name: str,
    model_type: str,
    training_metrics: Dict[str, float],
    weight_distribution: Optional[Dict[float, int]] = None,
    feature_importance: Optional[Dict[str, float]] = None
) -> str:
    """
    Format training summary for console output.
    
    Args:
        model_name: Name of the model
        model_type: Type of model
        training_metrics: Training performance metrics
        weight_distribution: Optional weight distribution
        feature_importance: Optional feature importance scores
        
    Returns:
        Formatted string summary
    """
    lines = []
    lines.append("=" * 70)
    lines.append(f"TRAINING SUMMARY - {model_name}")
    lines.append(f"Model Type: {model_type}")
    lines.append("=" * 70)
    
    # Training metrics
    lines.append("\nTraining Metrics:")
    lines.append("-" * 40)
    for metric, value in training_metrics.items():
        if isinstance(value, float):
            lines.append(f"  {metric:<20}: {value:.4f}")
        else:
            lines.append(f"  {metric:<20}: {value}")
    
    # Weight distribution if available
    if weight_distribution:
        lines.append("\nWeight Distribution:")
        lines.append("-" * 40)
        for weight, count in sorted(weight_distribution.items()):
            percentage = count / sum(weight_distribution.values()) * 100
            lines.append(f"  {weight:.1f}: {count:3d} ({percentage:5.1f}%)")
    
    # Feature importance if available
    if feature_importance:
        lines.append("\nTop Feature Importance:")
        lines.append("-" * 40)
        sorted_features = sorted(feature_importance.items(), key=lambda x: abs(x[1]), reverse=True)
        for feature, importance in sorted_features[:10]:
            lines.append(f"  {feature:<25}: {importance:8.4f}")
    
    lines.append("=" * 70)
    
    return "\n".join(lines)


def format_evaluation_summary(
    model_name: str,
    baseline_metrics: Dict[str, float],
    model_metrics: Dict[str, float],
    test_queries: int
) -> str:
    """
    Format evaluation summary comparing baseline to model.
    
    Args:
        model_name: Name of the model
        baseline_metrics: Baseline performance metrics
        model_metrics: Model performance metrics
        test_queries: Number of test queries
        
    Returns:
        Formatted string summary
    """
    lines = []
    lines.append("=" * 70)
    lines.append(f"EVALUATION SUMMARY - {model_name}")
    lines.append(f"Test Queries: {test_queries}")
    lines.append("=" * 70)
    
    lines.append(f"\n{'Metric':<20} {'Baseline':>12} {'Model':>12} {'Improvement':>15}")
    lines.append("-" * 70)
    
    # Get all metrics
    all_metrics = set(baseline_metrics.keys()) | set(model_metrics.keys())
    
    improvements = {}
    for metric in sorted(all_metrics):
        baseline_val = baseline_metrics.get(metric, 0.0)
        model_val = model_metrics.get(metric, 0.0)
        
        # Calculate improvement
        if baseline_val > 0:
            improvement = ((model_val - baseline_val) / baseline_val) * 100
            improvement_str = f"{improvement:+.2f}%"
        else:
            improvement_str = "N/A"
        
        lines.append(f"{metric:<20} {baseline_val:12.4f} {model_val:12.4f} {improvement_str:>15}")
        
        if baseline_val > 0:
            improvements[metric] = (model_val - baseline_val) / baseline_val
    
    # Overall assessment
    lines.append("=" * 70)
    if improvements:
        avg_improvement = sum(improvements.values()) / len(improvements) * 100
        if avg_improvement > 0:
            lines.append(f"✓ Average Improvement: {avg_improvement:.2f}%")
        else:
            lines.append(f"⚠ Average Degradation: {avg_improvement:.2f}%")
    
    return "\n".join(lines)


def load_standardized_results(model_name: str, output_dir: str = "dynamic_hybrid") -> Dict[str, Any]:
    """
    Load standardized results for a model.
    
    Args:
        model_name: Name of the model
        output_dir: Directory containing outputs
        
    Returns:
        Dictionary containing metadata and evaluation results
    """
    results = {}
    
    # Load metadata
    metadata_path = os.path.join(output_dir, f"{model_name}_metadata.json")
    if os.path.exists(metadata_path):
        with open(metadata_path, 'r') as f:
            results["metadata"] = json.load(f)
    
    # Load evaluation results
    eval_path = os.path.join(output_dir, f"{model_name}_evaluation.json")
    if os.path.exists(eval_path):
        with open(eval_path, 'r') as f:
            results["evaluation"] = json.load(f)
    
    return results
