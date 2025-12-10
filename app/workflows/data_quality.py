"""
Data Quality Pipeline Implementation
Demonstrates the workflow engine with a realistic data quality assessment pipeline.
"""
from typing import Dict, Any, List
from app.models.state import WorkflowState
from app.tools.registry import get_tool_registry
import logging
import statistics
import random

logger = logging.getLogger(__name__)


# Data Quality Pipeline Tools

async def profile_data(state: WorkflowState, **config) -> Dict[str, Any]:
    """
    Profile the input data to understand its characteristics.
    Calculates basic statistics and identifies data types.
    """
    logger.info("Profiling data...")
    
    data = state.get("dataset", [])
    
    if not data:
        return {
            "profile": {"error": "No data provided"},
            "anomaly_count": 0
        }
    
    # Calculate statistics
    numeric_data = [x for x in data if isinstance(x, (int, float))]
    
    profile = {
        "total_records": len(data),
        "numeric_records": len(numeric_data),
        "mean": statistics.mean(numeric_data) if numeric_data else 0,
        "median": statistics.median(numeric_data) if numeric_data else 0,
        "std_dev": statistics.stdev(numeric_data) if len(numeric_data) > 1 else 0,
        "min": min(numeric_data) if numeric_data else None,
        "max": max(numeric_data) if numeric_data else None,
    }
    
    logger.info(f"Data profile: {profile}")
    
    return {
        "profile": profile,
        "iteration": state.get("iteration", 0)
    }


async def identify_anomalies(state: WorkflowState, **config) -> Dict[str, Any]:
    """
    Identify anomalies in the data based on statistical methods.
    Uses IQR (Interquartile Range) method for outlier detection.
    """
    logger.info("Identifying anomalies...")
    
    data = state.get("dataset", [])
    profile = state.get("profile", {})
    
    numeric_data = [x for x in data if isinstance(x, (int, float))]
    
    if len(numeric_data) < 4:
        return {
            "anomalies": [],
            "anomaly_count": 0,
            "anomaly_indices": []
        }
    
    # Calculate quartiles
    sorted_data = sorted(numeric_data)
    q1_idx = len(sorted_data) // 4
    q3_idx = 3 * len(sorted_data) // 4
    q1 = sorted_data[q1_idx]
    q3 = sorted_data[q3_idx]
    iqr = q3 - q1
    
    # Define outlier boundaries
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    # Find anomalies
    anomalies = []
    anomaly_indices = []
    
    for idx, value in enumerate(data):
        if isinstance(value, (int, float)):
            if value < lower_bound or value > upper_bound:
                anomalies.append({
                    "index": idx,
                    "value": value,
                    "reason": "statistical_outlier",
                    "bounds": {"lower": lower_bound, "upper": upper_bound}
                })
                anomaly_indices.append(idx)
    
    # Also check for null/missing values
    for idx, value in enumerate(data):
        if value is None or (isinstance(value, float) and value != value):  # NaN check
            anomalies.append({
                "index": idx,
                "value": value,
                "reason": "missing_value"
            })
            if idx not in anomaly_indices:
                anomaly_indices.append(idx)
    
    logger.info(f"Found {len(anomalies)} anomalies")
    
    return {
        "anomalies": anomalies,
        "anomaly_count": len(anomalies),
        "anomaly_indices": anomaly_indices,
        "detection_params": {
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound
        }
    }


async def generate_rules(state: WorkflowState, **config) -> Dict[str, Any]:
    """
    Generate data quality rules based on identified anomalies.
    Creates rules for fixing or flagging problematic data points.
    """
    logger.info("Generating quality rules...")
    
    anomalies = state.get("anomalies", [])
    profile = state.get("profile", {})
    detection_params = state.get("detection_params", {})
    
    rules = []
    
    # Rule 1: Handle missing values
    missing_value_count = sum(1 for a in anomalies if a.get("reason") == "missing_value")
    if missing_value_count > 0:
        rules.append({
            "rule_id": "rule_001",
            "type": "imputation",
            "action": "replace_with_median",
            "target": "missing_values",
            "params": {"replacement_value": profile.get("median", 0)},
            "affected_count": missing_value_count
        })
    
    # Rule 2: Handle statistical outliers
    outlier_count = sum(1 for a in anomalies if a.get("reason") == "statistical_outlier")
    if outlier_count > 0:
        rules.append({
            "rule_id": "rule_002",
            "type": "capping",
            "action": "cap_at_bounds",
            "target": "outliers",
            "params": {
                "lower_bound": detection_params.get("lower_bound"),
                "upper_bound": detection_params.get("upper_bound")
            },
            "affected_count": outlier_count
        })
    
    # Rule 3: Validation rule for future data
    rules.append({
        "rule_id": "rule_003",
        "type": "validation",
        "action": "enforce_range",
        "target": "all_numeric",
        "params": {
            "min": profile.get("min"),
            "max": profile.get("max")
        }
    })
    
    logger.info(f"Generated {len(rules)} quality rules")
    
    return {
        "rules": rules,
        "rules_count": len(rules)
    }


async def apply_rules(state: WorkflowState, **config) -> Dict[str, Any]:
    """
    Apply generated rules to clean the data.
    Modifies the dataset according to the quality rules.
    """
    logger.info("Applying quality rules...")
    
    data = state.get("dataset", []).copy()
    rules = state.get("rules", [])
    anomaly_indices = state.get("anomaly_indices", [])
    anomalies = state.get("anomalies", [])
    iteration = state.get("iteration", 0)
    
    modifications_made = 0
    
    for rule in rules:
        if rule["type"] == "imputation" and rule["action"] == "replace_with_median":
            # Replace missing values with median
            replacement = rule["params"]["replacement_value"]
            for anomaly in anomalies:
                if anomaly.get("reason") == "missing_value":
                    idx = anomaly["index"]
                    if 0 <= idx < len(data):
                        data[idx] = replacement
                        modifications_made += 1
        
        elif rule["type"] == "capping" and rule["action"] == "cap_at_bounds":
            # Cap outliers at bounds
            lower = rule["params"]["lower_bound"]
            upper = rule["params"]["upper_bound"]
            for anomaly in anomalies:
                if anomaly.get("reason") == "statistical_outlier":
                    idx = anomaly["index"]
                    if 0 <= idx < len(data):
                        value = data[idx]
                        if isinstance(value, (int, float)):
                            if value < lower:
                                data[idx] = lower
                                modifications_made += 1
                            elif value > upper:
                                data[idx] = upper
                                modifications_made += 1
    
    logger.info(f"Applied rules, made {modifications_made} modifications")
    
    # Calculate quality score
    original_count = state.get("profile", {}).get("total_records", len(data))
    quality_score = ((original_count - len(anomaly_indices)) / original_count * 100) if original_count > 0 else 0
    
    return {
        "dataset": data,
        "modifications_made": modifications_made,
        "quality_score": quality_score,
        "iteration": iteration + 1
    }


def register_data_quality_tools():
    """Register all data quality pipeline tools with the global registry."""
    registry = get_tool_registry()
    
    registry.register("profile_data", profile_data)
    registry.register("identify_anomalies", identify_anomalies)
    registry.register("generate_rules", generate_rules)
    registry.register("apply_rules", apply_rules)
    
    logger.info("Registered data quality pipeline tools")


def create_sample_dataset() -> List[float]:
    """
    Create a sample dataset with some anomalies for testing.
    Returns a list of numeric values with intentional outliers and missing values.
    """
    # Generate normal data
    dataset = [random.gauss(100, 15) for _ in range(100)]
    
    # Add some outliers
    dataset.extend([200, 250, -50, 300])
    
    # Add some missing values
    dataset.extend([None, None])
    
    # Shuffle
    random.shuffle(dataset)
    
    return dataset


def get_data_quality_graph_definition() -> Dict[str, Any]:
    """
    Get the graph definition for the data quality pipeline.
    This is the JSON structure that would be POSTed to /graph/create
    """
    return {
        "name": "data_quality_pipeline",
        "description": "Automated data quality assessment and improvement pipeline",
        "nodes": [
            {
                "name": "profile_data",
                "type": "standard",
                "tool": "profile_data",
                "config": {}
            },
            {
                "name": "identify_anomalies",
                "type": "standard",
                "tool": "identify_anomalies",
                "config": {}
            },
            {
                "name": "generate_rules",
                "type": "standard",
                "tool": "generate_rules",
                "config": {}
            },
            {
                "name": "apply_rules",
                "type": "loop",
                "tool": "apply_rules",
                "config": {}
            }
        ],
        "edges": [
            {
                "from_node": "profile_data",
                "to_node": "identify_anomalies"
            },
            {
                "from_node": "identify_anomalies",
                "to_node": "generate_rules"
            },
            {
                "from_node": "generate_rules",
                "to_node": "apply_rules"
            },
            {
                "from_node": "apply_rules",
                "to_node": "identify_anomalies"
            }
        ],
        "loops": [
            {
                "node": "apply_rules",
                "condition": {
                    "field": "anomaly_count",
                    "operator": "<",
                    "value": 5
                },
                "max_iterations": 5
            }
        ],
        "entry_point": "profile_data"
    }