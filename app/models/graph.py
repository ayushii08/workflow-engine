"""
Pydantic models for graph definitions and API requests/responses.
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Callable
from enum import Enum


class NodeType(str, Enum):
    """Types of nodes in the workflow."""
    STANDARD = "standard"
    CONDITIONAL = "conditional"
    LOOP = "loop"


class EdgeCondition(BaseModel):
    """Condition for conditional branching."""
    field: str = Field(..., description="Field in state to check")
    operator: str = Field(..., description="Comparison operator: ==, !=, >, <, >=, <=, in, not_in")
    value: Any = Field(..., description="Value to compare against")


class NodeDefinition(BaseModel):
    """Definition of a workflow node."""
    name: str = Field(..., description="Unique node identifier")
    type: NodeType = Field(default=NodeType.STANDARD, description="Type of node")
    tool: str = Field(..., description="Name of the tool/function to execute")
    config: Dict[str, Any] = Field(default_factory=dict, description="Node-specific configuration")
    
    class Config:
        use_enum_values = True


class EdgeDefinition(BaseModel):
    """Definition of an edge between nodes."""
    from_node: str = Field(..., description="Source node name")
    to_node: str = Field(..., description="Target node name")
    condition: Optional[EdgeCondition] = Field(None, description="Optional condition for this edge")


class LoopDefinition(BaseModel):
    """Definition of a loop in the workflow."""
    node: str = Field(..., description="Node to loop on")
    condition: EdgeCondition = Field(..., description="Condition to exit loop")
    max_iterations: int = Field(default=10, description="Maximum iterations to prevent infinite loops")


class GraphDefinition(BaseModel):
    """Complete graph definition."""
    name: str = Field(..., description="Graph name")
    description: Optional[str] = Field(None, description="Graph description")
    nodes: List[NodeDefinition] = Field(..., description="List of nodes")
    edges: List[EdgeDefinition] = Field(..., description="List of edges")
    loops: Optional[List[LoopDefinition]] = Field(default_factory=list, description="Loop definitions")
    entry_point: str = Field(..., description="Name of the starting node")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "data_quality_pipeline",
                "description": "Data quality assessment and improvement pipeline",
                "nodes": [
                    {"name": "profile_data", "type": "standard", "tool": "profile_data"},
                    {"name": "identify_anomalies", "type": "standard", "tool": "identify_anomalies"},
                    {"name": "generate_rules", "type": "standard", "tool": "generate_rules"},
                    {"name": "apply_rules", "type": "standard", "tool": "apply_rules"}
                ],
                "edges": [
                    {"from_node": "profile_data", "to_node": "identify_anomalies"},
                    {"from_node": "identify_anomalies", "to_node": "generate_rules"},
                    {"from_node": "generate_rules", "to_node": "apply_rules"},
                    {"from_node": "apply_rules", "to_node": "identify_anomalies"}
                ],
                "loops": [
                    {
                        "node": "apply_rules",
                        "condition": {"field": "anomaly_count", "operator": "<", "value": 5},
                        "max_iterations": 5
                    }
                ],
                "entry_point": "profile_data"
            }
        }


class CreateGraphRequest(BaseModel):
    """Request model for creating a graph."""
    graph: GraphDefinition


class CreateGraphResponse(BaseModel):
    """Response model for graph creation."""
    graph_id: str
    message: str


class RunGraphRequest(BaseModel):
    """Request model for running a graph."""
    graph_id: str
    initial_state: Dict[str, Any]


class RunGraphResponse(BaseModel):
    """Response model for graph execution."""
    run_id: str
    final_state: Dict[str, Any]
    execution_log: List[Dict[str, Any]]
    status: str
    message: str


class GetStateResponse(BaseModel):
    """Response model for getting state."""
    run_id: str
    graph_id: str
    current_state: Dict[str, Any]
    status: str
    execution_log: List[Dict[str, Any]]