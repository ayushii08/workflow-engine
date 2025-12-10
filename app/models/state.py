"""
State management models for workflow execution.
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum
import uuid


class ExecutionStatus(str, Enum):
    """Status of workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LogEntry(BaseModel):
    """Single log entry for execution tracking."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node: str
    action: str
    details: Optional[Dict[str, Any]] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "timestamp": "2025-01-15T10:30:00",
                "node": "profile_data",
                "action": "executed",
                "details": {"rows_processed": 1000}
            }
        }


class WorkflowState(BaseModel):
    """
    The shared state that flows through the workflow.
    This is a flexible container that can hold any workflow-specific data.
    """
    data: Dict[str, Any] = Field(default_factory=dict, description="Actual workflow data")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata about execution")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from state data."""
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a value in state data."""
        self.data[key] = value
    
    def update(self, updates: Dict[str, Any]) -> None:
        """Update multiple values in state data."""
        self.data.update(updates)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary."""
        return {"data": self.data, "metadata": self.metadata}


class WorkflowRun(BaseModel):
    """
    Complete execution run of a workflow.
    Tracks the entire lifecycle of a graph execution.
    """
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    graph_id: str
    status: ExecutionStatus = Field(default=ExecutionStatus.PENDING)
    state: WorkflowState = Field(default_factory=WorkflowState)
    execution_log: List[LogEntry] = Field(default_factory=list)
    current_node: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    
    class Config:
        use_enum_values = True
    
    def add_log(self, node: str, action: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Add a log entry to execution log."""
        entry = LogEntry(node=node, action=action, details=details)
        self.execution_log.append(entry)
    
    def mark_started(self) -> None:
        """Mark run as started."""
        self.status = ExecutionStatus.RUNNING
        self.started_at = datetime.utcnow()
    
    def mark_completed(self) -> None:
        """Mark run as completed."""
        self.status = ExecutionStatus.COMPLETED
        self.completed_at = datetime.utcnow()
    
    def mark_failed(self, error: str) -> None:
        """Mark run as failed with error message."""
        self.status = ExecutionStatus.FAILED
        self.completed_at = datetime.utcnow()
        self.error = error