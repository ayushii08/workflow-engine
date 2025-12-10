"""
Node implementations for the workflow engine.
"""
from typing import Callable, Dict, Any, Optional
from app.models.state import WorkflowState
from app.models.graph import NodeType, EdgeCondition
import logging

logger = logging.getLogger(__name__)


class Node:
    """
    Represents a single node in the workflow graph.
    Each node executes a tool/function and can modify the shared state.
    """
    
    def __init__(
        self,
        name: str,
        tool: Callable,
        node_type: NodeType = NodeType.STANDARD,
        config: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.tool = tool
        self.node_type = node_type
        self.config = config or {}
    
    async def execute(self, state: WorkflowState) -> WorkflowState:
        """
        Execute the node's tool with the current state.
        
        Args:
            state: Current workflow state
            
        Returns:
            Updated workflow state
        """
        logger.info(f"Executing node: {self.name}")
        
        try:
            # Pass state and config to the tool
            result = await self.tool(state, **self.config)
            
            # If tool returns a dict, update state with it
            if isinstance(result, dict):
                state.update(result)
            elif isinstance(result, WorkflowState):
                state = result
            
            logger.info(f"Node {self.name} completed successfully")
            return state
            
        except Exception as e:
            logger.error(f"Error executing node {self.name}: {str(e)}")
            raise


class ConditionalNode(Node):
    """
    A node that performs conditional routing.
    Determines next node based on state conditions.
    """
    
    def __init__(
        self,
        name: str,
        tool: Callable,
        conditions: Dict[str, EdgeCondition],
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, tool, NodeType.CONDITIONAL, config)
        self.conditions = conditions
    
    def evaluate_next_node(self, state: WorkflowState) -> Optional[str]:
        """
        Evaluate conditions and return the next node to execute.
        
        Args:
            state: Current workflow state
            
        Returns:
            Name of next node or None
        """
        for next_node, condition in self.conditions.items():
            if self._evaluate_condition(state, condition):
                return next_node
        return None
    
    def _evaluate_condition(self, state: WorkflowState, condition: EdgeCondition) -> bool:
        """Evaluate a single condition against the state."""
        value = state.get(condition.field)
        
        if condition.operator == "==":
            return value == condition.value
        elif condition.operator == "!=":
            return value != condition.value
        elif condition.operator == ">":
            return value > condition.value
        elif condition.operator == "<":
            return value < condition.value
        elif condition.operator == ">=":
            return value >= condition.value
        elif condition.operator == "<=":
            return value <= condition.value
        elif condition.operator == "in":
            return value in condition.value
        elif condition.operator == "not_in":
            return value not in condition.value
        else:
            logger.warning(f"Unknown operator: {condition.operator}")
            return False


class LoopNode(Node):
    """
    A node that can loop back based on a condition.
    Useful for iterative refinement workflows.
    """
    
    def __init__(
        self,
        name: str,
        tool: Callable,
        exit_condition: EdgeCondition,
        max_iterations: int = 10,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(name, tool, NodeType.LOOP, config)
        self.exit_condition = exit_condition
        self.max_iterations = max_iterations
        self.current_iteration = 0
    
    def should_exit_loop(self, state: WorkflowState) -> bool:
        """
        Check if loop should exit based on condition or max iterations.
        
        Args:
            state: Current workflow state
            
        Returns:
            True if loop should exit, False otherwise
        """
        # Check max iterations
        if self.current_iteration >= self.max_iterations:
            logger.info(f"Loop {self.name} reached max iterations: {self.max_iterations}")
            return True
        
        # Check exit condition
        value = state.get(self.exit_condition.field)
        should_exit = self._evaluate_exit_condition(value)
        
        if should_exit:
            logger.info(f"Loop {self.name} exit condition met")
        
        return should_exit
    
    def _evaluate_exit_condition(self, value: Any) -> bool:
        """Evaluate the loop exit condition."""
        condition = self.exit_condition
        
        if condition.operator == "==":
            return value == condition.value
        elif condition.operator == "!=":
            return value != condition.value
        elif condition.operator == ">":
            return value > condition.value
        elif condition.operator == "<":
            return value < condition.value
        elif condition.operator == ">=":
            return value >= condition.value
        elif condition.operator == "<=":
            return value <= condition.value
        elif condition.operator == "in":
            return value in condition.value
        elif condition.operator == "not_in":
            return value not in condition.value
        else:
            return False
    
    def increment_iteration(self) -> None:
        """Increment iteration counter."""
        self.current_iteration += 1
    
    def reset_iteration(self) -> None:
        """Reset iteration counter."""
        self.current_iteration = 0