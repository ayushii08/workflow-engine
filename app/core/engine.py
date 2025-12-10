"""
Workflow execution engine - the heart of the graph processing system.
"""
from typing import Optional, AsyncGenerator, Dict, Any
from app.core.graph import Graph
from app.core.node import LoopNode
from app.models.state import WorkflowState, WorkflowRun, ExecutionStatus
import logging
import asyncio

logger = logging.getLogger(__name__)


class ExecutionEngine:
    """
    Executes workflow graphs with support for:
    - Sequential execution
    - Conditional branching
    - Loops with exit conditions
    - Async execution
    - Real-time logging
    """
    
    def __init__(self):
        self.active_runs: Dict[str, WorkflowRun] = {}
    
    async def execute_graph(
        self,
        graph: Graph,
        run: WorkflowRun,
        stream_logs: bool = False
    ) -> WorkflowRun:
        """
        Execute a complete workflow graph.
        
        Args:
            graph: The graph to execute
            run: The workflow run object to track execution
            stream_logs: Whether to stream logs in real-time
            
        Returns:
            Completed workflow run
        """
        logger.info(f"Starting execution of graph {graph.graph_id}, run {run.run_id}")
        
        try:
            # Mark run as started
            run.mark_started()
            self.active_runs[run.run_id] = run
            
            # Start execution from entry point
            current_node_name = graph.entry_point
            
            while current_node_name:
                current_node = graph.get_node(current_node_name)
                
                if not current_node:
                    raise ValueError(f"Node {current_node_name} not found in graph")
                
                # Update current node in run
                run.current_node = current_node_name
                
                # Log execution start
                run.add_log(
                    node=current_node_name,
                    action="started",
                    details={"iteration": getattr(current_node, 'current_iteration', 0)}
                )
                
                # Execute the node
                run.state = await current_node.execute(run.state)
                
                # Log execution completion
                run.add_log(
                    node=current_node_name,
                    action="completed",
                    details={"state_snapshot": run.state.data.copy()}
                )
                
                # Determine next node
                next_node_name = await self._determine_next_node(
                    graph, current_node, run.state
                )
                
                # Handle loops
                if isinstance(current_node, LoopNode):
                    current_node.increment_iteration()
                    
                    if current_node.should_exit_loop(run.state):
                        run.add_log(
                            node=current_node_name,
                            action="loop_exited",
                            details={
                                "iterations": current_node.current_iteration,
                                "reason": "condition_met"
                            }
                        )
                        current_node.reset_iteration()
                        # End the workflow after loop exit
                        next_node_name = None
                    else:
                        # Continue loop - next_node_name stays as determined
                        run.add_log(
                            node=current_node_name,
                            action="loop_continued",
                            details={"iteration": current_node.current_iteration}
                        )
                
                current_node_name = next_node_name
                
                # Small delay to prevent tight loops from overwhelming
                await asyncio.sleep(0.01)
            
            # Mark run as completed
            run.mark_completed()
            logger.info(f"Completed execution of run {run.run_id}")
            
        except Exception as e:
            logger.error(f"Error during execution of run {run.run_id}: {str(e)}", exc_info=True)
            run.mark_failed(str(e))
            run.add_log(
                node=run.current_node or "unknown",
                action="error",
                details={"error": str(e)}
            )
        
        finally:
            # Remove from active runs
            if run.run_id in self.active_runs:
                del self.active_runs[run.run_id]
        
        return run
    
    async def _determine_next_node(
        self,
        graph: Graph,
        current_node,
        state: WorkflowState
    ) -> Optional[str]:
        """
        Determine the next node to execute based on edges and conditions.
        
        Args:
            graph: The workflow graph
            current_node: Current node being executed
            state: Current workflow state
            
        Returns:
            Name of next node or None if workflow should end
        """
        current_node_name = current_node.name
        
        # Get potential next nodes
        next_nodes = graph.get_next_nodes(current_node_name)
        
        if not next_nodes:
            logger.info(f"No next nodes from {current_node_name}, ending workflow")
            return None
        
        # If only one next node and no conditions, return it
        if len(next_nodes) == 1 and current_node_name not in graph.conditional_edges:
            return next_nodes[0]
        
        # Handle conditional edges
        if current_node_name in graph.conditional_edges:
            conditions = graph.conditional_edges[current_node_name]
            
            for next_node, edge_def in conditions.items():
                if edge_def.condition and self._evaluate_condition(state, edge_def.condition):
                    logger.info(f"Condition met for edge {current_node_name} -> {next_node}")
                    return next_node
        
        # Default to first next node if no conditions matched
        return next_nodes[0]
    
    def _evaluate_condition(self, state: WorkflowState, condition) -> bool:
        """Evaluate a condition against the current state."""
        value = state.get(condition.field)
        
        try:
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
        except Exception as e:
            logger.error(f"Error evaluating condition: {str(e)}")
            return False
    
    async def execute_graph_async(
        self,
        graph: Graph,
        run: WorkflowRun
    ) -> str:
        """
        Execute graph asynchronously as a background task.
        
        Args:
            graph: The graph to execute
            run: The workflow run object
            
        Returns:
            Run ID for tracking
        """
        asyncio.create_task(self.execute_graph(graph, run))
        return run.run_id
    
    def get_run_status(self, run_id: str) -> Optional[WorkflowRun]:
        """Get the status of an active run."""
        return self.active_runs.get(run_id)
    
    async def stream_execution(
        self,
        graph: Graph,
        run: WorkflowRun
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Execute graph and stream log entries as they happen.
        
        Args:
            graph: The graph to execute
            run: The workflow run object
            
        Yields:
            Log entries as they are generated
        """
        logger.info(f"Starting streaming execution for run {run.run_id}")
        
        # Start execution in background
        execution_task = asyncio.create_task(self.execute_graph(graph, run))
        
        last_log_count = 0
        
        # Stream logs as they are generated
        while not execution_task.done():
            current_log_count = len(run.execution_log)
            
            # Yield new log entries
            if current_log_count > last_log_count:
                for i in range(last_log_count, current_log_count):
                    log_entry = run.execution_log[i]
                    yield {
                        "type": "log",
                        "data": log_entry.dict()
                    }
                last_log_count = current_log_count
            
            await asyncio.sleep(0.1)
        
        # Wait for execution to complete
        await execution_task
        
        # Yield any remaining logs
        current_log_count = len(run.execution_log)
        if current_log_count > last_log_count:
            for i in range(last_log_count, current_log_count):
                log_entry = run.execution_log[i]
                yield {
                    "type": "log",
                    "data": log_entry.dict()
                }
        
        # Yield final status
        yield {
            "type": "complete",
            "data": {
                "status": run.status,
                "final_state": run.state.to_dict()
            }
        }


# Global engine instance
_engine_instance: Optional[ExecutionEngine] = None


def get_engine() -> ExecutionEngine:
    """Get or create the global execution engine instance."""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = ExecutionEngine()
    return _engine_instance