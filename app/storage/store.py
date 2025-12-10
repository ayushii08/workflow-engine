"""
Storage layer for persisting graphs and workflow runs.
Currently uses in-memory storage, easily extensible to database.
"""
from typing import Dict, Optional, List
from app.core.graph import Graph
from app.models.state import WorkflowRun
import logging

logger = logging.getLogger(__name__)


class InMemoryStorage:
    """
    In-memory storage for graphs and runs.
    For production, this should be replaced with persistent storage (PostgreSQL, etc.)
    """
    
    def __init__(self):
        self.graphs: Dict[str, Graph] = {}
        self.runs: Dict[str, WorkflowRun] = {}
        self.graph_runs: Dict[str, List[str]] = {}  # graph_id -> [run_ids]
    
    # Graph operations
    
    def save_graph(self, graph: Graph) -> str:
        """
        Save a graph to storage.
        
        Args:
            graph: Graph to save
            
        Returns:
            Graph ID
        """
        self.graphs[graph.graph_id] = graph
        self.graph_runs[graph.graph_id] = []
        logger.info(f"Saved graph {graph.graph_id}")
        return graph.graph_id
    
    def get_graph(self, graph_id: str) -> Optional[Graph]:
        """
        Retrieve a graph by ID.
        
        Args:
            graph_id: Graph ID
            
        Returns:
            Graph or None if not found
        """
        return self.graphs.get(graph_id)
    
    def list_graphs(self) -> List[Graph]:
        """List all graphs."""
        return list(self.graphs.values())
    
    def delete_graph(self, graph_id: str) -> bool:
        """
        Delete a graph.
        
        Args:
            graph_id: Graph ID
            
        Returns:
            True if deleted, False if not found
        """
        if graph_id in self.graphs:
            del self.graphs[graph_id]
            if graph_id in self.graph_runs:
                del self.graph_runs[graph_id]
            logger.info(f"Deleted graph {graph_id}")
            return True
        return False
    
    # Run operations
    
    def save_run(self, run: WorkflowRun) -> str:
        """
        Save a workflow run.
        
        Args:
            run: Workflow run to save
            
        Returns:
            Run ID
        """
        self.runs[run.run_id] = run
        
        # Add to graph's run list
        if run.graph_id in self.graph_runs:
            self.graph_runs[run.graph_id].append(run.run_id)
        
        logger.debug(f"Saved run {run.run_id}")
        return run.run_id
    
    def get_run(self, run_id: str) -> Optional[WorkflowRun]:
        """
        Retrieve a run by ID.
        
        Args:
            run_id: Run ID
            
        Returns:
            WorkflowRun or None if not found
        """
        return self.runs.get(run_id)
    
    def list_runs(self, graph_id: Optional[str] = None) -> List[WorkflowRun]:
        """
        List runs, optionally filtered by graph ID.
        
        Args:
            graph_id: Optional graph ID to filter by
            
        Returns:
            List of workflow runs
        """
        if graph_id:
            run_ids = self.graph_runs.get(graph_id, [])
            return [self.runs[rid] for rid in run_ids if rid in self.runs]
        return list(self.runs.values())
    
    def update_run(self, run: WorkflowRun) -> bool:
        """
        Update an existing run.
        
        Args:
            run: Updated workflow run
            
        Returns:
            True if updated, False if not found
        """
        if run.run_id in self.runs:
            self.runs[run.run_id] = run
            logger.debug(f"Updated run {run.run_id}")
            return True
        return False
    
    def delete_run(self, run_id: str) -> bool:
        """
        Delete a run.
        
        Args:
            run_id: Run ID
            
        Returns:
            True if deleted, False if not found
        """
        if run_id in self.runs:
            run = self.runs[run_id]
            del self.runs[run_id]
            
            # Remove from graph's run list
            if run.graph_id in self.graph_runs:
                self.graph_runs[run.graph_id] = [
                    rid for rid in self.graph_runs[run.graph_id] if rid != run_id
                ]
            
            logger.info(f"Deleted run {run_id}")
            return True
        return False
    
    # Utility methods
    
    def clear_all(self) -> None:
        """Clear all storage (useful for testing)."""
        self.graphs.clear()
        self.runs.clear()
        self.graph_runs.clear()
        logger.warning("Cleared all storage")
    
    def get_stats(self) -> Dict:
        """Get storage statistics."""
        return {
            "total_graphs": len(self.graphs),
            "total_runs": len(self.runs),
            "graphs": [
                {
                    "graph_id": gid,
                    "name": graph.name,
                    "runs": len(self.graph_runs.get(gid, []))
                }
                for gid, graph in self.graphs.items()
            ]
        }


# Global storage instance
_storage_instance: Optional[InMemoryStorage] = None


def get_storage() -> InMemoryStorage:
    """Get or create the global storage instance."""
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = InMemoryStorage()
        logger.info("Initialized in-memory storage")
    return _storage_instance


# For future database implementation:
# 
# class DatabaseStorage:
#     """PostgreSQL/SQLite storage implementation."""
#     
#     def __init__(self, database_url: str):
#         self.engine = create_engine(database_url)
#         # Initialize tables, migrations, etc.
#     
#     async def save_graph(self, graph: Graph) -> str:
#         # Save to database
#         pass
#     
#     # ... implement all methods with database queries