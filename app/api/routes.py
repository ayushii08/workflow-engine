"""
FastAPI route handlers for the workflow engine API.
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List

from app.models.graph import (
    CreateGraphRequest, CreateGraphResponse,
    RunGraphRequest, RunGraphResponse,
    GetStateResponse, GraphDefinition
)
from app.models.state import WorkflowState, WorkflowRun
from app.core.graph import GraphBuilder
from app.core.engine import get_engine
from app.storage.store import get_storage
from app.tools.registry import get_tool_registry
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/graph/create", response_model=CreateGraphResponse)
async def create_graph(request: CreateGraphRequest):
    """
    Create a new workflow graph.
    
    The graph definition includes nodes, edges, and optional loop definitions.
    Nodes are connected via edges to form the execution flow.
    """
    try:
        logger.info(f"Creating graph: {request.graph.name}")
        
        # Get tool registry
        tool_registry = get_tool_registry()
        
        # Build graph from definition
        graph = GraphBuilder.build_from_definition(
            request.graph,
            tool_registry.get_all()
        )
        
        # Save to storage
        storage = get_storage()
        graph_id = storage.save_graph(graph)
        
        logger.info(f"Graph created successfully: {graph_id}")
        
        return CreateGraphResponse(
            graph_id=graph_id,
            message=f"Graph '{request.graph.name}' created successfully"
        )
    
    except ValueError as e:
        logger.error(f"Validation error creating graph: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except Exception as e:
        logger.error(f"Error creating graph: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/graph/run", response_model=RunGraphResponse)
async def run_graph(request: RunGraphRequest, background_tasks: BackgroundTasks):
    """
    Execute a workflow graph with the provided initial state.
    
    This endpoint runs the graph synchronously and returns the final state
    and execution log. For long-running workflows, consider using the
    async execution endpoint or WebSocket streaming.
    """
    try:
        logger.info(f"Running graph: {request.graph_id}")
        
        # Get graph from storage
        storage = get_storage()
        graph = storage.get_graph(request.graph_id)
        
        if not graph:
            raise HTTPException(
                status_code=404,
                detail=f"Graph {request.graph_id} not found"
            )
        
        # Create workflow run
        initial_state = WorkflowState(data=request.initial_state)
        run = WorkflowRun(graph_id=request.graph_id, state=initial_state)
        
        # Save run to storage
        storage.save_run(run)
        
        # Execute graph
        engine = get_engine()
        completed_run = await engine.execute_graph(graph, run)
        
        # Update run in storage
        storage.update_run(completed_run)
        
        logger.info(f"Graph execution completed: {completed_run.run_id}")
        
        return RunGraphResponse(
            run_id=completed_run.run_id,
            final_state=completed_run.state.data,
            execution_log=[log.dict() for log in completed_run.execution_log],
            status=completed_run.status,
            message="Graph executed successfully"
        )
    
    except HTTPException:
        raise
    
    except Exception as e:
        logger.error(f"Error running graph: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Execution error: {str(e)}")


@router.post("/graph/run-async")
async def run_graph_async(request: RunGraphRequest, background_tasks: BackgroundTasks):
    """
    Execute a workflow graph asynchronously in the background.
    
    Returns immediately with a run_id that can be used to check status.
    Use GET /graph/state/{run_id} to monitor progress.
    """
    try:
        logger.info(f"Starting async execution of graph: {request.graph_id}")
        
        # Get graph from storage
        storage = get_storage()
        graph = storage.get_graph(request.graph_id)
        
        if not graph:
            raise HTTPException(
                status_code=404,
                detail=f"Graph {request.graph_id} not found"
            )
        
        # Create workflow run
        initial_state = WorkflowState(data=request.initial_state)
        run = WorkflowRun(graph_id=request.graph_id, state=initial_state)
        
        # Save run to storage
        storage.save_run(run)
        
        # Execute graph in background
        engine = get_engine()
        
        async def execute_and_save():
            completed_run = await engine.execute_graph(graph, run)
            storage.update_run(completed_run)
        
        background_tasks.add_task(execute_and_save)
        
        return {
            "run_id": run.run_id,
            "status": "started",
            "message": "Graph execution started in background"
        }
    
    except HTTPException:
        raise
    
    except Exception as e:
        logger.error(f"Error starting async execution: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/graph/state/{run_id}", response_model=GetStateResponse)
async def get_run_state(run_id: str):
    """
    Get the current state and status of a workflow run.
    
    This is useful for monitoring long-running workflows or checking
    the results of async executions.
    """
    try:
        logger.debug(f"Getting state for run: {run_id}")
        
        storage = get_storage()
        run = storage.get_run(run_id)
        
        if not run:
            raise HTTPException(
                status_code=404,
                detail=f"Run {run_id} not found"
            )
        
        return GetStateResponse(
            run_id=run.run_id,
            graph_id=run.graph_id,
            current_state=run.state.data,
            status=run.status,
            execution_log=[log.dict() for log in run.execution_log]
        )
    
    except HTTPException:
        raise
    
    except Exception as e:
        logger.error(f"Error getting run state: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/graphs")
async def list_graphs():
    """List all available graphs."""
    try:
        storage = get_storage()
        graphs = storage.list_graphs()
        
        return {
            "graphs": [
                {
                    "graph_id": g.graph_id,
                    "name": g.name,
                    "description": g.description,
                    "nodes": len(g.nodes),
                    "entry_point": g.entry_point
                }
                for g in graphs
            ]
        }
    
    except Exception as e:
        logger.error(f"Error listing graphs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/graphs/{graph_id}")
async def get_graph(graph_id: str):
    """Get detailed information about a specific graph."""
    try:
        storage = get_storage()
        graph = storage.get_graph(graph_id)
        
        if not graph:
            raise HTTPException(
                status_code=404,
                detail=f"Graph {graph_id} not found"
            )
        
        return graph.to_dict()
    
    except HTTPException:
        raise
    
    except Exception as e:
        logger.error(f"Error getting graph: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.delete("/graphs/{graph_id}")
async def delete_graph(graph_id: str):
    """Delete a graph."""
    try:
        storage = get_storage()
        success = storage.delete_graph(graph_id)
        
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Graph {graph_id} not found"
            )
        
        return {"message": f"Graph {graph_id} deleted successfully"}
    
    except HTTPException:
        raise
    
    except Exception as e:
        logger.error(f"Error deleting graph: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/runs")
async def list_runs(graph_id: str = None):
    """List all runs, optionally filtered by graph_id."""
    try:
        storage = get_storage()
        runs = storage.list_runs(graph_id)
        
        return {
            "runs": [
                {
                    "run_id": r.run_id,
                    "graph_id": r.graph_id,
                    "status": r.status,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                    "completed_at": r.completed_at.isoformat() if r.completed_at else None
                }
                for r in runs
            ]
        }
    
    except Exception as e:
        logger.error(f"Error listing runs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/tools")
async def list_tools():
    """List all registered tools."""
    try:
        registry = get_tool_registry()
        tools = registry.list_tools()
        
        return {
            "tools": tools,
            "count": len(tools)
        }
    
    except Exception as e:
        logger.error(f"Error listing tools: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/stats")
async def get_stats():
    """Get system statistics."""
    try:
        storage = get_storage()
        return storage.get_stats()
    
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")