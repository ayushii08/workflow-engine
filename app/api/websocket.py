"""
WebSocket handler for streaming real-time execution logs.
"""
import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.core.engine import get_engine
from app.core.graph import Graph
from app.models.state import WorkflowState, WorkflowRun
from app.storage.store import get_storage
import json
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/execute/{graph_id}")
async def websocket_execute_graph(websocket: WebSocket, graph_id: str):
    """
    Execute a graph and stream logs via WebSocket.
    
    The client should send initial_state as JSON after connecting.
    Logs will be streamed as they are generated during execution.
    
    Message format:
    - Client sends: {"initial_state": {...}}
    - Server sends: {"type": "log"|"complete"|"error", "data": {...}}
    """
    await websocket.accept()
    logger.info(f"WebSocket connection established for graph {graph_id}")
    
    try:
        # Get graph from storage
        storage = get_storage()
        graph = storage.get_graph(graph_id)
        
        if not graph:
            await websocket.send_json({
                "type": "error",
                "data": {"message": f"Graph {graph_id} not found"}
            })
            await websocket.close()
            return
        
        # Wait for initial state from client
        data = await websocket.receive_json()
        initial_state_data = data.get("initial_state", {})
        
        logger.info(f"Received initial state for execution: {list(initial_state_data.keys())}")
        
        # Create workflow run
        initial_state = WorkflowState(data=initial_state_data)
        run = WorkflowRun(graph_id=graph_id, state=initial_state)
        
        # Save run
        storage.save_run(run)
        
        # Send run_id to client
        await websocket.send_json({
            "type": "started",
            "data": {"run_id": run.run_id}
        })
        
        # Execute graph and stream logs
        engine = get_engine()
        
        async for message in engine.stream_execution(graph, run):
            await websocket.send_json(message)
        
        # Update run in storage
        storage.update_run(run)
        
        logger.info(f"WebSocket execution completed for run {run.run_id}")
    
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON received: {str(e)}")
        try:
            await websocket.send_json({
                "type": "error",
                "data": {"message": "Invalid JSON format"}
            })
        except:
            pass
    
    except Exception as e:
        logger.error(f"Error during WebSocket execution: {str(e)}", exc_info=True)
        try:
            await websocket.send_json({
                "type": "error",
                "data": {"message": str(e)}
            })
        except:
            pass
    
    finally:
        try:
            await websocket.close()
        except:
            pass


@router.websocket("/monitor/{run_id}")
async def websocket_monitor_run(websocket: WebSocket, run_id: str):
    """
    Monitor an existing run via WebSocket.
    
    Streams the execution log as the run progresses.
    Useful for monitoring async executions.
    """
    await websocket.accept()
    logger.info(f"WebSocket monitoring established for run {run_id}")
    
    try:
        storage = get_storage()
        run = storage.get_run(run_id)
        
        if not run:
            await websocket.send_json({
                "type": "error",
                "data": {"message": f"Run {run_id} not found"}
            })
            await websocket.close()
            return
        
        # Stream existing logs first
        for log in run.execution_log:
            await websocket.send_json({
                "type": "log",
                "data": log.dict()
            })
        
        # Monitor for new logs
        last_log_count = len(run.execution_log)
        
        while run.status in ["pending", "running"]:
            # Refresh run from storage
            run = storage.get_run(run_id)
            
            if not run:
                break
            
            # Send new logs
            current_log_count = len(run.execution_log)
            if current_log_count > last_log_count:
                for i in range(last_log_count, current_log_count):
                    await websocket.send_json({
                        "type": "log",
                        "data": run.execution_log[i].dict()
                    })
                last_log_count = current_log_count
            
            # Small delay
            await asyncio.sleep(0.5)
        
        # Send final status
        await websocket.send_json({
            "type": "complete",
            "data": {
                "status": run.status,
                "final_state": run.state.to_dict() if run else None
            }
        })
        
        logger.info(f"WebSocket monitoring completed for run {run_id}")
    
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    
    except Exception as e:
        logger.error(f"Error during WebSocket monitoring: {str(e)}", exc_info=True)
        try:
            await websocket.send_json({
                "type": "error",
                "data": {"message": str(e)}
            })
        except:
            pass
    
    finally:
        try:
            await websocket.close()
        except:
            pass