# Workflow Engine

FastAPI-based workflow/graph engine that executes async tool nodes with shared state, conditional edges, and loop support. Includes a sample Data Quality pipeline.

## Features
- Define graphs via API: nodes, edges, conditional edges, loop definitions, entry point.
- Shared `WorkflowState` flows between nodes; execution logs captured per run.
- Tool registry with default examples plus Data Quality tools (`profile_data`, `identify_anomalies`, `generate_rules`, `apply_rules`).
- Sync and async execution endpoints; WebSocket endpoints for live streaming and monitoring.
- In-memory storage for graphs/runs (swappable in future).

## Run the server
```bash
pip install -r requirements.txt
python -m app.main
# Server listens on http://0.0.0.0:8000
```

## Key APIs
- `POST /api/graph/create` – create a graph from a JSON definition (see sample below).
- `POST /api/graph/run` – run a graph synchronously and return final state + logs.
- `POST /api/graph/run-async` – start background execution; use state endpoint to monitor.
- `GET /api/graph/state/{run_id}` – fetch current state/logs for a run.
- `GET /api/graphs` / `GET /api/graphs/{graph_id}` – list/inspect graphs.
- `GET /api/runs` – list runs.
- `GET /api/tools` – list registered tools.
- WebSockets: `/ws/execute/{graph_id}` to execute with streaming logs, `/ws/monitor/{run_id}` to monitor an existing run.

## Data Quality workflow (Option C)
The data-quality tools are registered at startup. A ready-to-post graph definition lives in `app/workflows/data_quality.py` (`get_data_quality_graph_definition`). The included `test_pipeline.py` client script demonstrates the full flow:
```bash
python test_pipeline.py
```
(Ensure the server is running first.)

## How the engine works
- Graphs are built from a `GraphDefinition` (nodes, edges, loops, entry point).
- Execution walks edges from the entry node, evaluating conditional edges and loop exit conditions. Loop nodes track iterations and stop on condition or max iterations.
- Nodes call async tools; return values can mutate the shared state.
- Execution logs are appended on start/complete/loop events; stored with the run.

## Improvements with more time
- Add persistent storage (e.g., SQLite/Postgres) and periodic run/log flush for durability.
- Broaden tool lifecycle (versioning, hot reload) and validation.
- Harden conditional-node support and richer branching semantics.
- Add auth/rate limits and better error reporting.
