"""
Complete test script for the Data Quality Pipeline workflow.
Run this after starting the server to verify everything works.
"""
import requests
import json
import time
from typing import Dict, Any

BASE_URL = "http://localhost:8000/api"


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def create_graph() -> str:
    """Create the data quality pipeline graph."""
    print_section("Creating Data Quality Pipeline Graph")
    
    graph_definition = {
        "graph": {
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
                {"from_node": "profile_data", "to_node": "identify_anomalies"},
                {"from_node": "identify_anomalies", "to_node": "generate_rules"},
                {"from_node": "generate_rules", "to_node": "apply_rules"},
                {"from_node": "apply_rules", "to_node": "identify_anomalies"}
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
    }
    
    response = requests.post(f"{BASE_URL}/graph/create", json=graph_definition)
    response.raise_for_status()
    
    result = response.json()
    graph_id = result["graph_id"]
    
    print("Graph created successfully!")
    print(f"Graph ID: {graph_id}")
    print(f"Message: {result['message']}")
    
    return graph_id


def list_graphs():
    """List all available graphs."""
    print_section("Listing All Graphs")
    
    response = requests.get(f"{BASE_URL}/graphs")
    response.raise_for_status()
    
    result = response.json()
    graphs = result["graphs"]
    
    print(f"Found {len(graphs)} graph(s):\n")
    for i, graph in enumerate(graphs, 1):
        print(f"{i}. {graph['name']}")
        print(f"   ID: {graph['graph_id']}")
        print(f"   Nodes: {graph['nodes']}")
        print(f"   Entry Point: {graph['entry_point']}")
        if graph.get('description'):
            print(f"   Description: {graph['description']}")
        print()


def run_pipeline(graph_id: str, dataset: list) -> Dict[str, Any]:
    """Run the data quality pipeline with a dataset."""
    print_section("Running Data Quality Pipeline")
    
    print("Input Dataset:")
    print(f"   Data: {dataset}")
    print(f"   Length: {len(dataset)}")
    print(f"   Contains None: {None in dataset}\n")
    
    run_request = {
        "graph_id": graph_id,
        "initial_state": {
            "dataset": dataset
        }
    }
    
    print("⚙️  Executing workflow...")
    start_time = time.time()
    
    response = requests.post(f"{BASE_URL}/graph/run", json=run_request)
    response.raise_for_status()
    
    elapsed_time = time.time() - start_time
    result = response.json()
    
    print(f"Execution completed in {elapsed_time:.2f}s\n")
    
    return result


def print_execution_results(result: Dict[str, Any]):
    """Print detailed execution results."""
    print_section("Execution Results")
    
    print(f"Run ID: {result['run_id']}")
    print(f"Status: {result['status']}\n")
    
    final_state = result['final_state']
    
    print("Final State:")
    print(f"   Anomaly Count: {final_state.get('anomaly_count', 'N/A')}")
    print(f"   Quality Score: {final_state.get('quality_score', 'N/A'):.2f}%")
    print(f"   Iterations: {final_state.get('iteration', 'N/A')}")
    print(f"   Modifications Made: {final_state.get('modifications_made', 'N/A')}\n")
    
    if 'profile' in final_state:
        profile = final_state['profile']
        print("Data Profile:")
        print(f"   Total Records: {profile.get('total_records', 'N/A')}")
        print(f"   Numeric Records: {profile.get('numeric_records', 'N/A')}")
        print(f"   Mean: {profile.get('mean', 'N/A'):.2f}")
        print(f"   Median: {profile.get('median', 'N/A'):.2f}")
        print(f"   Std Dev: {profile.get('std_dev', 'N/A'):.2f}")
        print(f"   Min: {profile.get('min', 'N/A')}")
        print(f"   Max: {profile.get('max', 'N/A')}\n")
    
    if 'anomalies' in final_state and final_state['anomalies']:
        print(f"Remaining Anomalies: {len(final_state['anomalies'])}")
        for i, anomaly in enumerate(final_state['anomalies'][:3], 1):
            print(f"   {i}. Index {anomaly['index']}: {anomaly['value']} ({anomaly['reason']})")
        if len(final_state['anomalies']) > 3:
            print(f"   ... and {len(final_state['anomalies']) - 3} more\n")
    
    print("Execution Log:")
    execution_log = result['execution_log']
    print(f"   Total Log Entries: {len(execution_log)}\n")
    
    for i, log in enumerate(execution_log, 1):
        action_emoji = {
            'started': '',
            'completed': '',
            'loop_continued': '',
            'loop_exited': '',
            'error': ''
        }.get(log['action'], '')
        
        prefix = f"{action_emoji} " if action_emoji else ""
        print(f"   {i}. {prefix}[{log['node']}] {log['action']}")
        
        if log.get('details'):
            details = log['details']
            if 'iteration' in details:
                print(f"      Iteration: {details['iteration']}")
            if 'reason' in details:
                print(f"      Reason: {details['reason']}")
    
    print()


def get_run_state(run_id: str):
    """Get the state of a specific run."""
    print_section("Fetching Run State")
    
    response = requests.get(f"{BASE_URL}/graph/state/{run_id}")
    response.raise_for_status()
    
    result = response.json()
    
    print(f"Run ID: {result['run_id']}")
    print(f"Graph ID: {result['graph_id']}")
    print(f"Status: {result['status']}")
    print(f"Log Entries: {len(result['execution_log'])}\n")


def list_tools():
    """List all registered tools."""
    print_section("Available Tools")
    
    response = requests.get(f"{BASE_URL}/tools")
    response.raise_for_status()
    
    result = response.json()
    tools = result['tools']
    
    print(f"Registered Tools ({result['count']}):\n")
    for i, tool in enumerate(tools, 1):
        print(f"   {i}. {tool}")
    print()


def get_stats():
    """Get system statistics."""
    print_section("System Statistics")
    
    response = requests.get(f"{BASE_URL}/stats")
    response.raise_for_status()
    
    result = response.json()
    
    print(f"Total Graphs: {result['total_graphs']}")
    print(f"Total Runs: {result['total_runs']}\n")
    
    if result['graphs']:
        print("Graphs:")
        for graph in result['graphs']:
            print(f"   • {graph['name']}")
            print(f"     ID: {graph['graph_id']}")
            print(f"     Runs: {graph['runs']}\n")


def main():
    """Main test function."""
    print("\n" + "="*60)
    print("  DATA QUALITY PIPELINE - COMPLETE TEST")
    print("="*60)
    print("\n⚠️  Make sure the server is running on http://localhost:8000")
    print("   Start it with: python app/main.py\n")
    
    input("Press Enter to continue...")
    
    try:
        # Test 1: List available tools
        list_tools()
        
        # Test 2: Create graph
        graph_id = create_graph()
        
        # Test 3: List all graphs
        list_graphs()
        
        # Test 4: Run pipeline with sample data
        # Dataset with outliers and missing values
        test_dataset = [
            100, 105, 98, 102, 200,  # 200 is an outlier
            95, 103, None,            # None is missing value
            99, 250, 101, 97, 104,    # 250 is an outlier
            106, 94, 100, 102
        ]
        
        result = run_pipeline(graph_id, test_dataset)
        
        # Test 5: Print results
        print_execution_results(result)
        
        # Test 6: Get run state
        get_run_state(result['run_id'])
        
        # Test 7: Get system stats
        get_stats()
        
        print_section("All Tests Completed Successfully! ✅")
        
        print("""
Next Steps:
1. Try the interactive API docs at http://localhost:8000/docs
2. Test WebSocket streaming (see README for examples)
3. Create your own custom workflows
4. Explore the codebase and extend functionality
        """)
        
    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Cannot connect to server!")
        print("   Make sure the server is running:")
        print("   python app/main.py")
        return 1
    
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ HTTP Error: {e}")
        print(f"   Response: {e.response.text}")
        return 1
    
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())