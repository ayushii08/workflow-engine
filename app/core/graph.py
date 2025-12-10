"""
Graph definition and management for the workflow engine.
"""
from typing import Dict, List, Optional, Set
from app.core.node import Node, ConditionalNode, LoopNode
from app.models.graph import GraphDefinition, EdgeDefinition, LoopDefinition
import uuid
import logging

logger = logging.getLogger(__name__)


class Graph:
    """
    Represents a workflow graph with nodes and edges.
    Manages the structure and relationships between nodes.
    """
    
    def __init__(
        self,
        graph_id: str,
        name: str,
        description: Optional[str] = None,
        entry_point: Optional[str] = None
    ):
        self.graph_id = graph_id
        self.name = name
        self.description = description
        self.entry_point = entry_point
        
        # Core data structures
        self.nodes: Dict[str, Node] = {}
        self.edges: Dict[str, List[str]] = {}  # node_name -> [next_node_names]
        self.conditional_edges: Dict[str, Dict[str, EdgeDefinition]] = {}  # node -> {next_node -> condition}
        self.loop_definitions: Dict[str, LoopDefinition] = {}  # node_name -> loop_def
    
    def add_node(self, node: Node) -> None:
        """Add a node to the graph."""
        if node.name in self.nodes:
            raise ValueError(f"Node {node.name} already exists in graph")
        
        self.nodes[node.name] = node
        self.edges[node.name] = []
        logger.debug(f"Added node: {node.name} to graph {self.graph_id}")
    
    def add_edge(self, from_node: str, to_node: str, edge_def: Optional[EdgeDefinition] = None) -> None:
        """
        Add an edge between two nodes.
        
        Args:
            from_node: Source node name
            to_node: Target node name
            edge_def: Optional edge definition with conditions
        """
        if from_node not in self.nodes:
            raise ValueError(f"Source node {from_node} does not exist")
        if to_node not in self.nodes:
            raise ValueError(f"Target node {to_node} does not exist")
        
        # Add to edges list
        if to_node not in self.edges[from_node]:
            self.edges[from_node].append(to_node)
        
        # If edge has a condition, store it separately
        if edge_def and edge_def.condition:
            if from_node not in self.conditional_edges:
                self.conditional_edges[from_node] = {}
            self.conditional_edges[from_node][to_node] = edge_def
        
        logger.debug(f"Added edge: {from_node} -> {to_node}")
    
    def add_loop(self, loop_def: LoopDefinition) -> None:
        """Add a loop definition to the graph."""
        if loop_def.node not in self.nodes:
            raise ValueError(f"Loop node {loop_def.node} does not exist")
        
        self.loop_definitions[loop_def.node] = loop_def
        logger.debug(f"Added loop on node: {loop_def.node}")
    
    def get_next_nodes(self, current_node: str) -> List[str]:
        """
        Get the list of next nodes from current node.
        
        Args:
            current_node: Current node name
            
        Returns:
            List of next node names
        """
        return self.edges.get(current_node, [])
    
    def get_node(self, node_name: str) -> Optional[Node]:
        """Get a node by name."""
        return self.nodes.get(node_name)
    
    def has_loop(self, node_name: str) -> bool:
        """Check if a node is part of a loop."""
        return node_name in self.loop_definitions
    
    def get_loop_definition(self, node_name: str) -> Optional[LoopDefinition]:
        """Get loop definition for a node."""
        return self.loop_definitions.get(node_name)
    
    def validate(self) -> bool:
        """
        Validate the graph structure.
        
        Returns:
            True if valid, raises ValueError otherwise
        """
        # Check entry point exists
        if not self.entry_point:
            raise ValueError("Graph must have an entry point")
        
        if self.entry_point not in self.nodes:
            raise ValueError(f"Entry point {self.entry_point} does not exist")
        
        # Check for cycles (except intentional loops)
        visited: Set[str] = set()
        
        def has_cycle(node: str, path: Set[str]) -> bool:
            if node in path:
                # Check if this is an intentional loop
                if node not in self.loop_definitions:
                    return True
                return False
            
            if node in visited:
                return False
            
            visited.add(node)
            path.add(node)
            
            for next_node in self.get_next_nodes(node):
                if has_cycle(next_node, path.copy()):
                    return True
            
            return False
        
        if has_cycle(self.entry_point, set()):
            logger.warning(f"Graph {self.graph_id} contains cycles (may be intentional)")
        
        logger.info(f"Graph {self.graph_id} validated successfully")
        return True
    
    def to_dict(self) -> Dict:
        """Convert graph to dictionary representation."""
        return {
            "graph_id": self.graph_id,
            "name": self.name,
            "description": self.description,
            "entry_point": self.entry_point,
            "nodes": list(self.nodes.keys()),
            "edges": self.edges,
            "loops": {k: v.dict() for k, v in self.loop_definitions.items()}
        }


class GraphBuilder:
    """
    Builder class for constructing graphs from definitions.
    Handles the conversion from GraphDefinition to Graph objects.
    """
    
    @staticmethod
    def build_from_definition(
        definition: GraphDefinition,
        tool_registry: Dict[str, callable]
    ) -> Graph:
        """
        Build a Graph object from a GraphDefinition.
        
        Args:
            definition: Graph definition from API
            tool_registry: Registry of available tools
            
        Returns:
            Constructed Graph object
        """
        graph_id = str(uuid.uuid4())
        graph = Graph(
            graph_id=graph_id,
            name=definition.name,
            description=definition.description,
            entry_point=definition.entry_point
        )
        
        # Create nodes
        for node_def in definition.nodes:
            tool = tool_registry.get(node_def.tool)
            if not tool:
                raise ValueError(f"Tool {node_def.tool} not found in registry")
            
            # Check if this node is part of a loop
            loop_def = next((l for l in definition.loops if l.node == node_def.name), None)
            
            if loop_def:
                node = LoopNode(
                    name=node_def.name,
                    tool=tool,
                    exit_condition=loop_def.condition,
                    max_iterations=loop_def.max_iterations,
                    config=node_def.config
                )
            else:
                node = Node(
                    name=node_def.name,
                    tool=tool,
                    node_type=node_def.type,
                    config=node_def.config
                )
            
            graph.add_node(node)
        
        # Create edges
        for edge_def in definition.edges:
            graph.add_edge(edge_def.from_node, edge_def.to_node, edge_def)
        
        # Add loop definitions
        for loop_def in definition.loops:
            graph.add_loop(loop_def)
        
        # Validate graph
        graph.validate()
        
        logger.info(f"Built graph {graph_id} with {len(graph.nodes)} nodes")
        return graph