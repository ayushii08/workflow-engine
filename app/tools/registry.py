"""
Tool registry for managing available workflow tools/functions.
"""
from typing import Dict, Callable, Optional
import logging

logger = logging.getLogger(__name__)


class ToolRegistry:
    """
    Central registry for workflow tools.
    Tools are async functions that take state and optional config parameters.
    """
    
    def __init__(self):
        self._tools: Dict[str, Callable] = {}
    
    def register(self, name: str, tool: Callable) -> None:
        """
        Register a tool function.
        
        Args:
            name: Unique tool name
            tool: Callable tool function
        """
        if name in self._tools:
            logger.warning(f"Tool {name} already registered, overwriting")
        
        self._tools[name] = tool
        logger.info(f"Registered tool: {name}")
    
    def unregister(self, name: str) -> bool:
        """
        Unregister a tool.
        
        Args:
            name: Tool name to unregister
            
        Returns:
            True if tool was unregistered, False if not found
        """
        if name in self._tools:
            del self._tools[name]
            logger.info(f"Unregistered tool: {name}")
            return True
        return False
    
    def get(self, name: str) -> Optional[Callable]:
        """
        Get a tool by name.
        
        Args:
            name: Tool name
            
        Returns:
            Tool function or None if not found
        """
        return self._tools.get(name)
    
    def list_tools(self) -> list:
        """Get list of all registered tool names."""
        return list(self._tools.keys())
    
    def get_all(self) -> Dict[str, Callable]:
        """Get all registered tools."""
        return self._tools.copy()


# Global registry instance
_registry_instance: Optional[ToolRegistry] = None


def get_tool_registry() -> ToolRegistry:
    """Get or create the global tool registry instance."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = ToolRegistry()
        # Register default tools on first access
        _register_default_tools(_registry_instance)
    return _registry_instance


def _register_default_tools(registry: ToolRegistry) -> None:
    """Register default example tools."""
    
    # Example: Simple data transformation tool
    async def transform_data(state, **config):
        """Simple data transformation."""
        data = state.get("data", [])
        transformed = [x * 2 for x in data] if isinstance(data, list) else data
        return {"transformed_data": transformed}
    
    # Example: Validation tool
    async def validate_data(state, **config):
        """Validate data meets criteria."""
        data = state.get("data", [])
        is_valid = len(data) > 0 if isinstance(data, list) else False
        return {"is_valid": is_valid, "validation_result": "passed" if is_valid else "failed"}
    
    registry.register("transform_data", transform_data)
    registry.register("validate_data", validate_data)
    
    logger.info("Registered default tools")