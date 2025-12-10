# app/__init__.py
"""Workflow Engine - A minimal but powerful graph execution system."""
__version__ = "1.0.0"

# app/core/__init__.py
"""Core workflow engine components."""

# app/api/__init__.py
"""API layer for the workflow engine."""

# app/models/__init__.py
"""Data models and schemas."""

# app/storage/__init__.py
"""Storage layer for persisting graphs and runs."""

# app/tools/__init__.py
"""Tool registry and tool implementations."""

# app/workflows/__init__.py
"""Workflow implementations and examples."""
from app.workflows.data_quality import register_data_quality_tools

# Auto-register data quality tools on import
register_data_quality_tools()